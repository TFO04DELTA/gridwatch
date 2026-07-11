#!/usr/bin/env python3
"""
GRIDWATCH — FirstEnergy (Ohio) outage collector with weather + data center
proximity correlation.

Hypothesis under test: are fair-weather distribution outages clustering near
data center sites (operating or under construction) at a higher rate than
elsewhere in the territory?

Subcommands:
    poll      Fetch current outages from KUBRA StormCenter, enrich each with
              hourly weather (Open-Meteo) + active NWS alerts, classify, and
              log to SQLite. Run this on a schedule (Task Scheduler / cron,
              every 15 min — the map itself only refreshes that often).
    map       Render an interactive folium HTML map of the latest snapshot:
              outages colored by classification, DC sites with radius rings.
    report    Print longitudinal stats: fair-weather outage rate inside vs
              outside DC proximity rings. This is the actual answer over time.
    discover  Help find the KUBRA instance/view GUIDs from the FE redirect.

Setup (one time):
    1. pip install requests folium
    2. python gridwatch.py discover
       If auto-discovery fails: open https://outages-oh.firstenergycorp.com
       in a browser, DevTools > Network, filter "currentState". The URL looks
       like  .../stormcenters/{INSTANCE_ID}/views/{VIEW_ID}/currentState
       Put both GUIDs in gridwatch_config.json.
    3. Edit datacenters.json — verify/extend site coordinates.
    4. python gridwatch.py poll        (schedule it)
       python gridwatch.py map        (open gridwatch_map.html)
       python gridwatch.py report

No API keys required. NWS requires a descriptive User-Agent (set below).
"""

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "gridwatch_config.json")
DC_PATH = os.path.join(BASE_DIR, "datacenters.json")
DB_PATH = os.path.join(BASE_DIR, "gridwatch.db")
MAP_PATH = os.path.join(BASE_DIR, "gridwatch_map.html")

DEFAULT_CONFIG = {
    # One entry per FE StormCenter deployment. Each state map has its own
    # KUBRA instance/view GUID pair — grab them once via DevTools on that
    # state's outage page (Network tab, filter 'currentState'). Regions with
    # empty GUIDs are skipped with a notice.
    "regions": [
        {"name": "OH",
         "entry": "https://outages-oh.firstenergycorp.com/",
         "instance_id": "6c715f0e-bbec-465f-98cc-0b81623744be",
         "view_id": "db9c3f02-0a06-4672-a357-0f676eb75bfa",
         "bbox": {"west": -84.9, "south": 40.2, "east": -80.4, "north": 42.1}},
        {"name": "PA-NY",
         "entry": "https://outages-pa.firstenergycorp.com/",
         "instance_id": "", "view_id": "",
         "bbox": {"west": -80.6, "south": 39.6, "east": -74.4, "north": 42.9}},
        {"name": "NJ",
         "entry": "https://outages-nj.firstenergycorp.com/",
         "instance_id": "", "view_id": "",
         "bbox": {"west": -75.7, "south": 38.8, "east": -73.8, "north": 41.4}},
        {"name": "MD",
         "entry": "https://outages-md.firstenergycorp.com/",
         "instance_id": "", "view_id": "",
         "bbox": {"west": -79.6, "south": 39.1, "east": -77.0, "north": 39.8}},
        {"name": "WV",
         "entry": "https://outages-wv.firstenergycorp.com/",
         "instance_id": "", "view_id": "",
         "bbox": {"west": -82.7, "south": 37.1, "east": -77.7, "north": 40.7}},
        {"name": "OH-AEP", "provider": "ifactor",
         "base": "https://outagemap.aepohio.com",
         "entry": "https://outagemap.aepohio.com/",
         "bbox": {"west": -84.9, "south": 38.3, "east": -80.6, "north": 41.4}},
        {"name": "OH-AES", "provider": "ifactor",
         "base": "https://outagemap.aes-ohio.com",
         "entry": "https://outagemap.aes-ohio.com/",
         "bbox": {"west": -84.9, "south": 39.3, "east": -83.5, "north": 40.4}},
        {"name": "OH-DUKE",
         "entry": "https://outagemap.duke-energy.com/",
         "instance_id": "", "view_id": "",
         "bbox": {"west": -84.9, "south": 38.8, "east": -83.6, "north": 39.6}},
    ],
    # Alerting: file a GitHub issue when a DC-proximate fair-weather outage
    # of at least this many customers appears (CI only; needs issues:write)
    "alert_min_customers": 50,
    # Legacy single-region keys (still honored if regions is empty)
    "kubra_instance_id": "",
    "kubra_view_id": "",
    "utility_entry_url": "https://outages-oh.firstenergycorp.com/",
    "bbox": {"west": -84.9, "south": 40.2, "east": -80.4, "north": 42.1},
    # Tile layer name (from a live tile URL: .../public/cluster-5/{qk}.json)
    "kubra_layer": "cluster-5",
    # Quadkey zoom sweep. KUBRA serves cluster tiles at multiple zooms;
    # deeper zoom = more tile fetches but individual (non-clustered) outages.
    "zoom_min": 8,
    "zoom_max": 14,
    # Weather thresholds for calling an outage "weather-plausible"
    "wind_gust_kmh_threshold": 45.0,   # ~28 mph gusts
    "wind_sustained_kmh_threshold": 32.0,  # ~20 mph sustained
    "precip_mm_hr_threshold": 2.5,     # meaningful rain/snow water-equiv
    "temp_extreme_low_c": -12.0,       # ice/load extremes
    "temp_extreme_high_c": 35.0,       # AC load stress
    # Data center proximity ring (km). Construction impacts are local:
    # feeder-level. 8 km is generous; tighten after you see the data.
    "dc_radius_km": 8.0,
    # NWS etiquette — put a real contact in here, they rate-limit anons
    "nws_user_agent": "GRIDWATCH outage-weather research (contact: you@example.com)",
    # Weather grid cell size in degrees for batching Open-Meteo calls
    "weather_cell_deg": 0.25,
}


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        merged = dict(DEFAULT_CONFIG)
        merged.update(cfg)
        return merged
    with open(CONFIG_PATH, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=2)
    print(f"[i] Wrote default config to {CONFIG_PATH} — fill in KUBRA GUIDs "
          f"(run: gridwatch.py discover)")
    return dict(DEFAULT_CONFIG)


# ----------------------------------------------------------------------------
# Google polyline decoding (KUBRA encodes geom.p / geom.a this way)
# ----------------------------------------------------------------------------

def decode_polyline(encoded, precision=5):
    """Decode a Google encoded polyline string to [(lat, lon), ...]."""
    coords, index, lat, lon = [], 0, 0, 0
    factor = 10 ** precision
    while index < len(encoded):
        for is_lon in (False, True):
            shift, result = 0, 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lon:
                lon += delta
            else:
                lat += delta
        coords.append((lat / factor, lon / factor))
    return coords


# ----------------------------------------------------------------------------
# Quadkey math (Bing tile scheme, which KUBRA uses for its tile paths)
# ----------------------------------------------------------------------------

def latlon_to_tile(lat, lon, zoom):
    lat = max(min(lat, 85.05112878), -85.05112878)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def tile_to_quadkey(x, y, zoom):
    qk = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        qk.append(str(digit))
    return "".join(qk)


def quadkeys_for_bbox(bbox, zoom):
    x0, y0 = latlon_to_tile(bbox["north"], bbox["west"], zoom)
    x1, y1 = latlon_to_tile(bbox["south"], bbox["east"], zoom)
    keys = []
    for x in range(min(x0, x1), max(x0, x1) + 1):
        for y in range(min(y0, y1), max(y0, y1) + 1):
            keys.append(tile_to_quadkey(x, y, zoom))
    return keys


# ----------------------------------------------------------------------------
# KUBRA StormCenter client
# ----------------------------------------------------------------------------

KUBRA_BASE = "https://kubra.io"


class Kubra:
    def __init__(self, cfg, region=None, session=None):
        self.cfg = cfg
        self.region = region or {
            "name": "OH",
            "entry": cfg.get("utility_entry_url", ""),
            "instance_id": cfg.get("kubra_instance_id", ""),
            "view_id": cfg.get("kubra_view_id", ""),
            "bbox": cfg.get("bbox"),
        }
        self.s = session or requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0 (GRIDWATCH research)"})

    def discover_ids(self):
        """Follow the utility redirect to find the view id; instance id still
        usually needs DevTools, but we try known patterns."""
        r = self.s.get(self.region["entry"], allow_redirects=True, timeout=30)
        # Redirected URL looks like kubra.io/stormcenter/views/{view_id}/
        m = re.search(r"stormcenter/views/([0-9a-f-]{36})", r.url) or \
            re.search(r"stormcenter/views/([0-9a-f-]{36})", r.text)
        view_id = m.group(1) if m else None
        instance_id = None
        # The page HTML / bundled config frequently embeds the stormcenter
        # (instance) GUID; scan for candidate GUID pairs.
        guids = re.findall(r"stormcenters/([0-9a-f-]{36})", r.text)
        if guids:
            instance_id = guids[0]
        return instance_id, view_id

    def current_state(self):
        url = (f"{KUBRA_BASE}/stormcenter/api/v1/stormcenters/"
               f"{self.region['instance_id']}/views/"
               f"{self.region['view_id']}/currentState?preview=false")
        r = self.s.get(url, timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _extract_template(state):
        """FE's currentState carries a URL template:
             data.cluster_interval_generation_data =
               'cluster-data/{qkh}/{guid}/{guid}'
        where {qkh} is the LAST THREE digits of the tile quadkey, REVERSED
        (e.g. quadkey 030223223 -> '223' -> '322'). The GUID pair rotates
        every map regeneration, so this is re-read on every poll."""
        tmpl = (state.get("data") or {}).get("cluster_interval_generation_data")
        if not tmpl:
            # fall back to the classic non-sharded path
            tmpl = (state.get("data") or {}).get("interval_generation_data")
        if not tmpl:
            raise RuntimeError(
                "currentState has neither cluster_interval_generation_data "
                "nor interval_generation_data. Dump:\n"
                + json.dumps(state, indent=2)[:3000])
        return tmpl.strip("/")

    @staticmethod
    def _qkh(quadkey):
        return quadkey[-3:][::-1]

    def _prepare(self):
        state = self.current_state()
        template = self._extract_template(state)
        layer = self.cfg.get("kubra_layer", "cluster-5")
        print(f"[i] [{self.region['name']}] kubra template: {template} | layer: {layer}")
        return {"template": template, "layer": layer}

    def _tile_url(self, ctx, qk):
        path = ctx["template"].replace("{qkh}", self._qkh(qk))
        return f"{KUBRA_BASE}/{path}/public/{ctx['layer']}/{qk}.json"

    def _fetch_tile(self, ctx, qk):
        try:
            r = self.s.get(self._tile_url(ctx, qk), timeout=20)
            if r.status_code != 200:
                return None
            return r.json()
        except (requests.RequestException, ValueError):
            return None

    def fetch_outages(self):
        """Descend the quadkey pyramid. KUBRA item ids are only unique WITHIN
        a tile (e.g. '8-0'), so records are keyed by id+coords. Cluster
        markers are drilled into until they resolve to individual incidents
        (or zoom_max); a cluster is only kept if its children never resolved,
        so nothing is double-counted."""
        ctx = self._prepare()
        cfg = self.cfg
        bbox = self.region.get("bbox") or cfg["bbox"]
        incidents = {}          # key -> record (non-cluster, authoritative)
        pending_clusters = {}   # qk -> [cluster records] kept only if unresolved
        answered_qks = set()    # every tile that returned a payload

        def key(rec):
            return f"{rec['outage_id']}@{rec['lat']:.4f},{rec['lon']:.4f}"

        # find the top of the tile pyramid
        zoom = cfg["zoom_min"]
        frontier = quadkeys_for_bbox(bbox, zoom)
        while zoom <= cfg["zoom_max"]:
            next_frontier, n_inc, n_clu, any_tile = [], 0, 0, False
            for qk in frontier:
                payload = self._fetch_tile(ctx, qk)
                time.sleep(0.04)
                if payload is None:
                    continue
                any_tile = True
                answered_qks.add(qk)
                tile_clusters = []
                for item in payload.get("file_data", []):
                    rec = self._parse_item(item)
                    if not rec:
                        continue
                    if rec["cluster"] and zoom < cfg["zoom_max"]:
                        tile_clusters.append(rec)
                        n_clu += 1
                    else:
                        incidents[key(rec)] = rec
                        n_inc += 1
                if tile_clusters:
                    pending_clusters[qk] = tile_clusters
                    next_frontier.extend(qk + d for d in "0123")
                    # children resolved anything under this tile; drop parents
                    # of deeper pending clusters once children respond
            print(f"[i] zoom {zoom}: {len(frontier)} tiles, "
                  f"{n_inc} incidents, {n_clu} clusters pending "
                  f"({len(incidents)} unique incidents so far)")
            if not any_tile and not incidents and not pending_clusters \
                    and zoom < cfg["zoom_max"]:
                next_frontier = quadkeys_for_bbox(bbox, zoom + 1)
                print(f"[i] no tiles at zoom {zoom}; widening to {zoom+1}")
            frontier, zoom = next_frontier, zoom + 1
            if not frontier:
                break

        # A cluster's contents are re-emitted by whichever of its child
        # tiles exist. If NONE of its 4 children answered, the pyramid ends
        # here and the cluster is a leaf — keep it or lose its customers.
        kept_clusters = 0
        for qk, recs in pending_clusters.items():
            if any((qk + d) in answered_qks for d in "0123"):
                continue  # superseded by deeper data
            for rec in recs:
                incidents[key(rec)] = rec
                kept_clusters += 1
        if kept_clusters:
            print(f"[i] kept {kept_clusters} unresolved cluster markers")
        if not incidents:
            print("[!] Zero outages parsed. If the FE map visibly shows "
                  "outages right now, the layer name may differ — grab a "
                  "tile URL from DevTools (filter 'cluster') and set "
                  "'kubra_layer' in gridwatch_config.json to match.")
        for o in incidents.values():
            o["region"] = self.region["name"]
        return list(incidents.values())

    @staticmethod
    def _parse_item(item):
        desc = item.get("desc", {}) or {}
        geom = item.get("geom", {}) or {}
        pts = geom.get("p") or []
        lat = lon = None
        if pts:
            try:
                decoded = decode_polyline(pts[0])
                if decoded:
                    lat, lon = decoded[0]
            except Exception:
                pass
        if lat is None and geom.get("a"):
            # polygon outage area — use centroid of first ring
            try:
                ring = decode_polyline(geom["a"][0])
                lat = sum(p[0] for p in ring) / len(ring)
                lon = sum(p[1] for p in ring) / len(ring)
            except Exception:
                pass
        if lat is None:
            return None

        def _val(d, *keys):
            cur = d
            for k in keys:
                if not isinstance(cur, dict):
                    return None
                cur = cur.get(k)
            return cur

        cause = _val(desc, "cause", "EN-US") or _val(desc, "cause") or ""
        if isinstance(cause, dict):
            cause = next(iter(cause.values()), "")
        cust = _val(desc, "cust_a", "val")
        if cust is None:
            cust = desc.get("n_out") or 0
        etr = desc.get("etr") or desc.get("start_etr") or ""
        crew = desc.get("crew_status") or desc.get("crew_icon") or ""
        if isinstance(crew, dict):
            crew = crew.get("EN-US") or crew.get("orig") \
                or next(iter(crew.values()), "")
        oid = (item.get("id")
               or desc.get("inc_id")
               or f"{round(lat,4)},{round(lon,4)}:{cust}")
        return {
            "outage_id": str(oid),
            "lat": round(float(lat), 5),
            "lon": round(float(lon), 5),
            "customers": int(cust) if cust else 0,
            "cause": str(cause),
            "crew_status": str(crew),
            "etr": str(etr),
            "cluster": bool(desc.get("cluster") or item.get("title") == "cluster"),
        }

class IFactor(Kubra):
    """Legacy iFactor/KUBRA-classic maps (AEP Ohio, AES Ohio family).
    Scheme: {base}/resources/data/external/interval_generation_data/
              metadata.json  -> {"directory": "<stamp>"}
              {stamp}/outages/{quadkey}.json  -> same file_data schema.
    The region entry needs only: {"provider":"ifactor","base":"https://outagemap.aepohio.com"}.
    If the default paths 404, DevTools on the map (filter 'metadata' or
    'outages') shows the live ones; override with region["data_path"]."""

    BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0.0.0 Safari/537.36")

    def _prepare(self):
        base = self.region["base"].rstrip("/")
        # CloudFront fronting these maps sometimes 403s non-browser UAs
        self.s.headers["User-Agent"] = self.BROWSER_UA
        if self.region.get("data_path"):
            path_candidates = [self.region["data_path"]]
        else:
            path_candidates = [
                "resources/data/external/interval_generation_data",
                "resources/data/interval_generation_data",
                "data/interval_generation_data",
                "external/interval_generation_data",
            ]
        meta_candidates = []
        for dp in path_candidates:
            meta_candidates += [f"{base}/{dp}/metadata.json",
                                f"{base}/{dp}/metadata.xml"]
        directory, data_path = None, None
        for mu in meta_candidates:
            try:
                r = self.s.get(mu, timeout=20)
                if r.status_code != 200:
                    print(f"    [{self.region['name']}] {r.status_code} {mu}")
                    continue
                if mu.endswith(".json"):
                    directory = r.json().get("directory")
                else:
                    m = re.search(r"<directory>([^<]+)</directory>", r.text)
                    directory = m.group(1) if m else None
                if directory:
                    data_path = mu.rsplit("/metadata", 1)[0][len(base) + 1:]
                    break
            except (requests.RequestException, ValueError):
                continue
        if not directory:
            raise RuntimeError(
                f"[{self.region['name']}] iFactor metadata not found at "
                f"{meta_candidates}. Open the utility map with DevTools, "
                f"filter 'metadata', and set region['base']/'data_path' to match.")
        print(f"[i] [{self.region['name']}] ifactor: {data_path} -> {directory}")
        return {"base": base, "data_path": data_path, "dir": directory}

    def _tile_url(self, ctx, qk):
        return (f"{ctx['base']}/{ctx['data_path']}/{ctx['dir']}/"
                f"outages/{qk}.json")


def make_provider(cfg, region):
    if region.get("provider", "kubra") == "ifactor":
        return IFactor(cfg, region=region)
    return Kubra(cfg, region=region)


# ----------------------------------------------------------------------------
# Weather enrichment: Open-Meteo hourly (no key) + NWS active alerts
# ----------------------------------------------------------------------------

class Weather:
    def __init__(self, cfg, session=None):
        self.cfg = cfg
        self.s = session or requests.Session()
        self._cell_cache = {}
        self._alert_cache = {}

    def _cell(self, lat, lon):
        step = self.cfg["weather_cell_deg"]
        return (round(math.floor(lat / step) * step + step / 2, 3),
                round(math.floor(lon / step) * step + step / 2, 3))

    def conditions(self, lat, lon):
        """Current-hour wind/gust/precip/temp for the grid cell containing
        the point. Cached per cell so 200 outages != 200 API calls."""
        cell = self._cell(lat, lon)
        if cell in self._cell_cache:
            return self._cell_cache[cell]
        url = ("https://api.open-meteo.com/v1/forecast"
               f"?latitude={cell[0]}&longitude={cell[1]}"
               "&hourly=wind_speed_10m,wind_gusts_10m,precipitation,temperature_2m"
               "&past_hours=3&forecast_hours=1&timezone=UTC")
        out = {"wind_kmh": None, "gust_kmh": None, "precip_mm": None, "temp_c": None}
        try:
            r = self.s.get(url, timeout=20)
            r.raise_for_status()
            h = r.json().get("hourly", {})
            # take max over the trailing 3h window — outages lag weather
            def mx(key):
                vals = [v for v in h.get(key, []) if v is not None]
                return max(vals) if vals else None
            out["wind_kmh"] = mx("wind_speed_10m")
            out["gust_kmh"] = mx("wind_gusts_10m")
            out["precip_mm"] = mx("precipitation")
            temps = [v for v in h.get("temperature_2m", []) if v is not None]
            out["temp_c"] = temps[-1] if temps else None
        except requests.RequestException as e:
            print(f"[!] Open-Meteo failed for cell {cell}: {e}")
        self._cell_cache[cell] = out
        time.sleep(0.1)
        return out

    def active_alerts(self, lat, lon):
        """NWS active alert event names for the point (cached per cell)."""
        cell = self._cell(lat, lon)
        if cell in self._alert_cache:
            return self._alert_cache[cell]
        url = f"https://api.weather.gov/alerts/active?point={cell[0]},{cell[1]}"
        events = []
        try:
            r = self.s.get(url, timeout=20,
                           headers={"User-Agent": self.cfg["nws_user_agent"],
                                    "Accept": "application/geo+json"})
            if r.status_code == 200:
                for feat in r.json().get("features", []):
                    ev = feat.get("properties", {}).get("event")
                    if ev:
                        events.append(ev)
        except requests.RequestException as e:
            print(f"[!] NWS alerts failed for cell {cell}: {e}")
        events = sorted(set(events))
        self._alert_cache[cell] = events
        time.sleep(0.1)
        return events


# ----------------------------------------------------------------------------
# Data center proximity + classification
# ----------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def load_datacenters():
    if not os.path.exists(DC_PATH):
        print(f"[!] {DC_PATH} not found — no DC proximity layer. "
              f"See datacenters.json template.")
        return []
    with open(DC_PATH) as f:
        return json.load(f).get("sites", [])


def nearest_dc(lat, lon, sites):
    best = (None, 1e9)
    for s in sites:
        d = haversine_km(lat, lon, s["lat"], s["lon"])
        if d < best[1]:
            best = (s, d)
    return best


# Cause strings FE/KUBRA uses that already concede weather
WEATHERY_CAUSES = re.compile(
    r"weather|storm|wind|tree|lightning|ice|snow|flood", re.I)
# Cause strings consistent with third-party/construction damage
DIG_CAUSES = re.compile(
    r"dig|excavat|vehicle|car|pole|contact|construction|third.?party|damage", re.I)


def classify(outage, wx, alerts, dc, dc_km, cfg):
    """Return (classification, weather_flag, dc_flag, rationale)."""
    weather_signals = []
    if wx.get("gust_kmh") and wx["gust_kmh"] >= cfg["wind_gust_kmh_threshold"]:
        weather_signals.append(f"gusts {wx['gust_kmh']:.0f} km/h")
    if wx.get("wind_kmh") and wx["wind_kmh"] >= cfg["wind_sustained_kmh_threshold"]:
        weather_signals.append(f"sustained wind {wx['wind_kmh']:.0f} km/h")
    if wx.get("precip_mm") and wx["precip_mm"] >= cfg["precip_mm_hr_threshold"]:
        weather_signals.append(f"precip {wx['precip_mm']:.1f} mm/h")
    if wx.get("temp_c") is not None and (
            wx["temp_c"] <= cfg["temp_extreme_low_c"]
            or wx["temp_c"] >= cfg["temp_extreme_high_c"]):
        weather_signals.append(f"temp extreme {wx['temp_c']:.0f}C")
    if alerts:
        weather_signals.append("NWS: " + "; ".join(alerts))
    if WEATHERY_CAUSES.search(outage.get("cause", "")):
        weather_signals.append(f"utility cause='{outage['cause']}'")

    weather_flag = bool(weather_signals)
    dc_flag = dc is not None and dc_km <= cfg["dc_radius_km"]

    if weather_flag and dc_flag:
        cls = "AMBIGUOUS (weather + DC-proximate)"
    elif weather_flag:
        cls = "WEATHER-LIKELY"
    elif dc_flag:
        cls = "DC-PROXIMATE FAIR-WEATHER"
        if DIG_CAUSES.search(outage.get("cause", "")):
            cls = "DC-PROXIMATE FAIR-WEATHER (construction-type cause)"
    else:
        cls = "UNEXPLAINED FAIR-WEATHER"

    rationale = "; ".join(weather_signals) if weather_signals else "no weather signal"
    if dc:
        rationale += f" | nearest DC: {dc['name']} ({dc_km:.1f} km, {dc.get('status','?')})"
    return cls, weather_flag, dc_flag, rationale


# ----------------------------------------------------------------------------
# SQLite persistence
# ----------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY,
    polled_at TEXT NOT NULL,
    region TEXT DEFAULT 'OH',
    outage_id TEXT NOT NULL,
    lat REAL, lon REAL,
    customers INTEGER,
    cause TEXT, crew_status TEXT, etr TEXT,
    wind_kmh REAL, gust_kmh REAL, precip_mm REAL, temp_c REAL,
    nws_alerts TEXT,
    dc_name TEXT, dc_km REAL, dc_status TEXT,
    weather_flag INTEGER, dc_flag INTEGER,
    classification TEXT, rationale TEXT
);
CREATE INDEX IF NOT EXISTS idx_obs_poll ON observations(polled_at);
CREATE INDEX IF NOT EXISTS idx_obs_outage ON observations(outage_id);
"""


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(observations)")]
    if "region" not in cols:  # migrate pre-multi-state DBs
        conn.execute("ALTER TABLE observations ADD COLUMN region TEXT DEFAULT 'OH'")
        conn.commit()
    return conn


# ----------------------------------------------------------------------------
# Auto-OSINT: OSM-mapped data centers via Overpass (context layer)
# ----------------------------------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
AUTO_MAX_AGE_DAYS = 7


def _overpass_query(bboxes):
    parts = []
    for b in bboxes:
        box = f"{b['south']},{b['west']},{b['north']},{b['east']}"
        for tag in ('["telecom"="data_center"]', '["building"="data_center"]',
                    '["man_made"="data_centre"]'):
            parts.append(f"nwr{tag}({box});")
    return f"[out:json][timeout:60];({''.join(parts)});out center tags;"


def fetch_auto_datacenters(cfg, out_path):
    """Pull OSM-mapped data centers inside all region bboxes. These are
    mostly OPERATING facilities (OSM lags new construction) — a context
    layer, not the evidence layer. Fail-soft: never breaks a poll."""
    bboxes = [r["bbox"] for r in cfg.get("regions", []) if r.get("bbox")]
    if not bboxes:
        bboxes = [cfg["bbox"]]
    try:
        r = requests.post(OVERPASS_URL, data={"data": _overpass_query(bboxes)},
                          timeout=90, headers={"User-Agent": cfg["nws_user_agent"]})
        r.raise_for_status()
        elements = r.json().get("elements", [])
    except Exception as e:
        print(f"[!] Overpass auto-OSINT failed (non-fatal): {e}")
        return False
    sites, seen = [], set()
    for el in elements:
        tags = el.get("tags", {}) or {}
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if lat is None:
            continue
        key = (round(lat, 3), round(lon, 3))
        if key in seen:
            continue
        seen.add(key)
        sites.append({
            "name": tags.get("name") or tags.get("operator") or "unnamed data center",
            "operator": tags.get("operator", ""),
            "lat": round(lat, 5), "lon": round(lon, 5),
            "status": "operating (OSM)",
            "source": f"OpenStreetMap {el.get('type')}/{el.get('id')}",
        })
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "note": "Auto-discovered from OSM. Context layer only: mostly "
                       "operating facilities; new construction lags in OSM. "
                       "Not used in evidence rings.",
               "sites": sites}
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"[i] auto-OSINT: {len(sites)} OSM data centers -> {out_path}")
    return True


def fetch_powerlines(cfg, sites, out_path, radius_km=12):
    """Pull OSM-mapped transmission lines (power=line, i.e. HV transmission
    by OSM convention; minor_line = distribution) within radius_km of each
    ACTIVE curated site. OSM power mapping is crowd-sourced from visible
    infrastructure — public data, not CEII. Fail-soft."""
    active = [s for s in sites
              if s.get("lat") and s.get("status") in
              ("construction", "contested", "announced")]
    if not active:
        return False
    deg = radius_km / 111.0
    parts = []
    for s in active:
        box = (f"{s['lat']-deg},{s['lon']-deg/0.75},"
               f"{s['lat']+deg},{s['lon']+deg/0.75}")
        parts.append(f'way["power"="line"]({box});')
    q = f"[out:json][timeout:90];({''.join(parts)});out geom tags;"
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=120,
                          headers={"User-Agent": cfg["nws_user_agent"]})
        r.raise_for_status()
        elements = r.json().get("elements", [])
    except Exception as e:
        print(f"[!] Overpass powerlines failed (non-fatal): {e}")
        return False
    lines, seen = [], set()
    for el in elements:
        if el.get("type") != "way" or el.get("id") in seen:
            continue
        seen.add(el.get("id"))
        geom = [[round(p["lat"], 5), round(p["lon"], 5)]
                for p in el.get("geometry", [])]
        if len(geom) < 2:
            continue
        t = el.get("tags", {}) or {}
        lines.append({"id": el["id"],
                      "name": t.get("name", ""),
                      "voltage": t.get("voltage", ""),
                      "operator": t.get("operator", ""),
                      "cables": t.get("cables", ""),
                      "geom": geom})
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "radius_km": radius_km,
               "note": "OSM power=line ways within radius of active-change "
                       "sites. Crowd-mapped visible infrastructure. Which "
                       "specific line FEEDS a site needs the interconnection "
                       "docket; this shows what's physically nearby.",
               "lines": lines}
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"[i] powerlines: {len(lines)} OSM transmission segments -> {out_path}")
    return True


def _auto_layer_stale(path):
    if not os.path.exists(path):
        return True
    try:
        with open(path) as f:
            gen = json.load(f).get("generated_at", "")
        age = datetime.now(timezone.utc) - datetime.fromisoformat(gen)
        return age > timedelta(days=AUTO_MAX_AGE_DAYS)
    except Exception:
        return True


# ----------------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------------

def cmd_discover(cfg):
    for region in cfg.get("regions", []) or [None]:
        k = Kubra(cfg, region=region)
        name = region["name"] if region else "default"
        try:
            inst, view = k.discover_ids()
        except Exception as e:
            print(f"[{name}] discover failed: {e}")
            continue
        print(f"[{name}] instance_id: {inst or 'NOT FOUND - use DevTools on '
              + (region['entry'] if region else '')}")
        print(f"[{name}] view_id:     {view or 'NOT FOUND - use DevTools'}")
        if region and (inst or view):
            region["instance_id"] = inst or region.get("instance_id", "")
            region["view_id"] = view or region.get("view_id", "")
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    print(f"[i] Saved to {CONFIG_PATH}")
    return


def _old_discover(cfg):
    k = Kubra(cfg)
    inst, view = k.discover_ids()
    print(f"instance_id: {inst or 'NOT FOUND — use DevTools'}")
    print(f"view_id:     {view or 'NOT FOUND — use DevTools'}")
    if inst or view:
        cfg["kubra_instance_id"] = inst or cfg.get("kubra_instance_id", "")
        cfg["kubra_view_id"] = view or cfg.get("kubra_view_id", "")
        with open(CONFIG_PATH, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"[i] Saved to {CONFIG_PATH}")
    if not inst:
        print("\nManual fallback: open https://outages-oh.firstenergycorp.com,\n"
              "DevTools > Network > filter 'currentState'. URL contains\n"
              ".../stormcenters/{INSTANCE_ID}/views/{VIEW_ID}/currentState")


def _region_ready(r):
    if r.get("provider") == "ifactor":
        return bool(r.get("base"))
    return bool(r.get("instance_id") and r.get("view_id"))


def _regions(cfg):
    regs = [r for r in cfg.get("regions", []) if _region_ready(r)]
    if not regs and cfg.get("kubra_instance_id"):
        regs = [{"name": "OH", "entry": cfg.get("utility_entry_url", ""),
                 "instance_id": cfg["kubra_instance_id"],
                 "view_id": cfg["kubra_view_id"], "bbox": cfg.get("bbox")}]
    skipped = [r["name"] for r in cfg.get("regions", []) if not _region_ready(r)]
    if skipped:
        print(f"[i] regions without GUIDs (skipped): {', '.join(skipped)} — "
              f"grab from DevTools on each state's outage page")
    return regs


def cmd_poll(cfg, emit_dir=None, no_db=False):
    regions = _regions(cfg)
    if not regions:
        sys.exit("[!] No regions configured with KUBRA GUIDs.")
    polled_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sites = load_datacenters()
    wx = Weather(cfg)
    conn = None if no_db else db()
    counts, emitted = {}, []
    for region in regions:
        print(f"=== region {region['name']} ===")
        try:
            outages = make_provider(cfg, region).fetch_outages()
        except Exception as e:
            print(f"[!] region {region['name']} failed: {e}")
            continue
        print(f"[i] {region['name']}: {len(outages)} unique outages")
        for o in outages:
            cond = wx.conditions(o["lat"], o["lon"])
            alerts = wx.active_alerts(o["lat"], o["lon"])
            # Only active-change sites drive dc_flag / classification /
            # alerts. Operating + control sites are context layers.
            ACTIVE_STATUSES = ("construction", "contested", "announced")
            usable = [s for s in sites
                      if s.get("status") in ACTIVE_STATUSES and s.get("lat")]
            site, dist = nearest_dc(o["lat"], o["lon"], usable) if usable else (None, None)
            cls, wflag, dflag, why = classify(o, cond, alerts, site,
                                              dist if dist is not None else 1e9, cfg)
            counts[cls] = counts.get(cls, 0) + 1
            rec = dict(polled_at=polled_at, region=o["region"],
                       outage_id=o["outage_id"], lat=o["lat"], lon=o["lon"],
                       customers=o["customers"], cause=o["cause"],
                       crew_status=o["crew_status"], etr=o["etr"],
                       wind_kmh=cond["wind_kmh"], gust_kmh=cond["gust_kmh"],
                       precip_mm=cond["precip_mm"], temp_c=cond["temp_c"],
                       nws_alerts="; ".join(alerts),
                       dc_name=site["name"] if site else None,
                       dc_km=round(dist, 2) if dist is not None else None,
                       dc_status=site.get("status") if site else None,
                       weather_flag=int(wflag), dc_flag=int(dflag),
                       classification=cls, rationale=why)
            emitted.append(rec)
            if conn:
                conn.execute(
                    """INSERT INTO observations
                       (polled_at, region, outage_id, lat, lon, customers,
                        cause, crew_status, etr, wind_kmh, gust_kmh, precip_mm,
                        temp_c, nws_alerts, dc_name, dc_km, dc_status,
                        weather_flag, dc_flag, classification, rationale)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (rec["polled_at"], rec["region"], rec["outage_id"],
                     rec["lat"], rec["lon"], rec["customers"], rec["cause"],
                     rec["crew_status"], rec["etr"], rec["wind_kmh"],
                     rec["gust_kmh"], rec["precip_mm"], rec["temp_c"],
                     rec["nws_alerts"], rec["dc_name"], rec["dc_km"],
                     rec["dc_status"], rec["weather_flag"], rec["dc_flag"],
                     rec["classification"], rec["rationale"]))
    if conn:
        conn.commit()
        conn.close()
    if emit_dir:
        _emit_json(emit_dir, polled_at, emitted, cfg)
        auto_path = os.path.join(emit_dir, "datacenters_auto.json")
        if _auto_layer_stale(auto_path):
            fetch_auto_datacenters(cfg, auto_path)
        pl_path = os.path.join(emit_dir, "powerlines.json")
        if _auto_layer_stale(pl_path):
            fetch_powerlines(cfg, sites, pl_path)
        _maybe_alert(emit_dir, emitted, cfg)
    print("[i] Snapshot classification:")
    for cls, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {n:>4}  {cls}")


def _maybe_alert(emit_dir, records, cfg):
    """CI-only: file a GitHub issue when a NEW DC-proximate fair-weather
    outage >= alert_min_customers appears. Watching the repo = free
    email/push notifications. Requires GITHUB_TOKEN + issues:write."""
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        return
    hits = [r for r in records
            if r["dc_flag"] and not r["weather_flag"]
            and (r["customers"] or 0) >= cfg.get("alert_min_customers", 50)]
    if not hits:
        return
    seen_path = os.path.join(emit_dir, "alerted.json")
    try:
        with open(seen_path) as f:
            seen = set(json.load(f))
    except (OSError, ValueError):
        seen = set()
    new_hits = []
    for r in hits:
        k = f"{r['region']}|{r['outage_id']}|{r['lat']:.4f},{r['lon']:.4f}"
        if k not in seen:
            new_hits.append(r)
            seen.add(k)
    if not new_hits:
        return
    lines = [f"- **{r['dc_name']}** ({r['dc_km']} km): {r['customers']} customers, "
             f"[{r['region']}] cause: {r['cause'] or 'none given'} — {r['rationale']}"
             for r in new_hits]
    body = ("Automated GRIDWATCH alert — fair-weather outage(s) inside a data "
            "center proximity ring:\n\n" + "\n".join(lines) +
            f"\n\nPolled at {new_hits[0]['polled_at']}. One snapshot is not "
            "evidence; check persistence on the dashboard.")
    title = (f"[GRIDWATCH] {len(new_hits)} DC-proximate fair-weather outage(s) "
             f"— {new_hits[0]['polled_at'][:10]}")
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/issues",
            json={"title": title, "body": body, "labels": ["gridwatch-alert"]},
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"}, timeout=20)
        print(f"[i] alert issue: HTTP {resp.status_code}")
        if resp.status_code == 201:
            with open(seen_path, "w") as f:
                json.dump(sorted(seen), f)
    except requests.RequestException as e:
        print(f"[!] alert failed (non-fatal): {e}")


def _emit_json(emit_dir, polled_at, records, cfg):
    """Write docs/data/latest.json + append daily NDJSON history for the
    static GitHub Pages map. Small, git-friendly files."""
    os.makedirs(os.path.join(emit_dir, "history"), exist_ok=True)
    latest = {"generated_at": polled_at,
              "dc_radius_km": cfg["dc_radius_km"],
              "count": len(records),
              "outages": records}
    with open(os.path.join(emit_dir, "latest.json"), "w") as f:
        json.dump(latest, f, separators=(",", ":"))
    day = polled_at[:10]
    with open(os.path.join(emit_dir, "history", f"{day}.ndjson"), "a") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    # rebuild the day index with per-day summaries (unique/fair/weather)
    hist_dir = os.path.join(emit_dir, "history")
    days = []
    for fn in sorted(os.listdir(hist_dir)):
        if not fn.endswith(".ndjson"):
            continue
        p = os.path.join(hist_dir, fn)
        uniq = {}
        n = 0
        with open(p) as fh:
            for line in fh:
                if not line.strip():
                    continue
                n += 1
                try:
                    r = json.loads(line)
                    uniq[f"{r.get('region')}|{r.get('outage_id')}"] = r
                except ValueError:
                    continue
        fair = sum(1 for r in uniq.values() if not r.get("weather_flag"))
        cust = sum(r.get("customers") or 0 for r in uniq.values())
        days.append({"date": fn[:-7], "records": n, "unique": len(uniq),
                     "fair": fair, "weather": len(uniq) - fair,
                     "customers": cust})
    with open(os.path.join(hist_dir, "index.json"), "w") as f:
        json.dump({"days": days}, f, separators=(",", ":"))
    # copy the DC + infrastructure layers alongside so the page has one data root
    for src_name in ("datacenters.json", "infrastructure.json", "pjm_burden.json"):
        p = os.path.join(BASE_DIR, src_name)
        if os.path.exists(p):
            with open(p) as fin, open(os.path.join(emit_dir, src_name), "w") as fout:
                fout.write(fin.read())
    print(f"[i] emitted {len(records)} records -> {emit_dir}/latest.json "
          f"+ history/{day}.ndjson")


CLASS_COLORS = {
    "WEATHER-LIKELY": "#3b82f6",
    "AMBIGUOUS (weather + DC-proximate)": "#a855f7",
    "DC-PROXIMATE FAIR-WEATHER": "#ef4444",
    "DC-PROXIMATE FAIR-WEATHER (construction-type cause)": "#b91c1c",
    "UNEXPLAINED FAIR-WEATHER": "#f59e0b",
}


def cmd_map(cfg):
    import folium
    conn = db()
    row = conn.execute("SELECT MAX(polled_at) FROM observations").fetchone()
    if not row or not row[0]:
        sys.exit("[!] No observations yet — run 'poll' first.")
    latest = row[0]
    rows = conn.execute(
        """SELECT lat, lon, customers, cause, classification, rationale,
                  dc_name, dc_km, region FROM observations WHERE polled_at=?""",
        (latest,)).fetchall()
    conn.close()

    m = folium.Map(location=[41.1, -81.6], zoom_start=8, tiles="cartodbdark_matter")
    sites = load_datacenters()
    dc_layer = folium.FeatureGroup(name="Data centers")
    for s in sites:
        folium.Circle([s["lat"], s["lon"]], radius=cfg["dc_radius_km"] * 1000,
                      color="#ef4444", weight=1, fill=True, fill_opacity=0.06
                      ).add_to(dc_layer)
        folium.Marker([s["lat"], s["lon"]],
                      icon=folium.Icon(color="red", icon="server", prefix="fa"),
                      tooltip=f"{s['name']} — {s.get('status','?')} "
                              f"({s.get('operator','?')})").add_to(dc_layer)
    dc_layer.add_to(m)

    layers = {}
    for lat, lon, cust, cause, cls, why, dc_name, dc_km, region in rows:
        layers.setdefault(cls, folium.FeatureGroup(name=cls))
        folium.CircleMarker(
            [lat, lon],
            radius=max(4, min(18, 4 + (cust or 0) ** 0.5)),
            color=CLASS_COLORS.get(cls, "#9ca3af"),
            fill=True, fill_opacity=0.75, weight=1,
            popup=folium.Popup(
                f"<b>{cls}</b> [{region}]<br>Customers: {cust}<br>"
                f"Utility cause: {cause or '(none given)'}<br>{why}",
                max_width=320),
        ).add_to(layers[cls])
    for lyr in layers.values():
        lyr.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    m.save(MAP_PATH)
    print(f"[i] Map written: {MAP_PATH}  (snapshot {latest}, {len(rows)} outages)")


def cmd_report(cfg, days=30):
    conn = db()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    q = conn.execute(
        """SELECT dc_flag, weather_flag, COUNT(DISTINCT outage_id)
           FROM observations WHERE polled_at >= ?
           GROUP BY dc_flag, weather_flag""", (since,)).fetchall()
    cells = {(d, w): n for d, w, n in q}
    near_fair = cells.get((1, 0), 0)
    near_wx = cells.get((1, 1), 0)
    far_fair = cells.get((0, 0), 0)
    far_wx = cells.get((0, 1), 0)
    near_total, far_total = near_fair + near_wx, far_fair + far_wx

    print(f"\nGRIDWATCH report — unique outages, last {days} days")
    print("=" * 58)
    print(f"{'':24}{'fair-weather':>14}{'weather':>10}{'total':>8}")
    print(f"{'Within DC ring':24}{near_fair:>14}{near_wx:>10}{near_total:>8}")
    print(f"{'Rest of territory':24}{far_fair:>14}{far_wx:>10}{far_total:>8}")
    if near_total and far_total:
        r_near = near_fair / near_total
        r_far = far_fair / far_total
        print(f"\nFair-weather share near DCs:   {r_near:6.1%}")
        print(f"Fair-weather share elsewhere:  {r_far:6.1%}")
        if r_far > 0:
            print(f"Ratio (near/elsewhere):        {r_near / r_far:.2f}x")
        print("\nInterpretation notes:")
        print(" - A ratio persistently > ~1.5x across weeks is worth a FOIA/PUCO")
        print("   docket dig; a single snapshot is noise.")
        print(" - Normalize mentally for feeder density: DC rings sit in exurban")
        print("   growth corridors where tree cover and construction both differ.")
        print(" - 'Cause' strings from the utility are self-reported. Log them,")
        print("   don't trust them.")
    top = conn.execute(
        """SELECT dc_name, COUNT(DISTINCT outage_id) n FROM observations
           WHERE polled_at >= ? AND dc_flag=1 AND weather_flag=0
           GROUP BY dc_name ORDER BY n DESC LIMIT 10""", (since,)).fetchall()
    if top:
        print("\nFair-weather outages by nearest DC site:")
        for name, n in top:
            print(f"  {n:>4}  {name}")
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="FirstEnergy OH outage/weather/DC correlator")
    ap.add_argument("command",
                    choices=["poll", "map", "report", "discover", "osint"])
    ap.add_argument("--days", type=int, default=30, help="report window (days)")
    ap.add_argument("--emit", metavar="DIR", default=None,
                    help="also write latest.json + NDJSON history to DIR "
                         "(use docs/data for GitHub Pages)")
    ap.add_argument("--no-db", action="store_true",
                    help="skip SQLite (CI mode: NDJSON history only)")
    args = ap.parse_args()
    cfg = load_config()
    {"poll": lambda c: cmd_poll(c, emit_dir=args.emit, no_db=args.no_db),
     "map": cmd_map, "discover": cmd_discover,
     "osint": lambda c: fetch_auto_datacenters(
         c, os.path.join(args.emit or "docs/data", "datacenters_auto.json")),
     "report": lambda c: cmd_report(c, args.days)}[args.command](cfg)


if __name__ == "__main__":
    main()
