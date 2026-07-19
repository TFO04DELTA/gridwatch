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
        {"name": "OH-DUKE", "provider": "duke", "jurisdiction": "DEM",
         "entry": "https://outagemap.duke-energy.com/",
         "bbox": {"west": -84.9, "south": 38.4, "east": -83.6, "north": 39.6}},
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
    # Blind widening stops here: known pyramids start by zoom 10, so no
    # tiles by then = zero outages in the region.
    "widen_max_zoom": 10,
    # Hard circuit breaker per region (healthy storm-day use: a few hundred)
    "max_tiles_per_region": 20000,
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

    def _layer_candidates(self):
        """Kubra views can each use a different cluster layer number. Try the
        configured/remembered one first, then sweep the rest."""
        first = (self.region.get("kubra_layer")
                 or self.cfg.get("kubra_layer", "cluster-5"))
        rest = [f"cluster-{i}" for i in range(1, 8) if f"cluster-{i}" != first]
        return [first] + rest

    def fetch_outages(self):
        ctx = self._prepare()
        if "layer" not in ctx:            # non-layered providers (ifactor)
            incidents, answered = self._descend(ctx)
            return self._finish(incidents)
        for i, layer in enumerate(self._layer_candidates()):
            ctx["layer"] = layer
            incidents, answered = self._descend(ctx)
            if answered:
                if i > 0:
                    print(f"[i] [{self.region['name']}] layer auto-detected: "
                          f"{layer} (remember it: set 'kubra_layer': "
                          f"'{layer}' on this region in gridwatch_config.json)")
                    self.region["kubra_layer"] = layer
                return self._finish(incidents)
        print(f"[i] [{self.region['name']}] no tiles on any layer — region "
              f"is quiet (0 outages) across all cluster layers.")
        return self._finish({})

    def _finish(self, incidents):
        """Label each outage. FirstEnergy serves ONE dataset across all its
        state views, so we sweep once and assign a state label by point-in-
        bbox against `sub_regions` (checked in order; first match wins).
        This removes the duplicate sweeps and the cross-region double counting
        that separate per-state regions caused."""
        subs = self.region.get("sub_regions") or []
        counts = {}
        for o in incidents.values():
            label = self.region["name"]
            for s in subs:
                b = s["bbox"]
                if (b["south"] <= o["lat"] <= b["north"]
                        and b["west"] <= o["lon"] <= b["east"]):
                    label = s["name"]
                    break
            o["region"] = label
            counts[label] = counts.get(label, 0) + 1
        if subs and counts:
            print("[i] [{}] states: {}".format(
                self.region["name"],
                ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))))
        return list(incidents.values())

    def _descend(self, ctx):
        """One full quadkey descent.

        Records are collected per tile, then resolved bottom-up:

            resolve(T) = union(resolve(children)) IF the children's records
                         account for at least (1 - tol) of T's customers,
                         otherwise T's own records.

        This is loss-safe in both directions. A parent is only discarded when
        its children demonstrably re-emit the same outages at higher
        resolution (customer totals reconcile); if a child tile answers empty
        or only partially covers the parent, the parent's records are kept.
        Nothing is ever counted twice — every subtree yields either the parent
        or the children, never both.

        Returns (incidents dict, any_tile_ever).
        """
        cfg = self.cfg
        bbox = self.region.get("bbox") or cfg["bbox"]
        tile_records = {}       # qk -> [rec, ...]  (only tiles WITH records)
        any_ever = False
        out_of_box = 0
        tol = cfg.get("supersede_tolerance", 0.98)

        def key(rec):
            return f"{rec['outage_id']}@{rec['lat']:.4f},{rec['lon']:.4f}"

        def cust(recs):
            return sum(r.get("customers") or 0 for r in recs)

        zoom = cfg["zoom_min"]
        top_zoom = None
        frontier = quadkeys_for_bbox(bbox, zoom)
        tiles_requested = 0
        budget = (self.region.get("max_tiles")
                  or cfg.get("max_tiles_per_region", 20000))
        while zoom <= cfg["zoom_max"] and frontier:
            if tiles_requested >= budget:
                print(f"[!] [{self.region['name']}] tile budget ({budget}) "
                      f"exhausted at zoom {zoom} — COVERAGE INCOMPLETE. "
                      f"Raise 'max_tiles' for this region.")
                break
            if tiles_requested + len(frontier) > budget:
                allowed = budget - tiles_requested
                print(f"[!] [{self.region['name']}] budget caps zoom {zoom} at "
                      f"{allowed}/{len(frontier)} — COVERAGE INCOMPLETE.")
                frontier = frontier[:allowed]

            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(
                    max_workers=cfg.get("tile_workers", 8)) as tp:
                payloads = list(tp.map(lambda q: self._fetch_tile(ctx, q),
                                       frontier))
            tiles_requested += len(frontier)

            next_frontier, n_rec, n_tiles = [], 0, 0
            for qk, payload in zip(frontier, payloads):
                if payload is None:
                    continue
                any_ever = True
                n_tiles += 1
                recs = [r for r in
                        (self._parse_item(i)
                         for i in payload.get("file_data", []))
                        if r]
                if recs:
                    tile_records[qk] = recs
                    n_rec += len(recs)
                    if top_zoom is None:
                        top_zoom = zoom
                    next_frontier.extend(qk + d for d in "0123")

            print(f"[i] [{self.region['name']}] zoom {zoom}: "
                  f"{len(frontier)} req, {n_tiles} ok, {n_rec} records")

            if not tile_records:          # nothing found anywhere yet
                if zoom < cfg.get("widen_max_zoom", 10):
                    next_frontier = quadkeys_for_bbox(bbox, zoom + 1)
                else:
                    break
            frontier, zoom = next_frontier, zoom + 1

        if not tile_records:
            return {}, any_ever

        def resolve(qk):
            mine = tile_records.get(qk, [])
            kids = [qk + d for d in "0123" if (qk + d) in tile_records]
            if kids:
                kid_recs = []
                for k in kids:
                    kid_recs.extend(resolve(k))
                if not mine or cust(kid_recs) >= cust(mine) * tol:
                    return kid_recs          # children fully account for me
                # children under-account (partial pyramid) — trust myself
            return mine

        # Roots = tiles with NO ancestor that also holds records. (Using
        # "shallowest zoom" instead would orphan records that sit under a
        # border tile whose own records were all filtered out by the bbox.)
        roots = [qk for qk in tile_records
                 if not any(qk[:i] in tile_records for i in range(1, len(qk)))]
        resolved = {}
        coarse_cust = sum(cust(tile_records[qk]) for qk in roots)
        for qk in roots:
            for rec in resolve(qk):
                resolved[key(rec)] = rec
        # Border tiles legitimately carry neighbouring states' outages (a
        # zoom-8 tile is ~90 miles wide, and FE serves one dataset across its
        # views). Reconciliation above needed those records; the map does not.
        # Filter to this region's bbox now that the tree is resolved.
        incidents = {}
        for k, rec in resolved.items():
            if bbox and not (bbox["south"] <= rec["lat"] <= bbox["north"]
                             and bbox["west"] <= rec["lon"] <= bbox["east"]):
                out_of_box += 1
                continue
            incidents[k] = rec
        final_cust = sum(r.get("customers") or 0 for r in incidents.values())
        if out_of_box:
            print(f"[i] [{self.region['name']}] dropped {out_of_box} outages "
                  f"outside region bbox (border-tile overlap)")
        resolved_cust = sum(r.get("customers") or 0 for r in resolved.values())
        flag = ""
        if coarse_cust and resolved_cust < coarse_cust * 0.95:
            flag = (f"  [!] resolved customers {resolved_cust} < top-zoom "
                    f"{coarse_cust} — possible pyramid gap")
        print(f"[i] [{self.region['name']}] {len(incidents)} outages, "
              f"{final_cust} customers (top-zoom total {coarse_cust}, "
              f"{tiles_requested} tiles){flag}")
        return incidents, any_ever

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


class DukeAPI(Kubra):
    """Duke Energy outage-maps REST API (jurisdiction DEM = Ohio/Kentucky).

    Discovered from a live browser session on
    outagemap.duke-energy.com/#/current-outages/ohky: the SPA calls an Apigee
    gateway directly, with no KUBRA storm center and no tile pyramid:

        GET https://prod.apigee.duke-energy.app/outage-maps/v1/outages
            ?jurisdiction=DEM

    Sibling endpoints seen in the same session (unused, but documented):
        /outage-maps/v1/jurisdictions/DEM
        /outage-maps/v1/counties?jurisdiction=DEM
        /outage-maps/v1/alerts?jurisdiction=DEM
        /outage-maps/v1/mapsettings?jurisdiction=DEM

    No auth request appeared in the capture, so we call it plainly with
    browser-ish headers. If the gateway ever answers 401/403, set
    'config_url' on the region and we'll harvest consumer keys from it and
    retry with Basic auth (the older cust-api pattern).
    """

    API_BASE = "https://prod.apigee.duke-energy.app/outage-maps/v1"

    def _headers(self):
        """Duke's Apigee gateway REQUIRES Basic auth — a bare call fails with
        'Unresolved variable : request.header.Authorization'. The token is a
        static consumer key:secret pair baked into their public JS bundle and
        sent by their own map. It's their credential, so we read it from the
        environment (GitHub Actions secret DUKE_BASIC_TOKEN) rather than
        committing it. Fallback: region['basic_token'] or a config_url the
        keys can be harvested from."""
        h = {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://outagemap.duke-energy.com",
            "Referer": "https://outagemap.duke-energy.com/",
            "User-Agent": IFactor.BROWSER_UA,
        }
        token = (os.environ.get("DUKE_BASIC_TOKEN")
                 or self.region.get("basic_token"))
        if token:
            token = token.strip()
            if not token.lower().startswith("basic "):
                token = f"Basic {token}"
            h["Authorization"] = token
        return h

    def _basic_auth_retry(self, url):
        """Only used if the plain call is rejected AND a config_url is set."""
        import base64
        cfg_url = self.region.get("config_url")
        if not cfg_url:
            return None
        try:
            c = self.s.get(cfg_url, timeout=30)
            c.raise_for_status()
            cj = c.json()
            key = cj.get("consumer_key_emp") or cj.get("consumer_key")
            sec = cj.get("consumer_secret_emp") or cj.get("consumer_secret")
            if not (key and sec):
                print(f"[!] [{self.region['name']}] config_url had no consumer keys")
                return None
            tok = base64.b64encode(f"{key}:{sec}".encode()).decode()
            r = self.s.get(url, timeout=30,
                           headers={**self._headers(),
                                    "Authorization": f"Basic {tok}"})
            r.raise_for_status()
            print(f"[i] [{self.region['name']}] authorized via Basic token")
            return r
        except requests.RequestException as e:
            print(f"[!] [{self.region['name']}] basic-auth retry failed: {e}")
            return None

    @staticmethod
    def _pick(d, *names):
        for n in names:
            v = d.get(n)
            if v not in (None, ""):
                return v
        return None

    def _coords(self, ev):
        """Duke has used several coordinate shapes; accept the known ones."""
        lat = self._pick(ev, "deviceLatitudeLocation", "latitude", "lat")
        lon = self._pick(ev, "deviceLongitudeLocation", "longitude", "lon", "lng")
        if lat is None or lon is None:
            geom = ev.get("geometry") or ev.get("location") or {}
            if isinstance(geom, dict):
                coords = geom.get("coordinates")
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]   # GeoJSON order
                else:
                    lat = lat if lat is not None else geom.get("lat")
                    lon = lon if lon is not None else geom.get("lng") or geom.get("lon")
        try:
            return float(lat), float(lon)
        except (TypeError, ValueError):
            return None, None

    def fetch_outages(self):
        name = self.region["name"]
        juris = self.region.get("jurisdiction", "DEM")
        url = f"{self.API_BASE}/outages?jurisdiction={juris}"
        self.s.headers.update(self._headers())
        try:
            r = self.s.get(url, timeout=30)
            if r.status_code in (400, 401, 403):
                if "Authorization" not in self.s.headers:
                    print(f"[!] [{name}] {r.status_code}: Duke requires Basic "
                          f"auth and no token is set. Add repo secret "
                          f"DUKE_BASIC_TOKEN (the 'Basic ...' value their map "
                          f"sends) and expose it as an env var in the workflow.")
                print(f"[!] [{name}] outages endpoint returned "
                      f"{r.status_code}; trying config-based auth")
                r2 = self._basic_auth_retry(url)
                if r2 is None:
                    raise RuntimeError(
                        f"{r.status_code} from Duke API (no usable auth). If "
                        f"this is a datacenter-IP block, poll Duke from a "
                        f"residential IP and push results.")
                r = r2
            else:
                r.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Duke request failed: {e}")

        payload = r.json()
        data = payload.get("data", payload if isinstance(payload, list) else [])
        if isinstance(data, dict):
            data = data.get("outages") or data.get("events") or []
        print(f"[i] [{name}] duke api: {len(data)} raw records "
              f"(jurisdiction {juris})")

        bbox = self.region.get("bbox") or {}
        out, skipped, sample_printed = [], 0, False
        for i, ev in enumerate(data):
            if not isinstance(ev, dict):
                continue
            lat, lon = self._coords(ev)
            if lat is None:
                skipped += 1
                if not sample_printed:
                    print(f"[!] [{name}] no coords in record; keys="
                          f"{sorted(ev.keys())[:14]}")
                    sample_printed = True
                continue
            if bbox and not (bbox["south"] <= lat <= bbox["north"]
                             and bbox["west"] <= lon <= bbox["east"]):
                continue
            cust = self._pick(ev, "customersAffectedNumber",
                              "customersAffected", "custAffected") or 0
            if isinstance(cust, dict):
                cust = cust.get("value") or cust.get("val") or 0
            out.append({
                "outage_id": str(self._pick(ev, "sourceEventNumber",
                                            "eventNumber", "outageId",
                                            "id") or f"duke-{i}"),
                "lat": round(lat, 5), "lon": round(lon, 5),
                "customers": int(cust) if str(cust).isdigit() else 0,
                "cause": str(self._pick(ev, "outageCause",
                                        "convertedOutageCauseCode",
                                        "cause") or ""),
                "crew_status": str(self._pick(ev, "crewStatus", "deviceStatus",
                                              "status") or ""),
                "etr": str(self._pick(ev, "estimatedRestorationTime",
                                      "etr", "etrOverride") or ""),
                "cluster": False,
                "region": name,
            })
        print(f"[i] [{name}] parsed outages: {len(out)}"
              + (f" ({skipped} unparseable)" if skipped else ""))
        return out


class AESXml(Kubra):
    """AES Ohio (Dayton) legacy DP&L OMS feed: a single XML document at
    /DATA/DPLOMSDATA.xml with <Markers> point incidents. No auth."""

    def fetch_outages(self):
        import xml.etree.ElementTree as ET
        url = self.region["url"]
        self.s.headers["User-Agent"] = IFactor.BROWSER_UA
        r = self.s.get(url, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        bbox = self.region.get("bbox") or {}
        out = []
        for m in root.findall("Markers"):
            def g(tag):
                el = m.find(tag)
                return el.text.strip() if el is not None and el.text else ""
            try:
                lat, lon = float(g("LAT")), float(g("LNG"))
            except ValueError:
                continue
            if bbox and not (bbox["south"] <= lat <= bbox["north"]
                             and bbox["west"] <= lon <= bbox["east"]):
                continue
            cust = g("TOTALCUSTS")
            out.append({"outage_id": g("INCIDENTID") or f"{lat},{lon}",
                        "lat": round(lat, 5), "lon": round(lon, 5),
                        "customers": int(cust) if cust.isdigit() else 0,
                        "cause": "",
                        "crew_status": g("COUNTY").title(),
                        "etr": g("EstimateTime"),
                        "cluster": False,
                        "region": self.region["name"]})
        print(f"[i] [{self.region['name']}] aes xml: {len(out)} incidents")
        return out


class XmlFeed(Kubra):
    """Generic single-file XML outage feed (NOVEC stormcenter style:
    /stormcenter/data/outagedtl.xml). Root observed live as <outages/>;
    child schema unknown until outages exist, so field extraction is
    heuristic over both child tags and attributes, and unparseable records
    are dumped to the log for a one-paste fix."""

    LAT_KEYS = ("lat", "latitude", "y")
    LON_KEYS = ("lon", "lng", "long", "longitude", "x")
    CUST_KEYS = ("consumersaffected", "custaffected", "customersaffected",
                 "consumers", "custout", "numout", "affected", "cust")
    ID_KEYS = ("id", "outageid", "outage_id", "case", "casenumber", "number",
               "incidentid")
    ETR_KEYS = ("etr", "estimatedrestoration", "est_restore", "restoretime",
                "esttime")
    CAUSE_KEYS = ("cause", "reason")

    def fetch_outages(self):
        import xml.etree.ElementTree as ET
        url = self.region["url"]
        self.s.headers.update({"User-Agent": IFactor.BROWSER_UA,
                               "Referer": self.region.get("entry", url)})
        r = self.s.get(f"{url}?{int(time.time()*1000)}", timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        bbox = self.region.get("bbox") or {}

        def fields(el):
            d = {k.lower(): (v or "").strip() for k, v in el.attrib.items()}
            for c in el:
                tag = c.tag.split("}")[-1].lower()
                if c.text and c.text.strip():
                    d.setdefault(tag, c.text.strip())
                d.update({f"{tag}.{k.lower()}": (v or "").strip()
                          for k, v in c.attrib.items()})
            return d

        def pick(d, keys):
            for k in keys:
                for dk, v in d.items():
                    if dk == k or dk.endswith("." + k):
                        if v:
                            return v
            return ""

        out, dumped = [], False
        for el in list(root):
            d = fields(el)
            try:
                lat = float(pick(d, self.LAT_KEYS))
                lon = float(pick(d, self.LON_KEYS))
            except ValueError:
                if not dumped:
                    import xml.etree.ElementTree as ET2
                    print(f"[!] [{self.region['name']}] unparsed record — "
                          f"paste this to fix the mapping: "
                          f"{ET2.tostring(el, encoding='unicode')[:400]}")
                    dumped = True
                continue
            if bbox and not (bbox["south"] <= lat <= bbox["north"]
                             and bbox["west"] <= lon <= bbox["east"]):
                continue
            cust = pick(d, self.CUST_KEYS)
            out.append({"outage_id": pick(d, self.ID_KEYS) or f"{lat},{lon}",
                        "lat": round(lat, 5), "lon": round(lon, 5),
                        "customers": int(float(cust)) if cust else 0,
                        "cause": pick(d, self.CAUSE_KEYS),
                        "crew_status": "",
                        "etr": pick(d, self.ETR_KEYS),
                        "cluster": False,
                        "region": self.region["name"]})
        print(f"[i] [{self.region['name']}] xml feed: {len(out)} outages")
        return out


class PPLOmap(Kubra):
    """PPL Electric (+ Rhode Island Energy) custom OMAP API.
      GET https://omap.prod.pplweb.com/OMAP/api/Omap/Outage/Clusters/{zoom}?opco=PA
    Flat JSON array of {id, a:lat, o:lon, nc:customersOut}. id==0 is an
    unexpanded aggregate cluster; higher zoom splits it. We fetch coarse,
    then re-request only cluster cells at deeper zoom until they resolve to
    id!=0 points or we hit zoom cap. No per-tile URLs — zoom is a single
    query param, so 'descent' here means bumping the zoom for the whole map
    and de-duplicating by (id, rounded lat/lon)."""

    API = "https://omap.prod.pplweb.com/OMAP/api/Omap/Outage/Clusters"

    def fetch_outages(self):
        opco = self.region.get("opco", "PA")
        self.s.headers.update({"Accept": "application/json",
                               "User-Agent": IFactor.BROWSER_UA,
                               "Referer": self.region.get("entry", self.API)})
        bbox = self.region.get("bbox") or {}
        zmin = self.region.get("omap_zoom_min", 8)
        zmax = self.region.get("omap_zoom_max", 14)
        seen, out, cluster_seen = {}, [], False
        for z in range(zmin, zmax + 1):
            try:
                r = self.s.get(f"{self.API}/{z}?opco={opco}", timeout=30)
                r.raise_for_status()
                rows = r.json()
            except (requests.RequestException, ValueError) as e:
                print(f"[!] [{self.region['name']}] omap zoom {z}: {e}")
                continue
            zc = 0
            for it in rows:
                try:
                    lat = float(it["a"]); lon = float(it["o"])
                except (KeyError, TypeError, ValueError):
                    continue
                nc = it.get("nc") or 0
                oid = it.get("id", 0)
                if oid == 0:      # unexpanded cluster — count once, keep drilling
                    zc += 1; cluster_seen = True
                    continue
                if bbox and not (bbox["south"] <= lat <= bbox["north"]
                                 and bbox["west"] <= lon <= bbox["east"]):
                    continue
                k = f"{oid}"
                seen[k] = {"outage_id": str(oid), "lat": round(lat, 5),
                           "lon": round(lon, 5),
                           "customers": int(nc) if nc else 0,
                           "cause": "", "crew_status": "", "etr": "",
                           "cluster": False, "region": self.region["name"]}
            print(f"[i] [{self.region['name']}] omap zoom {z}: "
                  f"{len(rows)} cells, {zc} clusters, {len(seen)} points")
            if zc == 0 and z > zmin:
                break     # everything resolved to points
        out = list(seen.values())
        # if the deepest zoom STILL had clusters, surface aggregate customer
        # counts so a big storm cluster isn't dropped to zero
        if cluster_seen:
            print(f"[i] [{self.region['name']}] residual clusters at "
                  f"zoom {zmax}; appending centroids so customers aren't lost")
            try:
                r = self.s.get(f"{self.API}/{zmax}?opco={opco}", timeout=30)
                for it in r.json():
                    if it.get("id"):      # real points already collected
                        continue
                    lat, lon = float(it["a"]), float(it["o"])
                    if bbox and not (bbox["south"] <= lat <= bbox["north"]
                                     and bbox["west"] <= lon <= bbox["east"]):
                        continue
                    out.append({"outage_id": f"cluster-{lat:.3f},{lon:.3f}",
                                "lat": round(lat, 5), "lon": round(lon, 5),
                                "customers": int(it.get("nc") or 0),
                                "cause": "", "crew_status": "", "etr": "",
                                "cluster": True, "region": self.region["name"]})
            except (requests.RequestException, ValueError):
                pass
        print(f"[i] [{self.region['name']}] omap: {len(out)} outages")
        return out


class AvangridAPI(Kubra):
    """Avangrid utilities (NYSEG, RG&E, CMP). REST behind an Apigee-style
    gateway needing an ocp-apim-subscription-key (public, from their JS).
      GET https://apim.avangrid.com/{opco}/v1/public/outagedata?...&filter=county
    NOTE: filter=county returns COUNTY AGGREGATES (centroids + totals), not
    point outages — the only public granularity. We emit county centroids as
    cluster points and label them so the map/UI don't imply false precision.
    Key from env AVANGRID_KEY or region['subscription_key']."""

    API = "https://apim.avangrid.com"

    def fetch_outages(self):
        opco = self.region.get("opco")
        key = (os.environ.get("AVANGRID_KEY")
               or self.region.get("subscription_key"))
        if not (opco and key):
            raise RuntimeError(
                f"[{self.region['name']}] needs opco + subscription key "
                f"(set repo secret AVANGRID_KEY, exposed as env var).")
        b = self.region.get("bbox") or {}
        params = (f"north={b.get('north',45)}&south={b.get('south',40)}"
                  f"&east={b.get('east',-73)}&west={b.get('west',-80)}"
                  f"&filter=county")
        self.s.headers.update({
            "Accept": "application/json",
            "ocp-apim-subscription-key": key,
            "Origin": self.region.get("origin", "https://portal.nyseg.com"),
            "Referer": self.region.get("entry", ""),
            "User-Agent": IFactor.BROWSER_UA})
        r = self.s.get(f"{self.API}/{opco}/v1/public/outagedata?{params}",
                       timeout=30)
        r.raise_for_status()
        data = r.json()
        rows = (data.get("counties") or data.get("data")
                or data.get("outages") or (data if isinstance(data, list) else []))
        out = []
        for c in rows if isinstance(rows, list) else []:
            lat = c.get("lat") or c.get("latitude") or (c.get("centroid") or {}).get("lat")
            lon = c.get("lon") or c.get("lng") or c.get("longitude") or (c.get("centroid") or {}).get("lon")
            cust = (c.get("customersAffected") or c.get("custOut")
                    or c.get("numOut") or c.get("affected") or 0)
            if lat is None or lon is None:
                continue
            name = c.get("county") or c.get("name") or "county"
            out.append({"outage_id": f"county-{name}",
                        "lat": round(float(lat), 5), "lon": round(float(lon), 5),
                        "customers": int(cust) if cust else 0,
                        "cause": "", "crew_status": "",
                        "etr": "", "cluster": True,
                        "region": self.region["name"]})
        print(f"[i] [{self.region['name']}] avangrid: {len(out)} county "
              f"aggregates (not point-level)")
        return out


def make_provider(cfg, region):
    p = region.get("provider", "kubra")
    if p == "ifactor":
        return IFactor(cfg, region=region)
    if p == "duke":
        return DukeAPI(cfg, region=region)
    if p == "aesxml":
        return AESXml(cfg, region=region)
    if p == "xmlfeed":
        return XmlFeed(cfg, region=region)
    if p == "pplomap":
        return PPLOmap(cfg, region=region)
    if p == "avangrid":
        return AvangridAPI(cfg, region=region)
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
        time.sleep(0.02)
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
        time.sleep(0.02)
        return events

    def prefetch(self, points, workers=6):
        """Warm the per-cell caches for a batch of (lat, lon) points in
        parallel. Enrichment afterwards is pure cache hits."""
        from concurrent.futures import ThreadPoolExecutor
        cells = sorted({self._cell(la, lo) for la, lo in points})
        if not cells:
            return
        print(f"[i] prefetching weather for {len(cells)} grid cells")
        with ThreadPoolExecutor(max_workers=workers) as tp:
            list(tp.map(lambda c: (self.conditions(*c),
                                   self.active_alerts(*c)), cells))


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
              if s.get("lat")
              and (s.get("status") in ("construction", "contested", "announced")
                   or s.get("powerlines"))]
    if not active:
        return False
    deg = radius_km / 111.0
    parts = []
    for s in active:
        box = (f"{s['lat']-deg},{s['lon']-deg/0.75},"
               f"{s['lat']+deg},{s['lon']+deg/0.75}")
        parts.append(f'way["power"="line"]({box});')
        parts.append(f'nwr["power"="substation"]({box});')
        parts.append(f'nwr["power"="plant"]({box});')
    q = f"[out:json][timeout:120];({''.join(parts)});out geom center tags;"
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=120,
                          headers={"User-Agent": cfg["nws_user_agent"]})
        r.raise_for_status()
        elements = r.json().get("elements", [])
    except Exception as e:
        print(f"[!] Overpass powerlines failed (non-fatal): {e}")
        return False
    lines, subs, plants, seen = [], [], [], set()
    for el in elements:
        eid = (el.get("type"), el.get("id"))
        if eid in seen:
            continue
        seen.add(eid)
        t = el.get("tags", {}) or {}
        p = t.get("power")
        if p == "line" and el.get("type") == "way":
            geom = [[round(pt["lat"], 5), round(pt["lon"], 5)]
                    for pt in el.get("geometry", [])]
            if len(geom) >= 2:
                lines.append({"id": el["id"], "name": t.get("name", ""),
                              "voltage": t.get("voltage", ""),
                              "operator": t.get("operator", ""),
                              "cables": t.get("cables", ""), "geom": geom})
            continue
        # substations / plants: nodes carry lat/lon; ways/relations a center
        la = el.get("lat") or (el.get("center") or {}).get("lat")
        lo = el.get("lon") or (el.get("center") or {}).get("lon")
        if la is None:
            continue
        item = {"id": el["id"], "name": t.get("name", ""),
                "operator": t.get("operator", ""),
                "voltage": t.get("voltage", ""),
                "lat": round(la, 5), "lon": round(lo, 5)}
        if p == "substation":
            subs.append(item)
        elif p == "plant":
            item["source"] = t.get("plant:source", "")
            item["output"] = t.get("plant:output:electricity", "")
            plants.append(item)
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "radius_km": radius_km,
               "note": "OSM power=line ways within radius of active-change "
                       "sites. Crowd-mapped visible infrastructure. Which "
                       "specific line FEEDS a site needs the interconnection "
                       "docket; this shows what's physically nearby.",
               "lines": lines, "substations": subs, "plants": plants}
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"[i] powerlines: {len(lines)} lines, {len(subs)} substations, "
          f"{len(plants)} plants -> {out_path}")
    return True


def _auto_layer_stale(path, max_age_hours=None):
    if not os.path.exists(path):
        return True
    try:
        with open(path) as f:
            gen = json.load(f).get("generated_at", "")
        age = datetime.now(timezone.utc) - datetime.fromisoformat(gen)
        if max_age_hours is not None:
            return age > timedelta(hours=max_age_hours)
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
    p = r.get("provider", "kubra")
    if p == "ifactor":
        return bool(r.get("base"))
    if p == "duke":
        return True
    if p in ("aesxml", "xmlfeed"):
        return bool(r.get("url"))
    if p == "pplomap":
        return True
    if p == "avangrid":
        return bool(r.get("opco"))
    return bool(r.get("instance_id") and r.get("view_id"))


def _regions(cfg):
    only = os.environ.get("GRIDWATCH_REGIONS", "").strip()
    wanted = {x.strip() for x in only.split(",") if x.strip()} if only else None
    regs = [r for r in cfg.get("regions", [])
            if _region_ready(r)
            and r.get("enabled", True)
            and (wanted is None or r["name"] in wanted)]
    if wanted:
        print(f"[i] region filter active: {sorted(wanted)}")
    if not regs and cfg.get("kubra_instance_id"):
        regs = [{"name": "OH", "entry": cfg.get("utility_entry_url", ""),
                 "instance_id": cfg["kubra_instance_id"],
                 "view_id": cfg["kubra_view_id"], "bbox": cfg.get("bbox")}]
    skipped = [r["name"] for r in cfg.get("regions", []) if not _region_ready(r)]
    if skipped:
        print(f"[i] regions without GUIDs (skipped): {', '.join(skipped)} — "
              f"grab from DevTools on each state's outage page")
    return regs


OVERPASS_ENDPOINTS = ["https://overpass-api.de/api/interpreter",
                      "https://overpass.kumi.systems/api/interpreter"]
OVERPASS_BATCH = 3      # regions per poll — spread heavy pulls across runs


def _overpass_query(cfg, q):
    """One Overpass query with endpoint rotation and a single 429 backoff.
    Free shared infrastructure: 25 heavy queries in one run got us 504/429'd,
    so collection is incremental (OVERPASS_BATCH regions/poll, resumed via a
    'pending' list persisted in the output file)."""
    import requests as rq
    last = None
    for ep in OVERPASS_ENDPOINTS:
        for attempt in (1, 2):
            try:
                r = rq.post(ep, data={"data": q}, timeout=180,
                            headers={"User-Agent": cfg.get(
                                "nws_user_agent", "GRIDWATCH")})
                if r.status_code == 429 and attempt == 1:
                    time.sleep(20)
                    continue
                r.raise_for_status()
                return r.json().get("elements", [])
            except Exception as e:
                last = e
                break
    raise RuntimeError(last)


def _layer_pending(path):
    try:
        with open(path) as f:
            return bool(json.load(f).get("pending"))
    except Exception:
        return False


def fetch_backbone(cfg, regions, out_path):
    """>=345kV transmission backbone across polled bboxes — collected
    incrementally (OVERPASS_BATCH regions/poll, resume via 'pending')."""
    state = {"lines": [], "pending": None}
    try:
        with open(out_path) as f:
            state = json.load(f)
    except Exception:
        pass
    named = [r for r in regions if r.get("bbox")]
    if state.get("pending") is None:
        state["pending"] = [r["name"] for r in named]
        state["lines"] = []
    pending = [r for r in named if r["name"] in state["pending"]]
    if not pending:
        return True
    VOLT = "345000|380000|400000|500000|765000"
    have = {l.get("id") for l in state.get("lines", [])}
    done_now = []
    for reg in pending[:OVERPASS_BATCH]:
        b = reg["bbox"]
        q = (f'[out:json][timeout:120];way["power"="line"]'
             f'["voltage"~"{VOLT}"]({b["south"]},{b["west"]},'
             f'{b["north"]},{b["east"]});out geom tags;')
        try:
            els = _overpass_query(cfg, q)
        except Exception as e:
            print(f"[!] backbone {reg['name']} failed (stays pending): {e}")
            continue
        for el in els:
            if el.get("id") in have:
                continue
            have.add(el.get("id"))
            t = el.get("tags", {}) or {}
            pts = [[round(p["lat"], 4), round(p["lon"], 4)]
                   for p in el.get("geometry", [])]
            geom = pts[::2] if len(pts) >= 4 else pts
            if len(geom) < 2:
                continue
            v = 0
            for tok in str(t.get("voltage", "")).split(";"):
                try:
                    v = max(v, int(tok))
                except ValueError:
                    pass
            state["lines"].append({"id": el.get("id"), "v": v,
                                   "name": t.get("name", ""),
                                   "operator": t.get("operator", ""),
                                   "geom": geom})
        done_now.append(reg["name"])
        time.sleep(5)
    state["pending"] = [n for n in state["pending"] if n not in done_now]
    state["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["min_voltage"] = 345000
    payload = json.dumps(state, separators=(",", ":"))
    if len(payload) > 9_000_000:
        print(f"[!] backbone {len(payload)/1e6:.1f}MB > guard — not written")
        return False
    with open(out_path, "w") as f:
        f.write(payload)
    print(f"[i] backbone: +{len(done_now)} regions this poll, "
          f"{len(state['lines'])} segments total, "
          f"{len(state['pending'])} regions pending")
    return True


def fetch_grid_assets(cfg, regions, out_path):
    """Every mapped substation + power plant across polled bboxes — the full
    public topology (true node-level data is CEII; OSM is the public
    reconstruction). Incremental like the backbone."""
    state = {"substations": [], "plants": [], "pending": None}
    try:
        with open(out_path) as f:
            state = json.load(f)
    except Exception:
        pass
    named = [r for r in regions if r.get("bbox")]
    if state.get("pending") is None:
        state["pending"] = [r["name"] for r in named]
        state["substations"], state["plants"] = [], []
    pending = [r for r in named if r["name"] in state["pending"]]
    if not pending:
        return True
    have = {(s.get("t"), s.get("id"))
            for s in state.get("substations", []) + state.get("plants", [])}
    done_now = []
    for reg in pending[:OVERPASS_BATCH]:
        b = reg["bbox"]
        box = f'{b["south"]},{b["west"]},{b["north"]},{b["east"]}'
        q = (f'[out:json][timeout:150];('
             f'nwr["power"="substation"]({box});'
             f'nwr["power"="plant"]({box}););out center tags;')
        try:
            els = _overpass_query(cfg, q)
        except Exception as e:
            print(f"[!] grid-assets {reg['name']} failed (stays pending): {e}")
            continue
        for el in els:
            k = (el.get("type"), el.get("id"))
            if k in have:
                continue
            have.add(k)
            t = el.get("tags", {}) or {}
            la = el.get("lat") or (el.get("center") or {}).get("lat")
            lo = el.get("lon") or (el.get("center") or {}).get("lon")
            if la is None:
                continue
            v = 0
            for tok in str(t.get("voltage", "")).split(";"):
                try:
                    v = max(v, int(tok))
                except ValueError:
                    pass
            item = {"t": el.get("type"), "id": el.get("id"),
                    "n": t.get("name", ""), "o": t.get("operator", ""),
                    "v": v, "lat": round(la, 4), "lon": round(lo, 4)}
            if t.get("power") == "substation":
                state["substations"].append(item)
            else:
                item["src"] = t.get("plant:source", "")
                item["out"] = t.get("plant:output:electricity", "")
                state["plants"].append(item)
        done_now.append(reg["name"])
        time.sleep(5)
    state["pending"] = [n for n in state["pending"] if n not in done_now]
    state["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["note"] = "OSM reconstruction; true topology is CEII"
    payload = json.dumps(state, separators=(",", ":"))
    if len(payload) > 9_000_000:
        state["substations"] = [s for s in state["substations"]
                                if s["v"] >= 100000]
        payload = json.dumps(state, separators=(",", ":"))
    with open(out_path, "w") as f:
        f.write(payload)
    print(f"[i] grid assets: +{len(done_now)} regions this poll, "
          f"{len(state['substations'])} subs / {len(state['plants'])} plants, "
          f"{len(state['pending'])} pending")
    return True


def fetch_nyiso_lmps(cfg, out_path):
    """Daily NYISO day-ahead zonal LBMP pull — fully public CSVs, no key,
    no redistribution restriction (unlike PJM DM2). Zone WEST hosts the
    Lake Mariner AI campus; the NYC-vs-WEST spread is the 'data centers
    chase cheap upstate power' story in dollars."""
    import requests as rq, csv, io
    from datetime import datetime, timedelta, timezone
    day = datetime.now(timezone.utc) - timedelta(days=1)
    ymd = day.strftime("%Y%m%d")
    url = f"http://mis.nyiso.com/public/csv/damlbmp/{ymd}damlbmp_zone.csv"
    try:
        r = rq.get(url, timeout=60,
                   headers={"User-Agent": cfg.get("nws_user_agent",
                                                  "GRIDWATCH")})
        r.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(r.text)))
    except Exception as e:
        print(f"[!] NYISO flows failed (non-fatal): {e}")
        return False
    agg = {}
    for row in rows:
        zone = (row.get("Name") or "").strip()
        if not zone:
            continue
        try:
            lmp = float(row.get("LBMP ($/MWHr)"))
            cong = float(row.get("Marginal Cost Congestion ($/MWHr)") or 0)
        except (TypeError, ValueError):
            continue
        a = agg.setdefault(zone, {"n": 0, "lmp": 0.0, "cong": 0.0})
        a["n"] += 1; a["lmp"] += lmp; a["cong"] += cong
    zones = [{"zone": z, "avg_lmp": round(a["lmp"]/a["n"], 2),
              "avg_congestion": round(a["cong"]/a["n"], 2)}
             for z, a in agg.items() if a["n"]]
    if not zones:
        print("[!] NYISO flows: CSV parsed but no zone rows — schema drift? "
              "First row keys: " + ", ".join(list(rows[0].keys())[:6] if rows else []))
        return False
    zones.sort(key=lambda x: -x["avg_lmp"])
    zl = {z["zone"]: z for z in zones}
    spread = None
    if "N.Y.C." in zl and "WEST" in zl:
        spread = round(zl["N.Y.C."]["avg_lmp"] - zl["WEST"]["avg_lmp"], 2)
    # append to history (the trend is the stat; one day is weather)
    hist = []
    try:
        with open(out_path) as f:
            hist = json.load(f).get("history", [])
    except Exception:
        pass
    dstr = day.strftime("%Y-%m-%d")
    hist = [h for h in hist if h.get("day") != dstr]
    hist.append({"day": dstr, "nyc_minus_west": spread,
                 "west": zl.get("WEST", {}).get("avg_lmp"),
                 "nyc": zl.get("N.Y.C.", {}).get("avg_lmp")})
    hist = sorted(hist, key=lambda h: h["day"])[-60:]
    with open(out_path, "w") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "day": dstr, "zones": zones, "nyc_minus_west": spread,
                   "history": hist}, f, separators=(",", ":"))
    print(f"[i] NYISO flows: {len(zones)} zones for {dstr}"
          + (f" | NYC-WEST spread ${spread}/MWh" if spread is not None else "")
          + f" | {len(hist)}d history")
    return True


def fetch_eia_demand(cfg, out_path):
    """Daily EIA-930 pull: hourly demand by PJM subregion, trailing 14 days,
    7d-vs-prior-7d per zone. US-government public-domain data — clean to
    publish, unlike PJM DM2. Free instant key: eia.gov/opendata/register.php
    -> repo secret EIA_API_KEY."""
    key = os.environ.get("EIA_API_KEY")
    if not key:
        print("[i] EIA demand: no EIA_API_KEY — skipping (free instant key "
              "at eia.gov/opendata/register.php)")
        return False
    import requests as rq
    from datetime import datetime as dt, timedelta as td, timezone as tz
    end = dt.now(tz.utc)
    start = end - td(days=14)
    ZONES = ["DOM", "ATSI", "AEP", "DAY", "DEOK", "PE", "PL", "PS", "JC",
             "BC", "PEP", "DPL", "AE", "ME", "PN", "APS", "DUQ"]
    base = [("api_key", key), ("frequency", "hourly"), ("data[0]", "value"),
            ("facets[parent][]", "PJM"),
            ("start", start.strftime("%Y-%m-%dT%H")),
            ("end", end.strftime("%Y-%m-%dT%H")),
            ("sort[0][column]", "period"), ("sort[0][direction]", "asc"),
            ("length", 5000)] + [("facets[subba][]", z) for z in ZONES]
    rows, offset = [], 0
    try:
        while True:
            r = rq.get("https://api.eia.gov/v2/electricity/rto/"
                       "region-sub-ba-data/data/",
                       params=base + [("offset", offset)], timeout=90)
            r.raise_for_status()
            chunk = (r.json().get("response") or {}).get("data") or []
            rows.extend(chunk)
            if len(chunk) < 5000 or offset > 40000:
                break
            offset += 5000
    except Exception as e:
        print(f"[!] EIA demand failed (non-fatal): {e}")
        return False
    mid = (end - td(days=7)).strftime("%Y-%m-%dT%H")
    agg = {}
    for row in rows:
        z = row.get("subba")
        try:
            v = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        half = "cur" if str(row.get("period", "")) >= mid else "prev"
        a = agg.setdefault(z, {"cur": [0, 0], "prev": [0, 0],
                               "name": row.get("subba-name") or z})
        a[half][0] += v; a[half][1] += 1
    zones = []
    for z, a in agg.items():
        cur = a["cur"][0] / a["cur"][1] if a["cur"][1] else None
        prev = a["prev"][0] / a["prev"][1] if a["prev"][1] else None
        if cur is None:
            continue
        zones.append({"zone": z, "name": a["name"], "avg_mw_7d": round(cur),
                      "delta_pct": (round(100 * (cur - prev) / prev, 1)
                                    if prev else None)})
    zones.sort(key=lambda x: -(x["avg_mw_7d"] or 0))
    with open(out_path, "w") as f:
        json.dump({"generated_at": dt.now(tz.utc).isoformat(timespec="seconds"),
                   "source": "EIA-930 (US public domain)", "zones": zones},
                  f, separators=(",", ":"))
    print(f"[i] EIA demand: {len(zones)} PJM subregions -> {out_path}")
    return bool(zones)


def fetch_eia_strain(cfg, out_path):
    """Grid-strain proxy from EIA-930 (public domain): actual demand (D) vs
    day-ahead forecast (DF) per balancing authority, trailing 72h. Sustained
    actual >= ~98% of forecast is the condition under which brownouts and
    conservation appeals happen. Honest label: 'strain conditions' — true
    voltage-sag data is utility-internal and not published anywhere."""
    key = os.environ.get("EIA_API_KEY")
    if not key:
        return False
    import requests as rq
    from datetime import datetime as dt, timedelta as td, timezone as tz
    end = dt.now(tz.utc)
    start = end - td(days=3)
    BAS = ["PJM", "NYIS", "ISNE"]
    params = [("api_key", key), ("frequency", "hourly"), ("data[0]", "value"),
              ("start", start.strftime("%Y-%m-%dT%H")),
              ("end", end.strftime("%Y-%m-%dT%H")),
              ("sort[0][column]", "period"), ("sort[0][direction]", "asc"),
              ("length", 5000)]
    params += [("facets[respondent][]", b) for b in BAS]
    params += [("facets[type][]", t) for t in ("D", "DF")]
    try:
        r = rq.get("https://api.eia.gov/v2/electricity/rto/region-data/data/",
                   params=params, timeout=90)
        r.raise_for_status()
        rows = (r.json().get("response") or {}).get("data") or []
    except Exception as e:
        print(f"[!] EIA strain failed (non-fatal): {e}")
        return False
    series = {}
    for row in rows:
        ba, typ, per = row.get("respondent"), row.get("type"), row.get("period")
        try:
            v = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        series.setdefault(ba, {}).setdefault(per, {})[typ] = v
    bas = []
    for ba, byper in series.items():
        ratios = []
        for per in sorted(byper):
            d, f = byper[per].get("D"), byper[per].get("DF")
            if d and f:
                ratios.append((per, 100.0 * d / f))
        if not ratios:
            continue
        recent = ratios[-48:]
        peak = max(recent, key=lambda x: x[1])
        bas.append({"ba": ba,
                    "latest_pct": round(recent[-1][1], 1),
                    "latest_period": recent[-1][0],
                    "max48_pct": round(peak[1], 1),
                    "peak_period": peak[0],
                    "hours_ge98": sum(1 for _, p in recent if p >= 98.0)})
    bas.sort(key=lambda x: -x["max48_pct"])
    with open(out_path, "w") as f:
        json.dump({"generated_at": dt.now(tz.utc).isoformat(timespec="seconds"),
                   "source": "EIA-930 D vs DF (US public domain)",
                   "bas": bas}, f, separators=(",", ":"))
    print(f"[i] EIA strain: {len(bas)} BAs -> {out_path}")
    return bool(bas)


def fetch_pjm_flows(cfg, out_path):
    """Daily PJM Data Miner 2 pull: yesterday's day-ahead hourly zonal LMPs,
    averaged per zone with the congestion component split out. This is the
    'who is pulling on the shared grid' layer — the Dominion-vs-Ohio spread
    is where NoVA data center load shows up in dollars. Needs the free
    Data Miner 2 subscription key in env PJM_API_KEY."""
    key = os.environ.get("PJM_API_KEY")
    if not key:
        print("[i] PJM flows: no PJM_API_KEY set — skipping (register free at "
              "dataminer2.pjm.com, put the key in a repo secret)")
        return False
    import requests as rq
    from datetime import datetime, timedelta, timezone
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%m/%d/%Y")
    params = {"rowCount": 50000, "startRow": 1, "download": "true",
              "datetime_beginning_ept": f"{day} 00:00to{day} 23:59",
              "type": "ZONE",
              "fields": "datetime_beginning_ept,pnode_name,type,"
                        "total_lmp_da,congestion_price_da"}
    try:
        r = rq.get("https://api.pjm.com/api/v1/da_hrl_lmps", params=params,
                   headers={"Ocp-Apim-Subscription-Key": key,
                            "User-Agent": cfg.get("nws_user_agent",
                                                  "GRIDWATCH")},
                   timeout=90)
        r.raise_for_status()
        data = r.json()
        rows = data if isinstance(data, list) else data.get("items", [])
    except Exception as e:
        print(f"[!] PJM flows failed (non-fatal): {e}")
        return False
    agg = {}
    for row in rows:
        z = row.get("pnode_name")
        try:
            lmp = float(row.get("total_lmp_da"))
            cong = float(row.get("congestion_price_da") or 0)
        except (TypeError, ValueError):
            continue
        a = agg.setdefault(z, {"n": 0, "lmp": 0.0, "cong": 0.0})
        a["n"] += 1; a["lmp"] += lmp; a["cong"] += cong
    zones = [{"zone": z, "avg_lmp": round(a["lmp"]/a["n"], 2),
              "avg_congestion": round(a["cong"]/a["n"], 2), "hours": a["n"]}
             for z, a in agg.items() if a["n"]]
    zones.sort(key=lambda x: -x["avg_lmp"])
    spread = None
    zl = {z["zone"]: z for z in zones}
    if "DOM" in zl and "ATSI" in zl:
        spread = round(zl["DOM"]["avg_lmp"] - zl["ATSI"]["avg_lmp"], 2)
    with open(out_path, "w") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "day": day, "zones": zones,
                   "dom_minus_atsi": spread}, f, separators=(",", ":"))
    print(f"[i] PJM flows: {len(zones)} zones for {day}"
          + (f" | DOM-ATSI spread ${spread}/MWh" if spread is not None else "")
          + f" -> {out_path}")
    return bool(zones)


def _fetch_region(cfg, region):
    try:
        outages = make_provider(cfg, region).fetch_outages()
        print(f"[i] {region['name']}: {len(outages)} unique outages")
        return {"outages": outages, "error": None}
    except Exception as e:
        print(f"[!] region {region['name']} failed: {e}")
        return {"outages": [], "error": f"{type(e).__name__}: {e}"[:160]}


def cmd_poll(cfg, emit_dir=None, no_db=False):
    from concurrent.futures import ThreadPoolExecutor
    regions = _regions(cfg)
    if not regions:
        sys.exit("[!] No regions configured with KUBRA GUIDs.")
    polled_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sites = load_datacenters()
    wx = Weather(cfg)
    conn = None if no_db else db()
    counts, emitted = {}, []
    print(f"[i] fetching {len(regions)} regions in parallel: "
          f"{', '.join(r['name'] for r in regions)}")
    with ThreadPoolExecutor(max_workers=min(9, len(regions))) as pool:
        results = list(pool.map(lambda r: _fetch_region(cfg, r), regions))
    region_results = [r["outages"] for r in results]
    feed_stats = [{"name": reg["name"],
                   "outages": len(res["outages"]),
                   "customers": sum(o.get("customers") or 0
                                    for o in res["outages"]),
                   "error": res["error"]}
                  for reg, res in zip(regions, results)]
    all_pts = [(o["lat"], o["lon"]) for outs in region_results for o in outs]
    wx.prefetch(all_pts)
    # Site pools are poll-level constants — build ONCE, not per outage.
    # Only active-change sites drive dc_flag / classification / alerts;
    # operating + control sites are context layers.
    ACTIVE_STATUSES = ("construction", "contested", "announced")
    usable = [s for s in sites
              if s.get("status") in ACTIVE_STATUSES and s.get("lat")]
    op_usable = [s for s in sites
                 if str(s.get("status", "")).startswith("operating")
                 and s.get("lat")
                 and not str(s.get("coverage", "")).startswith("context")]
    # merge OSM-mapped operating facilities so the pink rings the map draws
    # are the SAME set the classifier uses
    if emit_dir:
        try:
            with open(os.path.join(emit_dir, "datacenters_auto.json")) as f:
                auto_sites = json.load(f).get("sites", [])
            boxes = [r.get("bbox") for r in regions if r.get("bbox")]
            added = 0
            for a in auto_sites:
                la, lo = a.get("lat"), a.get("lon")
                if la is None:
                    continue
                if not any(b["south"] <= la <= b["north"]
                           and b["west"] <= lo <= b["east"] for b in boxes):
                    continue
                op_usable.append({"name": f"OSM: {a.get('name') or 'data center'}",
                                  "status": "operating", "lat": la, "lon": lo})
                added += 1
            if added:
                print(f"[i] operating pool: +{added} OSM facilities "
                      f"(total {len(op_usable)})")
        except (OSError, ValueError):
            pass
    for region, outages in zip(regions, region_results):
        for o in outages:
            cond = wx.conditions(o["lat"], o["lon"])
            alerts = wx.active_alerts(o["lat"], o["lon"])
            site, dist = nearest_dc(o["lat"], o["lon"], usable) if usable else (None, None)
            cls, wflag, dflag, why = classify(o, cond, alerts, site,
                                              dist if dist is not None else 1e9, cfg)
            op_site, op_dist = (nearest_dc(o["lat"], o["lon"], op_usable)
                                if op_usable else (None, None))
            op_near = (op_site is not None and op_dist is not None
                       and op_dist <= cfg["dc_radius_km"])
            if op_near:
                if not dflag and not wflag:
                    cls = "NEAR OPERATING DC FAIR-WEATHER (context)"
                why += (f" | within {op_dist:.1f} km of operating DC "
                        f"{op_site['name']} (context tier: excluded from "
                        f"evidence + alerts)")
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
                       op_dc_name=op_site["name"] if op_near else None,
                       op_dc_km=round(op_dist, 2) if op_near else None,
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
        _emit_json(emit_dir, polled_at, emitted, cfg, feeds=feed_stats)
        auto_path = os.path.join(emit_dir, "datacenters_auto.json")
        if _auto_layer_stale(auto_path):
            fetch_auto_datacenters(cfg, auto_path)
        pl_path = os.path.join(emit_dir, "powerlines.json")
        if _auto_layer_stale(pl_path):
            fetch_powerlines(cfg, sites, pl_path)
        bb_path = os.path.join(emit_dir, "backbone.json")
        if _auto_layer_stale(bb_path) or _layer_pending(bb_path):
            fetch_backbone(cfg, regions, bb_path)
        ga_path = os.path.join(emit_dir, "grid_assets.json")
        if _auto_layer_stale(ga_path) or _layer_pending(ga_path):
            fetch_grid_assets(cfg, regions, ga_path)
        fl_path = os.path.join(emit_dir, "pjm_flows.json")
        if os.environ.get("PJM_API_KEY") and _auto_layer_stale(fl_path, max_age_hours=24):
            fetch_pjm_flows(cfg, fl_path)
        ed_path = os.path.join(emit_dir, "eia_demand.json")
        if _auto_layer_stale(ed_path, max_age_hours=24):
            fetch_eia_demand(cfg, ed_path)
        es_path = os.path.join(emit_dir, "eia_strain.json")
        if _auto_layer_stale(es_path, max_age_hours=6):
            fetch_eia_strain(cfg, es_path)
        ny_path = os.path.join(emit_dir, "nyiso_flows.json")
        if _auto_layer_stale(ny_path, max_age_hours=24):
            fetch_nyiso_lmps(cfg, ny_path)
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


def _emit_json(emit_dir, polled_at, records, cfg, feeds=None):
    """Write docs/data/latest.json + append daily NDJSON history for the
    static GitHub Pages map. Small, git-friendly files."""
    os.makedirs(os.path.join(emit_dir, "history"), exist_ok=True)
    latest = {"generated_at": polled_at,
              "dc_radius_km": cfg["dc_radius_km"],
              "count": len(records),
              "feeds": feeds or [],
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
    _emit_durations(emit_dir, hist_dir, days)

    gj = {"type": "FeatureCollection",
          "features": [{"type": "Feature",
                        "geometry": {"type": "Point",
                                     "coordinates": [r["lon"], r["lat"]]},
                        "properties": {k: v for k, v in r.items()
                                       if k not in ("lat", "lon")}}
                       for r in records]}
    with open(os.path.join(emit_dir, "latest.geojson"), "w") as f:
        json.dump(gj, f, separators=(",", ":"))
    # weekly digest (regenerated when >6 days old) — publication cadence
    dg_path = os.path.join(emit_dir, "digest.json")
    digest = None
    try:
        with open(dg_path) as f:
            digest = json.load(f)
    except Exception:
        pass
    try:
        stale = (digest is None or
                 (datetime.now(timezone.utc)
                  - datetime.fromisoformat(digest["generated_at"])).days >= 7)
    except Exception:
        stale = True
    if stale and days:
        wk = days[-7:]
        digest = {"generated_at": datetime.now(timezone.utc).isoformat(
                      timespec="seconds"),
                  "week_ending": wk[-1]["date"], "days": len(wk),
                  "unique_outages": sum(d["unique"] for d in wk),
                  "fair": sum(d["fair"] for d in wk),
                  "weather": sum(d["weather"] for d in wk),
                  "customers": sum(d["customers"] for d in wk)}
        if digest["unique_outages"]:
            digest["fair_pct"] = round(100 * digest["fair"]
                                       / digest["unique_outages"], 1)
        with open(dg_path, "w") as f:
            json.dump(digest, f, separators=(",", ":"))
        print(f"[i] weekly digest regenerated (week ending "
              f"{digest['week_ending']})")
    try:
        fair = sum(1 for r in records if not r.get("weather_flag"))
        dcp = sum(1 for r in records if r.get("dc_flag"))
        cust = sum(r.get("customers") or 0 for r in records)
        title = (f"GRIDWATCH {polled_at[:16]}Z — {len(records)} outages, "
                 f"{cust:,} customers, {fair} fair-weather, "
                 f"{dcp} DC-proximate")
        top = sorted(records, key=lambda r: -(r.get("customers") or 0))[:5]
        desc = " | ".join(f"{r['region']}: {r.get('customers', 0)} cust "
                          f"({r.get('classification', '')[:28]})"
                          for r in top) or "quiet network"
        item = ("<item><title>" + title + "</title><description>" + desc
                + "</description><pubDate>" + polled_at + "</pubDate>"
                + '<guid isPermaLink="false">' + polled_at + "</guid></item>")
        ditem = ""
        if digest:
            ditem = ("<item><title>Weekly digest — week ending "
                     + digest.get("week_ending", "?") + ": "
                     + str(digest.get("unique_outages", 0)) + " outages, "
                     + str(digest.get("fair_pct", "?"))
                     + "% fair-weather</title><description>"
                     + f"{digest.get('customers', 0):,} customers affected; "
                     + str(digest.get("weather", 0)) + " weather-attributed vs "
                     + str(digest.get("fair", 0)) + " fair-weather."
                     + "</description><pubDate>"
                     + digest.get("generated_at", "") + "</pubDate>"
                     + '<guid isPermaLink="false">digest-'
                     + digest.get("week_ending", "") + "</guid></item>")
        with open(os.path.join(emit_dir, "feed.xml"), "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0">'
                    '<channel><title>GRIDWATCH</title>'
                    '<link>https://paradoxsnowman.github.io/gridwatch/</link>'
                    '<description>Outages x weather x data center proximity, '
                    'Midwest to Atlantic</description>' + item + ditem
                    + '</channel></rss>')
    except Exception as e:
        print(f"[!] rss emit failed (non-fatal): {e}")
    # Copy the curated data layers next to the outage data so the static page
    # has a single data root. (Without this the map silently serves whatever
    # stale copy is already committed — edits to these files never appear.)
    for src_name in ("datacenters.json", "infrastructure.json", "pjm_burden.json"):
        p = os.path.join(BASE_DIR, src_name)
        if not os.path.exists(p):
            continue
        with open(p) as fin, open(os.path.join(emit_dir, src_name), "w") as fout:
            fout.write(fin.read())
        print(f"[i] copied {src_name} -> {emit_dir}")
    print(f"[i] emitted {len(records)} records -> {emit_dir}/latest.json "
          f"+ history/{day}.ndjson")


def _emit_durations(emit_dir, hist_dir, days, window=7, poll_minutes=None):
    """Restoration analytics from the last `window` days of observations:
    per-region average/median observed close-out time and momentary-event
    share (incidents seen in exactly one poll ~ resolved < poll interval).
    Observed span understates true duration by up to one interval on each
    end; we add half an interval as the standard correction and say so."""
    from statistics import median
    all_ts = set()
    spans = {}  # key -> {first, last, region, customers, polls}
    for d in days[-window:]:
        p = os.path.join(hist_dir, f"{d['date']}.ndjson")
        try:
            with open(p) as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    k = f"{r.get('region')}|{r.get('outage_id')}"
                    t = r.get("polled_at", "")
                    all_ts.add(t)
                    s = spans.setdefault(k, {"first": t, "last": t,
                                             "region": r.get("region"),
                                             "customers": r.get("customers") or 0,
                                             "polls": 0})
                    s["polls"] += 1
                    if t < s["first"]:
                        s["first"] = t
                    if t > s["last"]:
                        s["last"] = t
                    s["customers"] = max(s["customers"], r.get("customers") or 0)
        except OSError:
            continue
    if poll_minutes is None:
        ts = sorted(all_ts)
        gaps = []
        for a, b in zip(ts, ts[1:]):
            try:
                g = (datetime.fromisoformat(b)
                     - datetime.fromisoformat(a)).total_seconds() / 60
                if 1 <= g <= 120:
                    gaps.append(g)
            except ValueError:
                pass
        gaps.sort()
        poll_minutes = round(gaps[len(gaps)//2]) if gaps else 15
        print(f"[i] durations: observed poll interval ~{poll_minutes} min")
    # exclude outages still active in the newest poll (not yet closed)
    newest = max((s["last"] for s in spans.values()), default="")
    by_region = {}
    for s in spans.values():
        if s["last"] == newest:
            continue  # still open — no close-out yet
        try:
            t0 = datetime.fromisoformat(s["first"])
            t1 = datetime.fromisoformat(s["last"])
        except ValueError:
            continue
        dur_min = (t1 - t0).total_seconds() / 60 + poll_minutes  # ± half-interval each end
        b = by_region.setdefault(s["region"], {"durs": [], "momentary": 0,
                                               "resolved": 0})
        b["resolved"] += 1
        b["durs"].append(dur_min)
        if s["polls"] == 1:
            b["momentary"] += 1
    out = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "window_days": window, "poll_minutes": poll_minutes,
           "note": ("Observed close-out = last-seen minus first-seen plus one "
                    "poll interval; true duration is within ±"
                    f"{poll_minutes} min. 'Momentary' = seen in a single poll "
                    "(resolved in under ~one interval) — the public-data "
                    "cousin of a MAIFI momentary-interruption metric. True "
                    "brownout/voltage-sag data is utility-internal and only "
                    "surfaces in PUCO reliability filings."),
           "regions": {}}
    longest = []
    for s in spans.values():
        if s["last"] == newest:
            continue
        try:
            t0 = datetime.fromisoformat(s["first"]); t1 = datetime.fromisoformat(s["last"])
        except ValueError:
            continue
        longest.append({"region": s["region"],
                        "minutes": round((t1 - t0).total_seconds() / 60 + poll_minutes),
                        "customers": s["customers"]})
    out["longest"] = sorted(longest, key=lambda x: -x["minutes"])[:5]
    for reg, b in sorted(by_region.items()):
        if not b["durs"]:
            continue
        out["regions"][reg] = {
            "resolved": b["resolved"],
            "avg_min": round(sum(b["durs"]) / len(b["durs"]), 1),
            "median_min": round(median(b["durs"]), 1),
            "momentary": b["momentary"],
            "momentary_pct": round(100 * b["momentary"] / b["resolved"], 1),
        }
    with open(os.path.join(emit_dir, "durations.json"), "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"[i] durations: {sum(b['resolved'] for b in by_region.values())} "
          f"resolved incidents analyzed across {len(by_region)} regions")


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


def cmd_verify(cfg, region_filter=None):
    """Diagnostics: exercise each configured feed, report reachability,
    record counts, and parser success. Writes verify_report.json."""
    regions = _regions(cfg)
    if region_filter:
        regions = [r for r in regions if r["name"] in region_filter]
    report = {"checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "regions": {}}
    for region in regions:
        name = region["name"]
        entry = {"provider": region.get("provider", "kubra"), "ok": False}
        t0 = time.time()
        try:
            prov = make_provider(cfg, region)
            outs = prov.fetch_outages()
            entry["ok"] = True
            entry["outages"] = len(outs)
            entry["customers"] = sum(o.get("customers") or 0 for o in outs)
            entry["with_coords"] = sum(1 for o in outs if o.get("lat"))
            entry["with_cause"] = sum(1 for o in outs if o.get("cause"))
            entry["with_etr"] = sum(1 for o in outs if o.get("etr"))
            entry["sample"] = outs[0] if outs else None
            if region.get("kubra_layer"):
                entry["kubra_layer"] = region["kubra_layer"]
        except Exception as e:
            entry["error"] = f"{type(e).__name__}: {e}"
        entry["seconds"] = round(time.time() - t0, 1)
        report["regions"][name] = entry
        status = "OK " if entry["ok"] else "FAIL"
        detail = (f"{entry.get('outages', 0)} outages, "
                  f"{entry.get('customers', 0)} customers"
                  if entry["ok"] else entry.get("error", "")[:90])
        print(f"[{status}] {name:8s} {entry['seconds']:5.1f}s  {detail}")
    path = os.path.join(BASE_DIR, "verify_report.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n[i] report written: {path}")
    ok = sum(1 for e in report["regions"].values() if e["ok"])
    print(f"[i] {ok}/{len(report['regions'])} feeds healthy")


def main():
    ap = argparse.ArgumentParser(description="FirstEnergy OH outage/weather/DC correlator")
    ap.add_argument("command",
                    choices=["poll", "map", "report", "discover", "osint",
                             "verify"])
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
     "verify": lambda c: cmd_verify(c),
     "osint": lambda c: fetch_auto_datacenters(
         c, os.path.join(args.emit or "docs/data", "datacenters_auto.json")),
     "report": lambda c: cmd_report(c, args.days)}[args.command](cfg)


if __name__ == "__main__":
    main()
