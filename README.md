# GRIDWATCH — FE Ohio outage / weather / data-center correlator

## What it does
Polls FirstEnergy Ohio's KUBRA StormCenter outage feed, enriches every outage
with trailing-3h weather (Open-Meteo) and active NWS alerts, computes distance
to each site in `datacenters.json`, classifies, logs to SQLite, and renders a
folium map. The `report` command answers the actual question longitudinally:
is the fair-weather outage rate inside DC proximity rings elevated vs the rest
of the territory? 

## Are data centers sucking the life outta our grid? 
<img width="523" height="382" alt="image" src="https://github.com/user-attachments/assets/b8aa2167-6b58-468e-a710-cc9595e4ebb0" />



## Classes
- **WEATHER-LIKELY** (blue) — gusts/wind/precip/temp-extreme/NWS alert/utility weather cause
- **DC-PROXIMATE FAIR-WEATHER** (red) — no weather signal, within ring; darker red if the utility's own cause string looks like dig-in/vehicle/construction damage
- **AMBIGUOUS** (purple) — both
- **UNEXPLAINED FAIR-WEATHER** (amber) — neither (equipment failure baseline)

# https://tfo04delta.github.io/gridwatch/
