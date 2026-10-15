"""Export Hyderabad Zone Map assets for Power BI (GeoJSON polygons, Point GeoJSON, and CSVs).

Generates:
1. exports/power_bi/hyderabad_zones.geojson        - 40 Polygon features for Power BI Shape Map
2. exports/power_bi/hyderabad_zone_points.geojson  - 40 Point features for GeoJSON tools
3. exports/power_bi/dim_zone_map.csv               - Zone dimension with Lat/Lon coordinates for Power BI Map
4. exports/power_bi/zone_surge_map.csv             - Zones joined with latest live/historical surge & KPI metrics
5. exports/power_bi/preview_map.html               - Standalone interactive HTML map viewer
"""
from __future__ import annotations

import json
import logging
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from scipy.spatial import Voronoi

from data.hyderabad import ZONES, LAT_LON_BOUNDS


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "exports" / "power_bi"
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("power_bi_map_exporter")


def clip_polygon_to_box(poly: list[list[float]], xmin: float, ymin: float, xmax: float, ymax: float) -> list[list[float]]:
    """Sutherland-Hodgman polygon clipping against a bounding rectangle [xmin, ymin, xmax, ymax]."""
    def clip_edge(p: list[list[float]], edge: int) -> list[list[float]]:
        res = []
        if not p:
            return res
        for i in range(len(p)):
            cur = p[i]
            prev = p[i - 1]
            if edge == 0:  # Left (x >= xmin)
                inside = cur[0] >= xmin
                prev_inside = prev[0] >= xmin
            elif edge == 1:  # Right (x <= xmax)
                inside = cur[0] <= xmax
                prev_inside = prev[0] <= xmax
            elif edge == 2:  # Bottom (y >= ymin)
                inside = cur[1] >= ymin
                prev_inside = prev[1] >= ymin
            elif edge == 3:  # Top (y <= ymax)
                inside = cur[1] <= ymax
                prev_inside = prev[1] <= ymax

            if inside:
                if not prev_inside:
                    res.append(intersect(prev, cur, edge))
                res.append(cur)
            elif prev_inside:
                res.append(intersect(prev, cur, edge))
        return res

    def intersect(p1: list[float], p2: list[float], edge: int) -> list[float]:
        x1, y1 = p1
        x2, y2 = p2
        dx = x2 - x1
        dy = y2 - y1
        if edge == 0:  # x = xmin
            t = (xmin - x1) / dx if dx != 0 else 0
            return [round(xmin, 6), round(y1 + t * dy, 6)]
        elif edge == 1:  # x = xmax
            t = (xmax - x1) / dx if dx != 0 else 0
            return [round(xmax, 6), round(y1 + t * dy, 6)]
        elif edge == 2:  # y = ymin
            t = (ymin - y1) / dy if dy != 0 else 0
            return [round(x1 + t * dx, 6), round(ymin, 6)]
        elif edge == 3:  # y = ymax
            t = (ymax - y1) / dy if dy != 0 else 0
            return [round(x1 + t * dx, 6), round(ymax, 6)]
        return [x2, y2]

    cp = poly
    for e in range(4):
        cp = clip_edge(cp, e)
    return cp


def generate_voronoi_polygons() -> list[list[list[float]]]:
    """Generate contiguous, non-overlapping Voronoi polygons for the 40 centroid coordinates."""
    points = np.array([[z.lon, z.lat] for z in ZONES])
    lat_min, lat_max, lon_min, lon_max = LAT_LON_BOUNDS

    # Add surrounding mirror points to bound infinite Voronoi outer rays
    margin = 0.6
    box_pts = np.array([
        [lon_min - margin, lat_min - margin],
        [lon_min - margin, lat_max + margin],
        [lon_max + margin, lat_min - margin],
        [lon_max + margin, lat_max + margin],
        [lon_min - margin, (lat_min + lat_max) / 2],
        [lon_max + margin, (lat_min + lat_max) / 2],
        [(lon_min + lon_max) / 2, lat_min - margin],
        [(lon_min + lon_max) / 2, lat_max + margin],
    ])
    all_pts = np.vstack([points, box_pts])
    vor = Voronoi(all_pts)

    polygons = []
    for i in range(len(ZONES)):
        region_idx = vor.point_region[i]
        region = vor.regions[region_idx]
        if -1 not in region and len(region) > 0:
            poly = [[round(vor.vertices[v][0], 6), round(vor.vertices[v][1], 6)] for v in region]
            clipped = clip_polygon_to_box(poly, lon_min, lat_min, lon_max, lat_max)
            if len(clipped) >= 3:
                # Ensure linear ring is closed (first coord == last coord)
                if clipped[0] != clipped[-1]:
                    clipped.append(clipped[0])
                polygons.append(clipped)
            else:
                polygons.append(_fallback_circle(ZONES[i].lat, ZONES[i].lon))
        else:
            polygons.append(_fallback_circle(ZONES[i].lat, ZONES[i].lon))

    return polygons


def _fallback_circle(lat: float, lon: float, radius_km: float = 1.8, num_points: int = 16) -> list[list[float]]:
    coords = []
    lat_scale = 1.0 / 110.574
    lon_scale = 1.0 / (111.320 * math.cos(math.radians(lat)))
    for i in range(num_points):
        angle = 2 * math.pi * i / num_points
        dx = radius_km * math.cos(angle)
        dy = radius_km * math.sin(angle)
        coords.append([round(lon + dx * lon_scale, 6), round(lat + dy * lat_scale, 6)])
    coords.append(coords[0])
    return coords


def export_assets() -> None:
    polygons = generate_voronoi_polygons()

    # 1. GeoJSON Polygons (For Power BI Shape Map)
    features_poly = []
    for z, poly in zip(ZONES, polygons):
        features_poly.append({
            "type": "Feature",
            "id": z.zone_id,
            "properties": {
                "zone_id": z.zone_id,
                "zone_name": z.name,
                "name": z.name,
                "kind": z.kind,
                "latitude": z.lat,
                "longitude": z.lon,
                "demand_weight": z.weight,
                "city": "Hyderabad",
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [poly],
            },
        })

    geojson_poly_doc = {
        "type": "FeatureCollection",
        "name": "Hyderabad_Ride_Hailing_Zones",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features_poly,
    }
    poly_path = OUT_DIR / "hyderabad_zones.geojson"
    poly_path.write_text(json.dumps(geojson_poly_doc, indent=2), encoding="utf-8")
    logger.info("Saved GeoJSON Polygons -> %s (%d zones)", poly_path, len(features_poly))

    # 2. GeoJSON Points
    features_pt = []
    for z in ZONES:
        features_pt.append({
            "type": "Feature",
            "id": z.zone_id,
            "properties": {
                "zone_id": z.zone_id,
                "zone_name": z.name,
                "name": z.name,
                "kind": z.kind,
                "demand_weight": z.weight,
                "city": "Hyderabad",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [z.lon, z.lat],
            },
        })
    geojson_pt_doc = {
        "type": "FeatureCollection",
        "name": "Hyderabad_Zone_Centroids",
        "features": features_pt,
    }
    pt_path = OUT_DIR / "hyderabad_zone_points.geojson"
    pt_path.write_text(json.dumps(geojson_pt_doc, indent=2), encoding="utf-8")
    logger.info("Saved GeoJSON Points -> %s", pt_path)

    # 3. CSV for Power BI Map Visual
    rows_dim = []
    for z in ZONES:
        rows_dim.append({
            "zone_id": z.zone_id,
            "zone_name": z.name,
            "kind": z.kind,
            "latitude": z.lat,
            "longitude": z.lon,
            "demand_weight": z.weight,
            "city": "Hyderabad",
        })
    df_dim = pd.DataFrame(rows_dim)
    dim_csv_path = OUT_DIR / "dim_zone_map.csv"
    df_dim.to_csv(dim_csv_path, index=False)
    logger.info("Saved Dimension CSV -> %s", dim_csv_path)

    # 4. Enriched Surge & KPIs CSV for Power BI Map
    kpi_path = ROOT / "data" / "gold" / "zone_hourly_kpis.parquet"
    if kpi_path.exists():
        df_kpi = pd.read_parquet(kpi_path)
        # Aggregate across historical hours for zone overview
        kpi_summary = df_kpi.groupby("pickup_zone_id").agg({
            "requests": "sum",
            "revenue_inr": "sum",
            "km": "sum",
            "avg_eta_min": "mean",
        }).reset_index().rename(columns={"pickup_zone_id": "zone_id"})
    else:
        kpi_summary = pd.DataFrame(columns=["zone_id", "requests", "revenue_inr", "km", "avg_eta_min"])

    # Load recent surge output if exists
    surge_path = ROOT / "data" / "lakehouse" / "bronze_surge"
    if surge_path.exists():
        df_s = pd.read_parquet(surge_path)
        if not df_s.empty:
            max_win = df_s["window_end"].max()
            recent_s = df_s[df_s["window_end"] == max_win][["zone_id", "demand", "supply", "surge_multiplier", "ratio"]]
        else:
            recent_s = pd.DataFrame(columns=["zone_id", "demand", "supply", "surge_multiplier", "ratio"])
    else:
        recent_s = pd.DataFrame(columns=["zone_id", "demand", "supply", "surge_multiplier", "ratio"])

    df_full = df_dim.merge(kpi_summary, on="zone_id", how="left").merge(recent_s, on="zone_id", how="left")
    df_full["surge_multiplier"] = df_full["surge_multiplier"].fillna(1.0)
    df_full["demand"] = df_full["demand"].fillna(df_full["demand_weight"] * 25).astype(int)
    df_full["supply"] = df_full["supply"].fillna(df_full["demand_weight"] * 22).astype(int)
    df_full["ratio"] = df_full["ratio"].fillna(1.1)

    surge_csv_path = OUT_DIR / "zone_surge_map.csv"
    df_full.to_csv(surge_csv_path, index=False)
    logger.info("Saved Surge Map CSV -> %s", surge_csv_path)

    # 5. Standalone HTML Map Preview
    html_content = _generate_html_preview(df_full, geojson_poly_doc)
    html_path = OUT_DIR / "preview_map.html"
    html_path.write_text(html_content, encoding="utf-8")
    logger.info("Saved HTML Map Preview -> %s", html_path)


def _generate_html_preview(df: pd.DataFrame, geojson_data: dict) -> str:
    zones_json = df.to_dict(orient="records")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Hyderabad Ride-Hailing Zone Map — Power BI Asset Preview</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    body {{ margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; }}
    #header {{ padding: 14px 20px; background: #1e293b; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }}
    h1 {{ margin: 0; font-size: 18px; font-weight: 600; color: #38bdf8; }}
    .badge {{ background: #0284c7; padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; }}
    #map {{ height: calc(100vh - 58px); width: 100vw; }}
    .legend {{ background: rgba(15, 23, 42, 0.9); padding: 12px 16px; border-radius: 8px; border: 1px solid #334155; line-height: 24px; font-size: 12px; }}
    .legend i {{ width: 14px; height: 14px; float: left; margin-right: 8px; border-radius: 3px; opacity: 0.8; }}
    .leaflet-popup-content-wrapper {{ background: #1e293b; color: #f8fafc; border-radius: 8px; border: 1px solid #475569; }}
    .leaflet-popup-tip {{ background: #1e293b; }}
  </style>
</head>
<body>
  <div id="header">
    <h1>🚕 Hyderabad 40-Zone Partitioned Map (Power BI Shape & Bubble Spec)</h1>
    <span class="badge">40 Zones &bull; Voronoi Polygons + Centroids</span>
  </div>
  <div id="map"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const zonesData = {json.dumps(zones_json)};
    const geojsonData = {json.dumps(geojson_data)};

    const map = L.map('map').setView([17.43, 78.44], 11);
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap &copy; CARTO'
    }}).addTo(map);

    function getColor(surge) {{
      return surge >= 2.0 ? '#ef4444' :
             surge >= 1.5 ? '#f97316' :
             surge >= 1.2 ? '#eab308' :
                            '#10b981';
    }}

    // Index stats by zone_id
    const statsMap = {{}};
    zonesData.forEach(z => statsMap[z.zone_id] = z);

    // Add Voronoi Polygons (Shape Map simulation)
    L.geoJSON(geojsonData, {{
      style: function(feature) {{
        const z = statsMap[feature.properties.zone_id] || {{}};
        const s = z.surge_multiplier || 1.0;
        return {{
          fillColor: getColor(s),
          weight: 1.5,
          opacity: 0.8,
          color: '#64748b',
          fillOpacity: 0.35
        }};
      }},
      onEachFeature: function(feature, layer) {{
        const z = statsMap[feature.properties.zone_id] || {{}};
        layer.bindPopup(`
          <div style="font-size:13px; line-height:1.6;">
            <strong style="font-size:15px; color:#38bdf8;">${{feature.properties.zone_name}}</strong> (Zone ${{feature.properties.zone_id}})<br>
            <b>Category:</b> ${{feature.properties.kind}}<br>
            <b>Surge Multiplier:</b> <span style="color:${{getColor(z.surge_multiplier || 1.0)}}; font-weight:700;">${{(z.surge_multiplier || 1.0).toFixed(2)}}x</span><br>
            <b>Demand:</b> ${{z.demand || '-'}} requests<br>
            <b>Supply:</b> ${{z.supply || '-'}} drivers<br>
            <b>Total Revenue:</b> ₹${{z.revenue_inr ? Math.round(z.revenue_inr).toLocaleString() : '-'}}
          </div>
        `);
      }}
    }}).addTo(map);

    // Add Centroid Markers
    zonesData.forEach(z => {{
      L.circleMarker([z.latitude, z.longitude], {{
        radius: 6,
        fillColor: getColor(z.surge_multiplier),
        color: '#ffffff',
        weight: 1.5,
        opacity: 0.9,
        fillOpacity: 0.9
      }}).bindTooltip(`<b>${{z.zone_name}}</b> (${{z.surge_multiplier.toFixed(2)}}x)`, {{ direction: 'top', offset: [0, -6] }}).addTo(map);
    }});

    // Legend
    const legend = L.control({{ position: 'bottomright' }});
    legend.onAdd = function() {{
      const div = L.DomUtil.create('div', 'legend');
      div.innerHTML = `
        <b>Surge Multiplier</b><br>
        <i style="background:#ef4444"></i> >= 2.0x (Peak Surge)<br>
        <i style="background:#f97316"></i> 1.5x - 1.9x (High)<br>
        <i style="background:#eab308"></i> 1.2x - 1.4x (Moderate)<br>
        <i style="background:#10b981"></i> 1.0x - 1.1x (Standard)<br>
      `;
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    export_assets()
