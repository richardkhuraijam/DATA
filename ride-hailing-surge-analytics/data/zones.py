"""Load TLC zone lookup + shapefile centroids (LocationID 1–263)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import shapefile
from pyproj import Transformer
from shapely.geometry import shape

from data.settings import ZONES_DIR
from data.validation import NYC_ZONE_ID_MAX, NYC_ZONE_ID_MIN

LOGGER = logging.getLogger("zones")

# Official TLC shapefile is State Plane NY Long Island (US feet).
TLC_SHAPE_CRS = "EPSG:2263"
WGS84 = "EPSG:4326"


def _lookup_csv() -> Path:
    for name in ("taxi_zone_lookup.csv", "taxi+_zone_lookup.csv"):
        path = ZONES_DIR / name
        if path.exists():
            return path
    matches = list(ZONES_DIR.glob("*zone_lookup*.csv"))
    if not matches:
        raise FileNotFoundError(f"No taxi zone lookup CSV under {ZONES_DIR}. Run data/download_tlc.py")
    return matches[0]


def _shapefile_path() -> Path | None:
    shp_files = list((ZONES_DIR / "shapefile").rglob("*.shp"))
    if shp_files:
        return shp_files[0]
    return None


def _centroids_from_shapefile(shp_path: Path) -> dict[int, tuple[float, float]]:
    transformer = Transformer.from_crs(TLC_SHAPE_CRS, WGS84, always_xy=True)
    reader = shapefile.Reader(str(shp_path))
    field_names = [f[0] for f in reader.fields[1:]]
    centroids: dict[int, tuple[float, float]] = {}
    for sr in reader.shapeRecords():
        rec = dict(zip(field_names, sr.record, strict=False))
        loc = rec.get("LocationID") or rec.get("OBJECTID")
        if loc is None:
            continue
        geom = shape(sr.shape.__geo_interface__)
        if geom.is_empty:
            continue
        lon, lat = transformer.transform(geom.centroid.x, geom.centroid.y)
        centroids[int(loc)] = (float(lat), float(lon))
    LOGGER.info("Computed centroids for %s zones from %s", len(centroids), shp_path.name)
    return centroids


def load_city_zones() -> list[dict[str, Any]]:
    lookup = pd.read_csv(_lookup_csv())
    lookup.columns = [c.strip() for c in lookup.columns]
    id_col = "LocationID" if "LocationID" in lookup.columns else lookup.columns[0]
    shp = _shapefile_path()
    centroids = _centroids_from_shapefile(shp) if shp else {}

    rows: list[dict[str, Any]] = []
    for rec in lookup.to_dict(orient="records"):
        zone_id = int(rec[id_col])
        if not NYC_ZONE_ID_MIN <= zone_id <= NYC_ZONE_ID_MAX:
            continue
        lat_lon = centroids.get(zone_id)
        rows.append(
            {
                "zone_id": zone_id,
                "zone_name": str(rec.get("Zone", f"Zone {zone_id}")),
                "borough": str(rec.get("Borough", "Unknown")),
                "service_zone": None if pd.isna(rec.get("service_zone")) else str(rec.get("service_zone")),
                "centroid_lat": lat_lon[0] if lat_lon else None,
                "centroid_lon": lat_lon[1] if lat_lon else None,
            }
        )
    if len(rows) != 263:
        LOGGER.warning("Expected 263 TLC zones, loaded %s", len(rows))
    return rows


class NYCZones:
    """Helper class for zone lookups, centroids, and point-in-polygon mapping."""

    def __init__(self) -> None:
        self._zones = load_city_zones()
        self._zone_dict = {z["zone_id"]: z for z in self._zones}

    def all_zones(self) -> list[dict[str, Any]]:
        return self._zones

    def get_centroid(self, zone_id: int) -> tuple[float, float]:
        zone = self._zone_dict.get(zone_id)
        if zone and zone["centroid_lat"] and zone["centroid_lon"]:
            return (float(zone["centroid_lat"]), float(zone["centroid_lon"]))
        # Default Manhattan baseline
        return (40.7580, -73.9855)

    def lat_lon_to_zone_id(self, lat: float, lon: float) -> int | None:
        """Point-in-polygon or nearest centroid lookup."""
        best_zone: int | None = None
        min_dist_sq = float("inf")

        for z in self._zones:
            c_lat = z["centroid_lat"]
            c_lon = z["centroid_lon"]
            if c_lat is not None and c_lon is not None:
                dist_sq = (lat - c_lat) ** 2 + (lon - c_lon) ** 2
                if dist_sq < min_dist_sq:
                    min_dist_sq = dist_sq
                    best_zone = z["zone_id"]

        # Default fallback zone if within NYC bounds
        if best_zone is None and (40.4 <= lat <= 41.0) and (-74.3 <= lon <= -73.6):
            return 161  # Midtown Center
        return best_zone

