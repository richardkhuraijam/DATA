#!/usr/bin/env python3
"""Download NYC TLC HVFHV Parquet (1–2 months) and the taxi zone map."""

from __future__ import annotations

import logging
import shutil
import ssl
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import certifi

from data.settings import RAW_DIR, TLC_DIR, ZONES_DIR, env, setup_logging

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
_OPENER = urllib.request.build_opener(urllib.request.HTTPSHandler(context=_SSL_CONTEXT))
_OPENER.addheaders = [("User-Agent", "ride-hailing-surge-analytics/1.0")]

LOGGER = logging.getLogger("download_tlc")

LOOKUP_CANDIDATES = (
    "misc/taxi_zone_lookup.csv",
    "misc/taxi+_zone_lookup.csv",
)
ZONE_SHAPE_CANDIDATES = (
    "misc/taxi_zones.zip",
    "misc/taxi_zones.parquet",
)


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    LOGGER.info("Downloading %s -> %s", url, dest)
    try:
        with _OPENER.open(url, timeout=120) as resp, tmp.open("wb") as fh:
            shutil.copyfileobj(resp, fh)
    except urllib.error.HTTPError as exc:
        if tmp.exists():
            tmp.unlink()
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    tmp.replace(dest)
    size_mb = dest.stat().st_size / (1024 * 1024)
    LOGGER.info("Saved %s (%.1f MB)", dest.name, size_mb)


def _first_available(base: str, relative_paths: tuple[str, ...], dest_dir: Path) -> Path:
    last_error: Exception | None = None
    for rel in relative_paths:
        url = f"{base.rstrip('/')}/{rel}"
        dest = dest_dir / Path(rel).name.replace("+", "")
        if dest.exists() and dest.stat().st_size > 0:
            LOGGER.info("Already present: %s", dest)
            return dest
        try:
            _download(url, dest)
            return dest
        except Exception as exc:  # noqa: BLE001 — try next mirror path
            last_error = exc
            LOGGER.warning("Failed %s (%s)", url, exc)
    raise RuntimeError(f"Could not download any of {relative_paths}: {last_error}")


def download_months(base: str, months: list[str]) -> list[Path]:
    paths: list[Path] = []
    TLC_DIR.mkdir(parents=True, exist_ok=True)
    for month in months:
        name = f"fhvhv_tripdata_{month}.parquet"
        dest = TLC_DIR / name
        url = f"{base.rstrip('/')}/trip-data/{name}"
        if dest.exists() and dest.stat().st_size > 0:
            LOGGER.info("Already present: %s", dest)
        else:
            _download(url, dest)
        paths.append(dest)
    return paths


def unpack_zones_zip(zip_path: Path) -> None:
    if zip_path.suffix.lower() != ".zip":
        return
    extract_dir = ZONES_DIR / "shapefile"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    LOGGER.info("Extracted shapefile to %s", extract_dir)


def main() -> None:
    setup_logging()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    TLC_DIR.mkdir(parents=True, exist_ok=True)
    ZONES_DIR.mkdir(parents=True, exist_ok=True)

    base = env("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")
    months = [m.strip() for m in env("TLC_MONTHS", "2026-01,2026-02").split(",") if m.strip()]
    if not 1 <= len(months) <= 2:
        raise SystemExit("TLC_MONTHS must list 1 or 2 YYYY-MM values")

    parquet_paths = download_months(base, months)
    lookup = _first_available(base, LOOKUP_CANDIDATES, ZONES_DIR)
    zone_map = _first_available(base, ZONE_SHAPE_CANDIDATES, ZONES_DIR)
    unpack_zones_zip(zone_map)

    LOGGER.info("HVFHV files: %s", ", ".join(p.name for p in parquet_paths))
    LOGGER.info("Zone lookup: %s", lookup)
    LOGGER.info("Zone map: %s", zone_map)


if __name__ == "__main__":
    main()
