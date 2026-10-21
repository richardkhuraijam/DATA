"""Hyderabad (India) city model: zones, demand curves, and a synthetic trip generator.

IMPORTANT: there is no public Hyderabad ride-hailing trip dataset (Ola/Uber/Rapido do not publish one), so
history and the live demand stream are SYNTHETIC but follow realistic structure: IT-corridor evening peaks,
residential morning peaks, airport night peaks, weekend dips, and monsoon rain demand uplift.
Zone centroids are approximate (locality centres, +/- ~1 km). Replace with GHMC ward polygons if you need
exact boundaries.  All timestamps in MySQL/history are IST (naive); the Kafka stream uses UTC ISO-8601.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

CITY = "Hyderabad"
IST_OFFSET = timedelta(hours=5, minutes=30)
LAT_LON_BOUNDS = (17.10, 17.75, 78.15, 78.75)       # lat_min, lat_max, lon_min, lon_max
WEATHER_LAT, WEATHER_LON = 17.3850, 78.4867          # Open-Meteo point for the city
VEHICLE_TYPES = ("Auto", "Mini", "Sedan", "SUV", "Bike")
CANCEL_REASONS = ("RIDER_CANCEL", "DRIVER_CANCEL", "TIMEOUT", "NO_SHOW")


@dataclass(frozen=True)
class Zone:
    zone_id: int
    name: str
    kind: str          # it_hub | residential | commercial | transit | airport
    lat: float
    lon: float
    weight: float      # relative share of city demand


# (name, kind, lat, lon, weight)
_RAW = [
    ("Hitech City", "it_hub", 17.4435, 78.3772, 1.6), ("Madhapur", "it_hub", 17.4485, 78.3908, 1.6),
    ("Gachibowli", "it_hub", 17.4401, 78.3489, 1.5), ("Financial District", "it_hub", 17.4172, 78.3434, 1.3),
    ("Kondapur", "it_hub", 17.4600, 78.3650, 1.2), ("Raidurg", "it_hub", 17.4260, 78.3830, 0.8),
    ("Kokapet", "residential", 17.4000, 78.3300, 0.7), ("Narsingi", "residential", 17.3870, 78.3550, 0.6),
    ("Manikonda", "residential", 17.4010, 78.3870, 0.9), ("Miyapur", "residential", 17.4969, 78.3548, 1.1),
    ("Kukatpally", "residential", 17.4849, 78.4138, 1.4), ("Nizampet", "residential", 17.5170, 78.3830, 0.8),
    ("Bachupally", "residential", 17.5440, 78.3580, 0.7), ("Banjara Hills", "commercial", 17.4156, 78.4347, 1.3),
    ("Jubilee Hills", "commercial", 17.4326, 78.4071, 1.2), ("Yousufguda", "residential", 17.4350, 78.4250, 0.8),
    ("Ameerpet", "transit", 17.4375, 78.4483, 1.4), ("SR Nagar", "residential", 17.4440, 78.4410, 0.8),
    ("Somajiguda", "commercial", 17.4250, 78.4580, 0.9), ("Begumpet", "transit", 17.4440, 78.4670, 1.1),
    ("Secunderabad", "transit", 17.4399, 78.4983, 1.5), ("Tarnaka", "residential", 17.4270, 78.5320, 0.8),
    ("Habsiguda", "residential", 17.4150, 78.5400, 0.7), ("Uppal", "residential", 17.4058, 78.5591, 1.0),
    ("Malkajgiri", "residential", 17.4500, 78.5270, 0.7), ("Alwal", "residential", 17.5060, 78.5080, 0.6),
    ("Kompally", "residential", 17.5400, 78.4860, 0.6), ("Sainikpuri", "residential", 17.4900, 78.5600, 0.5),
    ("Khairatabad", "commercial", 17.4100, 78.4650, 0.8), ("Abids", "commercial", 17.3910, 78.4750, 1.0),
    ("Nampally", "transit", 17.3922, 78.4685, 0.8), ("Koti", "commercial", 17.3850, 78.4800, 0.9),
    ("Charminar", "commercial", 17.3616, 78.4747, 0.9), ("Dilsukhnagar", "residential", 17.3688, 78.5247, 1.0),
    ("LB Nagar", "residential", 17.3457, 78.5522, 0.9), ("Mehdipatnam", "transit", 17.3950, 78.4400, 1.1),
    ("Tolichowki", "residential", 17.4024, 78.4116, 0.8), ("Attapur", "residential", 17.3660, 78.4270, 0.6),
    ("Rajendranagar", "residential", 17.3200, 78.4000, 0.5), ("RGIA Airport (Shamshabad)", "airport", 17.2403, 78.4294, 1.2),
]
ZONES: list[Zone] = [Zone(i + 1, *r) for i, r in enumerate(_RAW)]
ZONE_BY_ID = {z.zone_id: z for z in ZONES}
ZONE_ID_MIN, ZONE_ID_MAX = 1, len(ZONES)


def _bump(h: float, centre: float, width: float) -> float:
    d = min(abs(h - centre), 24 - abs(h - centre))      # circular distance in hours
    return math.exp(-(d / width) ** 2)


def hour_profile(kind: str, hour: float, weekend: bool) -> float:
    """Relative demand intensity (about 0.1 .. 1.0) for a zone kind at an IST hour."""
    if kind == "it_hub":
        v = 0.12 + 0.45 * _bump(hour, 9, 1.3) + 1.0 * _bump(hour, 19, 1.8) + 0.35 * _bump(hour, 1, 2.0) + 0.25 * _bump(hour, 13, 1.5)
        return v * (0.45 if weekend else 1.0)
    if kind == "residential":
        v = 0.12 + 1.0 * _bump(hour, 8.5, 1.4) + 0.65 * _bump(hour, 19.5, 1.8) + 0.3 * _bump(hour, 13, 2.0)
        return v * (1.1 if weekend else 1.0)
    if kind == "commercial":
        v = 0.15 + 0.7 * _bump(hour, 13, 2.5) + 0.9 * _bump(hour, 19, 2.0) + 0.4 * _bump(hour, 9, 1.5)
        return v * (1.2 if weekend else 1.0)
    if kind == "transit":
        v = 0.15 + 0.9 * _bump(hour, 8.5, 1.5) + 1.0 * _bump(hour, 18.5, 1.8) + 0.3 * _bump(hour, 13, 2.0)
        return v * (0.9 if weekend else 1.0)
    if kind == "airport":                                 # flight banks: early morning and late evening
        return 0.35 + 0.6 * _bump(hour, 5.5, 1.6) + 0.7 * _bump(hour, 22.5, 2.2) + 0.3 * _bump(hour, 14, 2.0)
    raise ValueError(kind)


def demand_rate_per_min(zone: Zone, ist_dt: datetime, scale: float = 1.0, rain_mm: float = 0.0) -> float:
    """Expected trip requests per minute for a zone at an IST datetime."""
    hour = ist_dt.hour + ist_dt.minute / 60
    base = zone.weight * hour_profile(zone.kind, hour, ist_dt.weekday() >= 5)
    rain_uplift = 1.0 + (0.35 if rain_mm >= 2.5 else 0.15 if rain_mm > 0.2 else 0.0)
    return scale * 0.45 * base * rain_uplift


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p = math.pi / 180
    a = 0.5 - math.cos((lat2 - lat1) * p) / 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * (1 - math.cos((lon2 - lon1) * p)) / 2
    return 2 * 6371 * math.asin(math.sqrt(a))


def nearest_zone_id(lat: float, lon: float) -> int:
    return min(ZONES, key=lambda z: (z.lat - lat) ** 2 + ((z.lon - lon) * math.cos(math.radians(lat))) ** 2).zone_id


def is_inside_city(lat: float, lon: float) -> bool:
    a, b, c, d = LAT_LON_BOUNDS
    return a <= lat <= b and c <= lon <= d


def traffic_speed_kmph(hour: int) -> float:
    return 16.0 if hour in (8, 9, 10, 17, 18, 19, 20) else 22.0 if 11 <= hour <= 16 else 30.0


def fare_inr(distance_km: float, vehicle: str, surge: float = 1.0) -> float:
    base, per_km = {"Auto": (30, 12), "Bike": (20, 8), "Mini": (50, 13), "Sedan": (65, 16), "SUV": (90, 21)}[vehicle]
    return round((base + per_km * distance_km) * surge, 2)


def sample_weather(day: datetime, rng: random.Random) -> list[dict]:
    """24 hourly rows (IST) with Hyderabad-like climate: hot Apr-May, monsoon Jun-Sep rain."""
    m = day.month
    mean_t = {1: 22, 2: 25, 3: 29, 4: 32, 5: 34, 6: 29, 7: 26, 8: 26, 9: 26, 10: 26, 11: 23, 12: 21}[m]
    rainy = rng.random() < (0.30 if 6 <= m <= 9 else 0.10 if m in (5, 10) else 0.02)
    start, length = rng.randint(12, 20), rng.randint(2, 5)
    rows = []
    for h in range(24):
        temp = mean_t + 5 * math.sin((h - 9) / 24 * 2 * math.pi) + rng.gauss(0, 0.8)
        rain = round(rng.uniform(1.0, 9.0), 1) if rainy and start <= h < start + length else 0.0
        rows.append({"hour_ts": day.replace(hour=h, minute=0, second=0, microsecond=0),
                     "temp_c": round(temp - (2 if rain else 0), 1), "rain_mm": rain})
    return rows


def generate_history(n_trips: int, days: int = 30, end_date: datetime | None = None, seed: int = 42,
                     scale: float | None = None) -> tuple[list[dict], list[dict]]:
    """Return (trips, hourly_weather). Trips are sampled from the minute-level demand intensity, so the
    time-of-day / zone / rain structure is real in the data.  `scale` is auto-fitted to give ~n_trips."""
    rng = random.Random(seed)
    end = (end_date or datetime.utcnow() + IST_OFFSET).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    weather: list[dict] = []
    for d in range(days):
        weather += sample_weather(start + timedelta(days=d), rng)
    rain_at = {w["hour_ts"]: w["rain_mm"] for w in weather}
    slots = [start + timedelta(minutes=5 * i) for i in range(days * 288)]          # 5-min grid
    rates = {}
    total = 0.0
    for t in slots:
        rain = rain_at[t.replace(minute=0)]
        for z in ZONES:
            r = demand_rate_per_min(z, t, 1.0, rain) * 5
            rates[(t, z.zone_id)] = r
            total += r
    k = (n_trips / total) if scale is None else scale
    trips: list[dict] = []
    for (t, zid), r in rates.items():
        lam = r * k
        n = _poisson(rng, lam)
        z = ZONE_BY_ID[zid]
        for _ in range(n):
            req = t + timedelta(seconds=rng.randint(0, 299))
            dest = _pick_dest(rng, z)
            dist = max(0.8, haversine_km(z.lat, z.lon, dest.lat, dest.lon) * 1.35 + rng.gauss(0, 0.6))
            veh = rng.choices(VEHICLE_TYPES, weights=[30, 30, 20, 8, 12])[0]
            peak = req.hour in (8, 9, 10, 17, 18, 19, 20)
            wait = max(1.0, rng.gauss(6.5 if peak else 4.0, 1.8) + (1.5 if rain_at[req.replace(minute=0, second=0)] > 0 else 0))
            dur = dist / traffic_speed_kmph(req.hour) * 60 * rng.uniform(0.9, 1.2)
            pickup = req + timedelta(minutes=wait)
            trips.append({"request_time": req, "pickup_time": pickup, "dropoff_time": pickup + timedelta(minutes=dur),
                          "pickup_zone_id": zid, "dropoff_zone_id": dest.zone_id, "distance_km": round(dist, 2),
                          "vehicle_type": veh, "fare": fare_inr(dist, veh), "wait_min": round(wait, 2)})
    trips.sort(key=lambda r: r["request_time"])
    return trips, weather


def _pick_dest(rng: random.Random, origin: Zone) -> Zone:
    others = [z for z in ZONES if z.zone_id != origin.zone_id]
    # people travel to nearby zones more often than far ones
    w = [z.weight / (1.0 + haversine_km(origin.lat, origin.lon, z.lat, z.lon) / 3.5) for z in others]
    return rng.choices(others, weights=w)[0]


def _poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def zones_as_rows() -> list[dict]:
    return [{"zone_id": z.zone_id, "zone_name": z.name, "borough": z.kind, "service_zone": CITY,
             "centroid_lat": z.lat, "centroid_lon": z.lon} for z in ZONES]


def iter_zone_ids() -> Iterable[int]:
    return (z.zone_id for z in ZONES)
