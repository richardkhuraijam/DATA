"""Surge store: HBase `surge_multipliers` (row key zone_id#yyyymmddHHMM, CF 's', TTL 7 d) + an in-memory twin.

Differences from the first version (bugs fixed):
- happybase's CF option is `time_to_live` (the old `ttl` raised and was swallowed -> table never created).
- NO silent fallback to memory. If HBase is unreachable we raise. Use SURGE_STORE=memory explicitly for tests.
- 'latest for a zone' uses a bounded forward range scan (portable across Thrift/happybase versions)
  instead of a reverse scan.
Keys use UTC minutes.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
logger = logging.getLogger("surge_store")

TABLE_NAME, COLUMN_FAMILY, TTL_SECONDS = "surge_multipliers", "s", 7 * 86400
FIELDS = ("multiplier", "demand", "supply", "ratio", "on_trip", "rain_mm", "temp_c", "updated_at")


def format_hbase_row_key(zone_id: int, timestamp_str: str) -> str:
    """zone_id (3 digits) # yyyymmddHHMM from an ISO string such as 2026-10-12T14:05:00Z."""
    digits = "".join(ch for ch in timestamp_str if ch.isdigit())[:12]
    return f"{int(zone_id):03d}#{digits}"


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def _key_minute(ts: datetime) -> str:
    return ts.strftime("%Y%m%d%H%M")


class _BaseStore:
    def put_surge(self, zone_id: int, timestamp_str: str, multiplier: float, demand: int, supply: int, ratio: float,
                  rain_mm: Optional[float] = None, temp_c: Optional[float] = None, on_trip: int = 0) -> str:
        raise NotImplementedError

    def _rows_between(self, zone_id: int, start_key: str, stop_key: str) -> List[Tuple[str, Dict[str, str]]]:
        raise NotImplementedError

    def get_latest_surge(self, zone_id: int, lookback_minutes: int = 60) -> Optional[Dict[str, str]]:
        now = datetime.utcnow()
        rows = self._rows_between(zone_id, f"{int(zone_id):03d}#{_key_minute(now - timedelta(minutes=lookback_minutes))}",
                                  f"{int(zone_id):03d}#{_key_minute(now + timedelta(minutes=10))}")
        return rows[-1][1] if rows else None

    def previous_multiplier(self, zone_id: int, window_end_iso: str) -> Optional[float]:
        """Multiplier of the minute strictly before `window_end` (replay-safe smoothing input)."""
        end = _parse(window_end_iso)
        cur = f"{int(zone_id):03d}#{_key_minute(end)}"
        rows = [r for r in self._rows_between(zone_id, f"{int(zone_id):03d}#{_key_minute(end - timedelta(minutes=10))}", cur) if r[0] < cur]
        return float(rows[-1][1]["multiplier"]) if rows else None

    def history(self, zone_id: int, minutes: int = 60) -> List[Tuple[str, Dict[str, str]]]:
        now = datetime.utcnow()
        return self._rows_between(zone_id, f"{int(zone_id):03d}#{_key_minute(now - timedelta(minutes=minutes))}",
                                  f"{int(zone_id):03d}#{_key_minute(now + timedelta(minutes=10))}")

    def close(self) -> None:  # pragma: no cover
        pass


class HBaseClient(_BaseStore):
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        self.host = host or os.getenv("HBASE_THRIFT_HOST", "localhost")
        self.port = int(port or os.getenv("HBASE_THRIFT_PORT", "9090"))
        self._conn: Any = None
        self._table: Any = None

    def connect(self, timeout: int = 15_000) -> "HBaseClient":
        import happybase  # lazy: only needed when HBase is actually used
        self._conn = happybase.Connection(self.host, self.port, timeout=timeout)
        self._conn.open()
        names = [t.decode() if isinstance(t, bytes) else t for t in self._conn.tables()]
        if TABLE_NAME not in names:
            self._conn.create_table(TABLE_NAME, {COLUMN_FAMILY: dict(max_versions=1, time_to_live=TTL_SECONDS)})
            logger.info("Created HBase table %s (TTL 7 days)", TABLE_NAME)
        self._table = self._conn.table(TABLE_NAME)
        return self

    def put_surge(self, zone_id, timestamp_str, multiplier, demand, supply, ratio, rain_mm=None, temp_c=None, on_trip=0):
        key = format_hbase_row_key(zone_id, timestamp_str)
        vals = {"multiplier": multiplier, "demand": demand, "supply": supply, "ratio": ratio, "on_trip": on_trip,
                "rain_mm": "" if rain_mm is None else rain_mm, "temp_c": "" if temp_c is None else temp_c, "updated_at": timestamp_str}
        self._table.put(key.encode(), {f"{COLUMN_FAMILY}:{k}".encode(): str(v).encode() for k, v in vals.items()})
        return key

    def _rows_between(self, zone_id, start_key, stop_key):
        out = []
        for k, v in self._table.scan(row_start=start_key.encode(), row_stop=(stop_key + "~").encode()):
            out.append((k.decode(), {c.decode().split(":")[-1]: x.decode() for c, x in v.items()}))
        return out

    def close(self) -> None:
        if self._conn:
            self._conn.close()


class MemoryStore(_BaseStore):
    """Test double with the same interface. Select explicitly with SURGE_STORE=memory."""
    def __init__(self) -> None:
        self.rows: Dict[str, Dict[str, str]] = {}

    def put_surge(self, zone_id, timestamp_str, multiplier, demand, supply, ratio, rain_mm=None, temp_c=None, on_trip=0):
        key = format_hbase_row_key(zone_id, timestamp_str)
        self.rows[key] = {"multiplier": str(multiplier), "demand": str(demand), "supply": str(supply), "ratio": str(ratio),
                          "on_trip": str(on_trip), "rain_mm": "" if rain_mm is None else str(rain_mm),
                          "temp_c": "" if temp_c is None else str(temp_c), "updated_at": timestamp_str}
        return key

    def _rows_between(self, zone_id, start_key, stop_key):
        return [(k, v) for k, v in sorted(self.rows.items()) if start_key <= k <= stop_key + "~"]


_MEM = MemoryStore()


def get_store() -> _BaseStore:
    if os.getenv("SURGE_STORE", "hbase").lower() == "memory":
        return _MEM
    return HBaseClient().connect()          # raises if HBase is down: failures must be visible


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    get_store(); print("HBase table ready:", TABLE_NAME)
