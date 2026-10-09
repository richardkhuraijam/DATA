"""HBase Schema & Client Utility.

Defines table surge_multipliers with column family 's'.
Row key: zero-padded zone_id#yyyymmddHHMM (e.g. 079#202610080100)
Supports HappyBase client with mock/fallback for offline dev environments.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("hbase_schema")

TABLE_NAME = "surge_multipliers"
COLUMN_FAMILY = "s"


def format_hbase_row_key(zone_id: int, timestamp_str: str) -> str:
    """Format row key zero-padded to 3 digits: zone_id#yyyymmddHHMM."""
    clean_ts = timestamp_str.replace("-", "").replace(":", "").replace("T", "").replace(" ", "")[:12]
    return f"{int(zone_id):03d}#{clean_ts}"


class HBaseClient:
    def __init__(self, host: str = "localhost", port: int = 9090) -> None:
        self.host = os.getenv("HBASE_THRIFT_HOST", host)
        self.port = int(os.getenv("HBASE_THRIFT_PORT", str(port)))
        self._connection: Optional[Any] = None
        self._in_memory_store: Dict[str, Dict[bytes, bytes]] = {}

    def connect(self) -> bool:
        try:
            import happybase  # type: ignore

            self._connection = happybase.Connection(self.host, self.port, timeout=5000)
            self._connection.open()
            tables = [t.decode("utf-8") if isinstance(t, bytes) else t for t in self._connection.tables()]
            if TABLE_NAME not in tables:
                self._connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict(max_versions=1, ttl=7 * 86400)})
                logger.info("Created HBase table '%s' with TTL=7 days.", TABLE_NAME)
            logger.info("Connected to HBase at %s:%d", self.host, self.port)
            return True
        except Exception as exc:
            logger.warning("Could not connect to HBase Thrift server at %s:%d (%s). Operating in memory mode.", self.host, self.port, exc)
            self._connection = None
            return False

    def put_surge(
        self,
        zone_id: int,
        timestamp_str: str,
        multiplier: float,
        demand: int,
        supply: int,
        ratio: float,
        rain_mm: float = 0.0,
        temp_c: float = 20.0,
    ) -> str:
        row_key = format_hbase_row_key(zone_id, timestamp_str)
        data = {
            f"{COLUMN_FAMILY}:multiplier": str(multiplier).encode("utf-8"),
            f"{COLUMN_FAMILY}:demand": str(demand).encode("utf-8"),
            f"{COLUMN_FAMILY}:supply": str(supply).encode("utf-8"),
            f"{COLUMN_FAMILY}:ratio": str(ratio).encode("utf-8"),
            f"{COLUMN_FAMILY}:rain_mm": str(rain_mm).encode("utf-8"),
            f"{COLUMN_FAMILY}:temp_c": str(temp_c).encode("utf-8"),
            f"{COLUMN_FAMILY}:updated_at": timestamp_str.encode("utf-8"),
        }

        if self._connection:
            try:
                table = self._connection.table(TABLE_NAME)
                table.put(row_key.encode("utf-8"), data)
                return row_key
            except Exception as exc:
                logger.warning("HBase put failed: %s. Storing in memory fallback.", exc)

        self._in_memory_store[row_key] = data
        return row_key

    def get_latest_surge(self, zone_id: int) -> Optional[Dict[str, str]]:
        prefix = f"{int(zone_id):03d}#"
        if self._connection:
            try:
                table = self._connection.table(TABLE_NAME)
                rows = list(table.scan(row_prefix=prefix.encode("utf-8"), reverse=True, limit=1))
                if rows:
                    _, data = rows[0]
                    return {k.decode("utf-8").split(":")[-1]: v.decode("utf-8") for k, v in data.items()}
            except Exception as exc:
                logger.warning("HBase scan failed: %s", exc)

        matching_keys = sorted([k for k in self._in_memory_store.keys() if k.startswith(prefix)], reverse=True)
        if matching_keys:
            data = self._in_memory_store[matching_keys[0]]
            return {k.decode("utf-8").split(":")[-1]: v.decode("utf-8") for k, v in data.items()}

        return None
