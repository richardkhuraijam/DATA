"""Environment-driven settings. No hosts or credentials are hardcoded."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
TLC_DIR = RAW_DIR / "tlc"
ZONES_DIR = RAW_DIR / "zones"


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_optional(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def mysql_url() -> str:
    user = env("MYSQL_USER")
    password = env("MYSQL_PASSWORD")
    host = env("MYSQL_HOST", "127.0.0.1")
    port = env("MYSQL_PORT", "3306")
    database = env("MYSQL_DATABASE")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
