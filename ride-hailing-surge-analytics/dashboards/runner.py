"""Process management for demo pipeline: weather, demand, gps, engine.

Writes PIDs to .run/<name>.pid and logs to logs/<name>.log.
Starts engine last, refuses to start duplicates, cleans up stale PIDs,
and terminates with SIGTERM followed by SIGKILL after timeout.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / ".run"
LOGS_DIR = ROOT / "logs"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("demo_runner")

PROCESS_SPECS: Dict[str, Dict[str, Any]] = {
    "weather": {
        "command": [sys.executable, "-m", "ingestion.producers.weather"],
        "order": 1,
        "description": "Open-Meteo current weather poller",
    },
    "demand": {
        "command": [
            sys.executable,
            "-m",
            "ingestion.producers.demand_stream",
            "--simulate-hour",
            "19",
            "--hotspot",
            "Hitech City",
            "--rain-mm",
            "5",
        ],
        "order": 2,
        "description": "Live trip request generator (peak hour 19, Hitech City hotspot)",
    },
    "gps": {
        "command": [
            sys.executable,
            "-m",
            "ingestion.producers.gps_simulator",
            "--drivers",
            "200",
            "--simulate-hour",
            "19",
        ],
        "order": 3,
        "description": "Fleet GPS pings (200 drivers, peak hour 19)",
    },
    "engine": {
        "command": [sys.executable, "-m", "processing.streaming.surge_stream"],
        "order": 4,
        "description": "Spark Structured Streaming surge engine",
    },
}


def _ensure_dirs() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def get_pid_file(name: str) -> Path:
    return RUN_DIR / f"{name}.pid"


def get_log_file(name: str) -> Path:
    return LOGS_DIR / f"{name}.log"


def is_pid_alive(pid: int) -> bool:
    """Check if process with given PID is still running."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def get_process_pid(name: str) -> Optional[int]:
    """Read PID file for process name if valid and running; cleans up stale PID files."""
    pf = get_pid_file(name)
    if not pf.exists():
        return None
    try:
        content = pf.read_text().strip()
        if not content:
            pf.unlink(missing_ok=True)
            return None
        pid = int(content)
        if is_pid_alive(pid):
            return pid
        # PID is dead -> cleanup stale file
        logger.info("Found stale PID file for %s (pid=%d dead); removing", name, pid)
        pf.unlink(missing_ok=True)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error reading PID file for %s: %s", name, exc)
        pf.unlink(missing_ok=True)
        return None


def start_process(name: str) -> Dict[str, Any]:
    """Start a single process if not already running."""
    _ensure_dirs()
    if name not in PROCESS_SPECS:
        return {"name": name, "success": False, "error": f"Unknown process name: {name}"}

    existing_pid = get_process_pid(name)
    if existing_pid is not None:
        return {
            "name": name,
            "success": False,
            "pid": existing_pid,
            "error": f"Process '{name}' is already running with PID {existing_pid}",
        }

    spec = PROCESS_SPECS[name]
    log_path = get_log_file(name)
    log_file = open(log_path, "a", encoding="utf-8")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    try:
        proc = subprocess.Popen(
            spec["command"],
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=str(ROOT),
            env=env,
            start_new_session=True,  # Run in its own process group
        )
        pid = proc.pid
        get_pid_file(name).write_text(str(pid))
        logger.info("Started %s [PID %d] -> %s", name, pid, log_path)
        log_file.close()
        return {"name": name, "success": True, "pid": pid}
    except Exception as exc:  # noqa: BLE001
        log_file.close()
        logger.error("Failed to start %s: %s", name, exc)
        return {"name": name, "success": False, "error": str(exc)}


def stop_process(name: str, timeout: float = 6.0) -> Dict[str, Any]:
    """Stop a process cleanly with SIGTERM, falling back to SIGKILL after timeout."""
    pid = get_process_pid(name)
    pf = get_pid_file(name)
    if pid is None:
        pf.unlink(missing_ok=True)
        return {"name": name, "success": True, "status": "not running"}

    try:
        pgid = os.getpgid(pid)
    except Exception:
        pgid = pid

    logger.info("Stopping %s [PID %d] with SIGTERM...", name, pid)
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass

    # Wait for process exit
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not is_pid_alive(pid):
            break
        time.sleep(0.3)

    # Force kill if still alive
    if is_pid_alive(pid):
        logger.warning("Process %s [PID %d] did not exit after %ss; sending SIGKILL", name, pid, timeout)
        try:
            os.killpg(pgid, signal.SIGKILL)
        except Exception:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        time.sleep(0.5)

    pf.unlink(missing_ok=True)
    return {"name": name, "success": True, "status": "stopped"}


def start_all() -> Dict[str, Any]:
    """Start weather, demand, gps, and then engine last."""
    _ensure_dirs()
    results = {}
    ordered_names = sorted(PROCESS_SPECS.keys(), key=lambda k: PROCESS_SPECS[k]["order"])

    for name in ordered_names:
        if name == "engine":
            # Give producers 1.5 seconds to publish initial records
            time.sleep(1.5)
        res = start_process(name)
        results[name] = res

    return results


def stop_all() -> Dict[str, Any]:
    """Stop all running demo processes."""
    results = {}
    # Stop engine first, then producers
    ordered_names = sorted(PROCESS_SPECS.keys(), key=lambda k: PROCESS_SPECS[k]["order"], reverse=True)
    for name in ordered_names:
        results[name] = stop_process(name)
    return results


def get_demo_status() -> Dict[str, Any]:
    """Return status of all demo processes."""
    status = {}
    for name, spec in PROCESS_SPECS.items():
        pid = get_process_pid(name)
        status[name] = {
            "name": name,
            "running": pid is not None,
            "pid": pid,
            "description": spec["description"],
            "log_file": str(get_log_file(name)),
        }
    return status


def get_docker_status() -> Dict[str, Any]:
    """Check health/status of ridehail docker containers."""
    containers = ["ridehail-kafka1-1", "ridehail-kafka2-1", "ridehail-kafka3-1", "ridehail-hbase-1", "ridehail-mysql-1"]
    status_map = {}
    try:
        out = subprocess.check_output(
            ["docker", "ps", "-a", "--filter", "name=ridehail-", "--format", "{{.Names}}\t{{.Status}}\t{{.State}}"],
            text=True,
            timeout=5,
        )
        for line in out.strip().splitlines():
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                name, st_str, state = parts[0], parts[1], parts[2]
                status_map[name] = {"status": st_str, "state": state, "up": state.lower() == "running"}
    except Exception as exc:
        status_map["error"] = str(exc)

    all_up = True
    for c in containers:
        if not status_map.get(c, {}).get("up", False):
            all_up = False
            break

    return {"containers": status_map, "all_up": all_up}


def get_log_tail(name: str, lines: int = 100) -> str:
    """Return the last N lines from the log file of the specified process."""
    log_path = get_log_file(name)
    if not log_path.exists():
        return f"Log file for '{name}' does not exist yet ({log_path})"
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
            tail_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return "".join(tail_lines)
    except Exception as exc:  # noqa: BLE001
        return f"Error reading log file {log_path}: {exc}"


def cli() -> None:
    parser = argparse.ArgumentParser(description="Demo pipeline runner")
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("start", help="Start all demo processes")
    sub.add_parser("stop", help="Stop all demo processes")
    sub.add_parser("status", help="Show demo processes status")
    
    start_one_parser = sub.add_parser("start-one", help="Start single process")
    start_one_parser.add_argument("name", choices=list(PROCESS_SPECS.keys()))

    stop_one_parser = sub.add_parser("stop-one", help="Stop single process")
    stop_one_parser.add_argument("name", choices=list(PROCESS_SPECS.keys()))

    tail_parser = sub.add_parser("tail", help="Tail logs")
    tail_parser.add_argument("name", choices=list(PROCESS_SPECS.keys()))
    tail_parser.add_argument("-n", "--lines", type=int, default=100)

    args = parser.parse_args()

    if args.action == "start":
        res = start_all()
        for k, v in res.items():
            st = f"RUNNING [PID {v.get('pid')}]" if v.get("success") else f"FAILED: {v.get('error')}"
            print(f"  {k:10s} : {st}")
    elif args.action == "stop":
        res = stop_all()
        for k, v in res.items():
            print(f"  {k:10s} : {v.get('status')}")
    elif args.action == "status":
        st = get_demo_status()
        print("\n--- DEMO PIPELINE STATUS ---")
        for k, v in st.items():
            state = f"RUNNING (PID {v['pid']})" if v["running"] else "STOPPED"
            print(f"  {k:10s} : {state:<20s} ({v['description']})")
        dk = get_docker_status()
        print(f"\nDocker Stack: {'ALL UP' if dk.get('all_up') else 'PARTIAL / DOWN'}")
        for cname, cinfo in dk.get("containers", {}).items():
            if isinstance(cinfo, dict):
                print(f"  {cname:20s} : {cinfo.get('status', 'unknown')}")
        print()
    elif args.action == "start-one":
        r = start_process(args.name)
        print(json.dumps(r, indent=2))
    elif args.action == "stop-one":
        r = stop_process(args.name)
        print(json.dumps(r, indent=2))
    elif args.action == "tail":
        print(get_log_tail(args.name, args.lines))


if __name__ == "__main__":
    cli()
