"""Tests for dashboards.runner process management."""
from __future__ import annotations

import os
from pathlib import Path
import time
from unittest.mock import patch

import pytest

from dashboards import runner


@pytest.fixture
def temp_run_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Isolate PID and logs directories during tests."""
    run_dir = tmp_path / ".run"
    logs_dir = tmp_path / "logs"
    run_dir.mkdir(parents=True)
    logs_dir.mkdir(parents=True)
    monkeypatch.setattr(runner, "RUN_DIR", run_dir)
    monkeypatch.setattr(runner, "LOGS_DIR", logs_dir)
    return run_dir, logs_dir


def test_pid_lifecycle(temp_run_dir):
    run_dir, _ = temp_run_dir
    name = "test_proc"
    pid_file = runner.get_pid_file(name)

    assert not pid_file.exists()
    assert runner.get_process_pid(name) is None

    # Write a live PID (current process)
    pid_file.write_text(str(os.getpid()))
    assert runner.get_process_pid(name) == os.getpid()

    pid_file.unlink()
    assert runner.get_process_pid(name) is None


def test_is_pid_alive():
    # Current process PID is definitely running
    assert runner.is_pid_alive(os.getpid()) is True
    # PID 99999999 should not be running
    assert runner.is_pid_alive(99999999) is False
    assert runner.is_pid_alive(0) is False
    assert runner.is_pid_alive(-1) is False


def test_get_status_stale_pid_cleanup(temp_run_dir):
    name = "weather"
    pid_file = runner.get_pid_file(name)
    # Write a dead PID
    pid_file.write_text("99999999")

    # get_process_pid cleans up stale PID files
    pid = runner.get_process_pid(name)
    assert pid is None
    assert not pid_file.exists()

    status = runner.get_demo_status()
    assert status[name]["running"] is False
    assert status[name]["pid"] is None


def test_engine_starts_last():
    """Verify that process start order places engine last."""
    specs = runner.PROCESS_SPECS
    assert specs["engine"]["order"] > specs["weather"]["order"]
    assert specs["engine"]["order"] > specs["demand"]["order"]
    assert specs["engine"]["order"] > specs["gps"]["order"]


def test_duplicate_start_refusal(temp_run_dir):
    name = "weather"
    runner.get_pid_file(name).write_text(str(os.getpid()))

    res = runner.start_process(name)
    assert res["success"] is False
    assert "already running" in res.get("error", "")


def test_unknown_process(temp_run_dir):
    res = runner.start_process("nonexistent_service")
    assert res["success"] is False
    assert "Unknown process" in res.get("error", "")

    stop_res = runner.stop_process("nonexistent_service")
    assert stop_res["success"] is True
    assert stop_res["status"] == "not running"


def test_log_tail(temp_run_dir):
    _, logs_dir = temp_run_dir
    log_file = logs_dir / "weather.log"

    assert "does not exist yet" in runner.get_log_tail("weather")

    log_file.write_text("\n".join(f"line {i}" for i in range(150)))
    tail = runner.get_log_tail("weather", lines=10)
    lines = tail.strip().split("\n")
    assert len(lines) == 10
    assert lines[-1] == "line 149"
