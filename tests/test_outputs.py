import json
import re
from pathlib import Path

LOGS = Path("logs")
SUMMARY = Path("summary.json")
TIMELINE = Path("timeline.json")
ALERTS = Path("alerts.txt")


def _read(p):
    return p.read_text(encoding="utf-8", errors="ignore")


def test_consumer_fifo_and_awk_only():
    """Ensure correct usage of FIFO + awk only (no file reads or system())."""
    run_sh = Path("run.sh").read_text()
    assert "cat pipe/events.fifo | awk -f consumer.awk" in run_sh
    assert "system(" not in Path("consumer.awk").read_text()


def test_runtime_volume_rotations_timeline():
    """Check minimum volume, rotation count, and valid timeline."""
    sm = json.loads(_read(SUMMARY))
    tm = json.loads(_read(TIMELINE))

    assert sm.get("producer_lines_total", 0) >= 1100, "need >=1100 lines"
    assert sm.get("rotations_min", 0) >= 3, "need ≥3 rotations"
    assert sm.get("parallel") is True
    assert tm["consumer_start_ms"] <= tm["producer_start_ms"]
    assert tm["producer_end_ms"] - tm["consumer_start_ms"] >= 12000, "overlap <12s"


def test_alerts_truthy_diverse_antispam():
    """Check alert truthiness, Unicode users, spam filtering."""
    lines = [line for line in _read(ALERTS).splitlines() if line.startswith("ALERT ")]
    users = set()
    for line in lines:
        tokens = line.split()
        assert len(tokens) >= 6, "alert format error"
        users.add(tokens[1])

    assert len(users) >= 2, "need ≥2 distinct users"
    assert any(ord(ch) > 127 for u in users for ch in u), "need ≥1 non-ASCII username"
    assert len(lines) >= 6, "need ≥6 alerts"

    # Check spam control (at least 3s between repeated alerts per user)
    timestamps = {}
    for line in lines:
        tokens = line.split()
        user = tokens[1]
        iso_ts = tokens[-1]
        dt = re.search(r"T(\d\d):(\d\d):(\d\d)Z", iso_ts)
        if not dt: continue
        _, _, s = map(int, dt.groups())
        timestamps.setdefault(user, []).append(s)

    for user, secs in timestamps.items():
        for i in range(1, len(secs)):
            delta = secs[i] - secs[i - 1]
            assert delta >= 3 or secs[i] < secs[i-1], f"alert spam for {user}: {delta}s"


def test_run_sh_parallel_and_idempotent():
    """Check backgrounding and lock mechanism."""
    rs = _read(Path("run.sh"))
    assert "flock" in rs
    assert "producer.py" in rs and "&" in rs, "producer must run in background"


def test_rotations_and_sizes():
    """Check presence and non-trivial size of all rotated logs."""
    log_files = [LOGS / "events.log", LOGS / "events.log.1", LOGS / "events.log.2", LOGS / "events.log.3"]
    for p in log_files:
        assert p.exists(), f"{p.name} missing"
        assert p.stat().st_size >= 2048, f"{p.name} too small; insufficient volume"


def test_summary_timeline_and_parallelism():
    """Ensure required summary + timeline keys are present and valid."""
    sm = json.loads(_read(SUMMARY))
    tm = json.loads(_read(TIMELINE))
    for k in ["producer_lines_total", "alerts", "rotations_min", "parallel"]:
        assert k in sm, f"summary.json missing {k}"
    for k in ["producer_start_ms", "consumer_start_ms", "producer_end_ms", "consumer_end_ms"]:
        assert k in tm, f"timeline.json missing {k}"
