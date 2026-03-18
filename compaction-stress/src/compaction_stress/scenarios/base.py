"""Scenario workers using kgo-repeater subprocesses.

Each scenario gets one kgo-repeater process with its own consumer group.
kgo-repeater handles produce+consume internally with configurable
parallelism (--workers), key space (--keys), and rate limiting.
"""

from __future__ import annotations

import multiprocessing
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import requests

from compaction_stress.config import ClusterConfig, ScenarioConfig

# Shared stats array indices
_PRODUCED = 0
_CONSUMED = 1
_ERRORS = 2
_ENQUEUED = 3
STATS_SIZE = 4

_next_port = 7900


def _alloc_port() -> int:
    global _next_port
    p = _next_port
    _next_port += 1
    return p


def _find_binary() -> str | None:
    for candidate in [
        Path.home() / "co" / "kgo-verifier" / "kgo-repeater",
        Path(__file__).resolve().parent.parent.parent.parent / "kgo-repeater",
    ]:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return shutil.which("kgo-repeater")


KGO_REPEATER = _find_binary()


class RepeaterWorker:
    """One kgo-repeater process producing+consuming for a scenario."""

    def __init__(self, name: str, proc: subprocess.Popen, port: int,
                 stats: multiprocessing.Array):
        self.name = name
        self.proc = proc
        self.port = port
        self._stats = stats

    def start_polling(self, interval: float = 2.0) -> None:
        t = threading.Thread(target=self._poll, args=(interval,), daemon=True)
        t.start()

    def _poll(self, interval: float) -> None:
        url = f"http://localhost:{self.port}/status"
        while self.proc.poll() is None:
            try:
                resp = requests.get(url, timeout=2)
                if resp.ok:
                    data = resp.json()
                    produced = consumed = errors = enqueued = 0
                    if isinstance(data, list):
                        for w in data:
                            produced += w.get("produced", 0)
                            consumed += w.get("consumed", 0)
                            errors += w.get("errors", 0)
                            enqueued += w.get("enqueued", 0)
                    self._stats[_PRODUCED] = produced
                    self._stats[_CONSUMED] = consumed
                    self._stats[_ERRORS] = errors
                    self._stats[_ENQUEUED] = enqueued
            except Exception:
                pass
            time.sleep(interval)

    def activate(self) -> None:
        url = f"http://localhost:{self.port}/activate"
        for _ in range(60):
            try:
                resp = requests.put(url, timeout=2)
                if resp.ok:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        print(f"[{self.name}] WARNING: failed to activate", flush=True)

    def shutdown(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)


def start_repeater(
    name: str,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topics: list[str],
    group: str,
    stats: multiprocessing.Array,
) -> RepeaterWorker:
    port = _alloc_port()
    cmd = [
        KGO_REPEATER,
        "--brokers", cluster.brokers,
        "--topics", ",".join(topics),
        "--group", group,
        "--keys", str(config.key_count),
        "--payload-size", str(config.msg_size),
        "--workers", str(config.num_producers),
        # kgo-repeater has a hardcoded channel of 128K slots. Stay under
        # that: (120000 / workers) * payload_size, converted to MB.
        "--initial-data-mb", str(max(4, 120000 // config.num_producers * config.msg_size // (1024 * 1024))),
        "--max-buffered-records", "8192",
        "--remote",
        "--remote-port", str(port),
    ]
    if config.rate_limit_bps > 0:
        cmd += ["--rate-limit-bps", str(config.rate_limit_bps)]
    # NOTE: kgo-repeater generates its payload once at init. If
    # tombstone_probability triggers, the entire worker produces only nil
    # values, stalling the produce-consume loop. Don't pass it through.
    # Tombstone pressure comes from the topic config (delete.retention.ms)
    # and the high key churn instead.
    if cluster.sasl_user:
        cmd += ["--username", cluster.sasl_user]
    if cluster.sasl_password:
        cmd += ["--password", cluster.sasl_password]
    if cluster.tls_enabled:
        cmd += ["--enable-tls"]

    # Log command (redact password)
    safe = []
    skip = False
    for arg in cmd:
        if skip:
            safe.append("***")
            skip = False
        elif arg == "--password":
            safe.append(arg)
            skip = True
        else:
            safe.append(arg)
    print(f"[{name}] cmd: {' '.join(safe)}", flush=True)

    proc = subprocess.Popen(cmd, stdout=None, stderr=None)
    worker = RepeaterWorker(name, proc, port, stats)
    worker.start_polling()
    worker.activate()
    return worker


class ScenarioHandle:
    """Reads stats from a scenario's RepeaterWorker."""

    def __init__(self, name: str, num_topics: int, msg_size: int):
        self.name = name
        self.num_topics = num_topics
        self.msg_size = msg_size
        self._stats: multiprocessing.Array | None = None
        self._prev_produced = 0
        self._prev_time = time.monotonic()

    def set_stats(self, stats: multiprocessing.Array) -> None:
        self._stats = stats

    def get_stats(self) -> dict[str, Any]:
        if not self._stats:
            return {"records": 0, "bytes_per_sec": 0, "errors": 0,
                    "tombstones": 0, "num_topics": self.num_topics}

        now = time.monotonic()
        produced = int(self._stats[_PRODUCED])
        consumed = int(self._stats[_CONSUMED])
        errors = int(self._stats[_ERRORS])
        enqueued = int(self._stats[_ENQUEUED])

        elapsed = now - self._prev_time
        records_delta = produced - self._prev_produced
        bps = (records_delta * self.msg_size) / elapsed if elapsed > 0 else 0
        self._prev_produced = produced
        self._prev_time = now

        return {
            "records": produced,
            "consumed": consumed,
            "bytes_per_sec": bps,
            "errors": errors,
            "enqueued": enqueued,
            "num_topics": self.num_topics,
        }
