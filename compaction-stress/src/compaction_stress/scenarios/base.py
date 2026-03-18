"""Scenario workers using kgo-verifier subprocesses (produce-only).

Each scenario gets one kgo-verifier process. kgo-verifier produces
until killed, with configurable key cardinality, message size, rate
limiting, and throughput. No consumer group needed.
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
_ACKED = 1
_ERRORS = 2
_TOMBSTONES = 3
STATS_SIZE = 4

_next_port = 7900


def _alloc_port() -> int:
    global _next_port
    p = _next_port
    _next_port += 1
    return p


def _find_binary() -> str | None:
    for candidate in [
        Path.home() / "co" / "kgo-verifier" / "kgo-verifier",
        Path(__file__).resolve().parent.parent.parent.parent / "kgo-verifier",
    ]:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return shutil.which("kgo-verifier")


KGO_VERIFIER = _find_binary()


class VerifierWorker:
    """One kgo-verifier process producing for a scenario."""

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
                    # kgo-verifier /status returns a single object (not array)
                    # with sent, acked, bad_offsets, tombstones_produced, etc.
                    if isinstance(data, list) and len(data) > 0:
                        data = data[0]
                    if isinstance(data, dict):
                        self._stats[_PRODUCED] = data.get("sent", 0)
                        self._stats[_ACKED] = data.get("acked", 0)
                        self._stats[_ERRORS] = data.get("bad_offsets", 0)
                        self._stats[_TOMBSTONES] = data.get("tombstones_produced", 0)
            except Exception:
                pass
            time.sleep(interval)

    def shutdown(self) -> None:
        try:
            requests.put(f"http://localhost:{self.port}/shutdown", timeout=2)
        except Exception:
            pass
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


def _redact_cmd(cmd: list[str]) -> str:
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
    return " ".join(safe)


def start_verifier(
    name: str,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topic: str,
    rate_limit_bps: int,
    stats: multiprocessing.Array,
) -> VerifierWorker:
    port = _alloc_port()
    cmd = [
        KGO_VERIFIER,
        "--brokers", cluster.brokers,
        "--topic", topic,
        "--msg_size", str(config.msg_size),
        "--produce_msgs", str(10_000_000_000),
        "--produce-throughput-bps", str(rate_limit_bps),
        "--key-set-cardinality", str(config.key_count),
        "--max-buffered-records", "8192",
        "--batch_max_bytes", "1048576",
        "--tolerate-data-loss",
        "--tolerate-failed-produce",
        "--seq_read=0",
        "--rand_read_msgs", "0",
        "--remote",
        "--remote-port", str(port),
    ]
    if cluster.sasl_user:
        cmd += ["--username", cluster.sasl_user]
    if cluster.sasl_password:
        cmd += ["--password", cluster.sasl_password]
    if cluster.tls_enabled:
        cmd += ["--enable-tls"]

    label = f"{name}/{topic}"
    print(f"[{label}] cmd: {_redact_cmd(cmd)}", flush=True)
    print(f"[{label}] Starting kgo-verifier at "
          f"{rate_limit_bps // (1024*1024)} MB/s (port {port})", flush=True)

    proc = subprocess.Popen(cmd, stdout=None, stderr=None)
    worker = VerifierWorker(label, proc, port, stats)
    worker.start_polling()
    return worker


class ScenarioHandle:
    """Reads and aggregates stats from a scenario's workers."""

    def __init__(self, name: str, num_topics: int, msg_size: int):
        self.name = name
        self.num_topics = num_topics
        self.msg_size = msg_size
        self._stats_arrays: list = []
        self._prev_acked = 0
        self._prev_time = time.monotonic()

    def add_stats(self, stats: multiprocessing.Array) -> None:
        self._stats_arrays.append(stats)

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        produced = acked = errors = tombstones = 0
        for sa in self._stats_arrays:
            produced += int(sa[_PRODUCED])
            acked += int(sa[_ACKED])
            errors += int(sa[_ERRORS])
            tombstones += int(sa[_TOMBSTONES])

        elapsed = now - self._prev_time
        delta = acked - self._prev_acked
        bps = (delta * self.msg_size) / elapsed if elapsed > 0 else 0
        self._prev_acked = acked
        self._prev_time = now

        return {
            "records": acked,
            "bytes_per_sec": bps,
            "errors": errors,
            "tombstones": tombstones,
            "num_topics": self.num_topics,
        }
