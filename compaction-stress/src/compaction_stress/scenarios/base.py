"""Scenario workers using kgo-verifier subprocesses."""

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
_RECORDS = 0
_BYTES = 1
_ERRORS = 2
_TOMBSTONES = 3
STATS_SIZE = 4

# Base port for kgo-verifier --remote-port. Each worker gets base + offset.
_REMOTE_PORT_BASE = 7900
_next_port = _REMOTE_PORT_BASE


def _alloc_port() -> int:
    global _next_port
    port = _next_port
    _next_port += 1
    return port


def key_prefixes_for(name: str, topics: list[str]) -> list[str]:
    """Return a key prefix per topic for the given scenario."""
    prefix_map = {
        "key_cardinality": "kc",
        "extreme_dedup": "ed",
        "continuous_write": "cw",
        "tombstone": "ts",
    }
    if name == "multi_partition":
        return [f"mp{i}" for i in range(len(topics))]
    return [prefix_map.get(name, name[:2])] * len(topics)


def _find_kgo_verifier() -> str | None:
    """Find the kgo-verifier binary."""
    # Check common locations
    for candidate in [
        Path.home() / "co" / "kgo-verifier" / "kgo-verifier",
        Path(__file__).resolve().parent.parent.parent.parent / "kgo-verifier",
    ]:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return shutil.which("kgo-verifier")


KGO_VERIFIER = _find_kgo_verifier()


def _kgo_cmd(
    cluster: ClusterConfig,
    topic: str,
    config: ScenarioConfig,
    rate_limit_bps: int,
    remote_port: int,
) -> list[str]:
    """Build a kgo-verifier producer command line."""
    cmd = [
        KGO_VERIFIER,
        "--brokers", cluster.brokers,
        "--topic", topic,
        "--msg_size", str(config.msg_size),
        # Large produce count — effectively infinite; we kill the process on shutdown
        "--produce_msgs", str(10_000_000_000),
        "--produce-throughput-bps", str(rate_limit_bps),
        "--key-set-cardinality", str(config.key_count),
        "--max-buffered-records", "8192",
        "--batch_max_bytes", "1048576",
        "--tolerate-failed-produce",
        "--remote",
        "--remote-port", str(remote_port),
    ]
    if config.tombstone_probability > 0:
        cmd += ["--tombstone-probability", str(config.tombstone_probability)]
    if cluster.sasl_user:
        cmd += ["--username", cluster.sasl_user]
    if cluster.sasl_password:
        cmd += ["--password", cluster.sasl_password]
    if cluster.tls_enabled:
        cmd += ["--enable-tls"]
    return cmd


def _redact_cmd(cmd: list[str]) -> str:
    safe = []
    skip_next = False
    for arg in cmd:
        if skip_next:
            safe.append("***")
            skip_next = False
        elif arg == "--password":
            safe.append(arg)
            skip_next = True
        else:
            safe.append(arg)
    return " ".join(safe)


class KgoWorker:
    """Manages a kgo-verifier subprocess and polls its HTTP status endpoint."""

    def __init__(
        self,
        name: str,
        proc: subprocess.Popen,
        remote_port: int,
        stats: multiprocessing.Array,
    ):
        self.name = name
        self.proc = proc
        self.port = remote_port
        self._stats = stats
        self._poll_thread: threading.Thread | None = None

    def start_polling(self, interval: float = 2.0) -> None:
        self._poll_thread = threading.Thread(
            target=self._poll_loop, args=(interval,), daemon=True,
        )
        self._poll_thread.start()

    def _poll_loop(self, interval: float) -> None:
        url = f"http://localhost:{self.port}/status"
        while self.proc.poll() is None:
            try:
                resp = requests.get(url, timeout=2)
                if resp.ok:
                    d = resp.json()
                    # kgo-verifier status returns various fields; extract what we need
                    produced = d.get("produced", 0)
                    bad_offsets = d.get("bad_offsets", 0)
                    # Approximate bytes from produced * msg_size (kgo-verifier
                    # doesn't report bytes directly in status)
                    self._stats[_RECORDS] = produced
                    self._stats[_ERRORS] = bad_offsets
            except Exception:
                pass
            time.sleep(interval)

    def activate(self) -> None:
        """Tell kgo-verifier to start producing (remote mode waits for activation)."""
        url = f"http://localhost:{self.port}/activate"
        for _ in range(30):
            try:
                resp = requests.put(url, timeout=2)
                if resp.ok:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        print(f"[{self.name}] WARNING: failed to activate kgo-verifier", flush=True)

    def shutdown(self) -> None:
        """Graceful shutdown via HTTP, then SIGTERM if needed."""
        try:
            requests.put(f"http://localhost:{self.port}/shutdown", timeout=2)
        except Exception:
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


def start_kgo_worker(
    name: str,
    worker_id: int,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topic: str,
    rate_limit_bps: int,
    stats: multiprocessing.Array,
) -> KgoWorker:
    """Start a kgo-verifier producer subprocess."""
    port = _alloc_port()
    cmd = _kgo_cmd(cluster, topic, config, rate_limit_bps, port)
    label = f"{name}/w{worker_id}/{topic}"
    print(f"[{label}] cmd: {_redact_cmd(cmd)}", flush=True)
    print(f"[{label}] Starting kgo-verifier: {topic} at "
          f"{rate_limit_bps // (1024*1024)} MB/s (port {port})", flush=True)

    proc = subprocess.Popen(
        cmd,
        stdout=None,  # inherit stdout for kgo-verifier's own logging
        stderr=None,  # inherit stderr
    )

    worker = KgoWorker(label, proc, port, stats)
    worker.start_polling()
    worker.activate()
    return worker


# ── ScenarioHandle: aggregates stats from multiple workers ───────────


class ScenarioHandle:
    """Main-process handle for reading stats from one or more worker processes."""

    def __init__(self, name: str, num_topics: int, msg_size: int):
        self.name = name
        self.num_topics = num_topics
        self.msg_size = msg_size
        self._stats_arrays: list = []
        self._prev_bytes = 0.0
        self._prev_records = 0
        self._prev_time = time.monotonic()

    def add_worker_stats(self, stats: multiprocessing.Array) -> None:
        self._stats_arrays.append(stats)

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        records = 0
        errors = 0
        tombstones = 0
        for sa in self._stats_arrays:
            records += int(sa[_RECORDS])
            errors += int(sa[_ERRORS])
            tombstones += int(sa[_TOMBSTONES])

        # Approximate bytes from records * msg_size
        total_bytes = float(records * self.msg_size)
        elapsed = now - self._prev_time
        bps = (total_bytes - self._prev_bytes) / elapsed if elapsed > 0 else 0.0
        self._prev_bytes = total_bytes
        self._prev_records = records
        self._prev_time = now

        return {
            "records": records,
            "bytes_per_sec": bps,
            "errors": errors,
            "tombstones": tombstones,
            "num_topics": self.num_topics,
        }
