"""Scenario workers: Go subprocess (preferred) or Python multiprocessing fallback."""

from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from compaction_stress.config import ClusterConfig, ScenarioConfig


# Shared stats array indices (used by both Go reader and Python fallback)
_RECORDS = 0
_BYTES = 1
_ERRORS = 2
_TOMBSTONES = 3
STATS_SIZE = 4


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


def _find_go_binary() -> str | None:
    """Find the ct-producer Go binary."""
    # Check next to the Python package first
    pkg_dir = Path(__file__).resolve().parent.parent.parent.parent
    candidate = pkg_dir / "ct-producer" / "ct-producer"
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return str(candidate)
    # Check PATH
    return shutil.which("ct-producer")


GO_BINARY = _find_go_binary()


def _go_worker_cmd(
    cluster: ClusterConfig,
    topic: str,
    key_prefix: str,
    config: ScenarioConfig,
    rate_limit_bps: int,
) -> list[str]:
    """Build the ct-producer command line."""
    cmd = [
        GO_BINARY,
        "--brokers", cluster.brokers,
        "--topic", topic,
        "--key-prefix", key_prefix,
        "--key-count", str(config.key_count),
        "--msg-size", str(config.msg_size),
        "--rate-limit", str(rate_limit_bps),
    ]
    if config.tombstone_probability > 0:
        cmd += ["--tombstone-prob", str(config.tombstone_probability)]
    if cluster.sasl_mechanism and cluster.sasl_user:
        cmd += [
            "--sasl-mechanism", cluster.sasl_mechanism,
            "--sasl-user", cluster.sasl_user,
            "--sasl-password", cluster.sasl_password or "",
        ]
    if cluster.tls_enabled:
        cmd += ["--tls"]
    return cmd


def start_go_worker(
    name: str,
    worker_id: int,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topic: str,
    key_prefix: str,
    rate_limit_bps: int,
    stats: multiprocessing.Array,
) -> subprocess.Popen:
    """Start a ct-producer Go subprocess and a reader thread for its stats."""
    cmd = _go_worker_cmd(cluster, topic, key_prefix, config, rate_limit_bps)
    label = f"{name}/w{worker_id}"
    print(f"[{label}] Starting Go producer: {topic} at "
          f"{rate_limit_bps // (1024*1024)} MB/s", flush=True)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=None,  # inherit stderr for error visibility
        bufsize=0,  # unbuffered — we read line by line below
    )

    # Reader thread: parse JSON stats lines from stdout, update shared array.
    # Use readline() instead of iterating (which buffers in 8KB chunks and
    # blocks until a full chunk is available).
    def reader():
        while True:
            line = proc.stdout.readline()
            if not line:
                break  # EOF — process exited
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                stats[_RECORDS] = d.get("records", 0)
                stats[_BYTES] = d.get("bytes", 0)
                stats[_ERRORS] = d.get("errors", 0)
                stats[_TOMBSTONES] = d.get("tombstones", 0)
            except (json.JSONDecodeError, ValueError):
                pass

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    return proc


# ── Python fallback (used when Go binary not found) ──────────────────


def _update_stats(stats: multiprocessing.Array, producers: list) -> None:
    stats[_RECORDS] = sum(sp.records for sp in producers)
    stats[_BYTES] = sum(sp.total_bytes for sp in producers)
    stats[_ERRORS] = sum(sp.errors for sp in producers)
    stats[_TOMBSTONES] = sum(sp.tombstones for sp in producers)


def scenario_worker(
    name: str,
    worker_id: int,
    num_workers: int,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topics: list[str],
    key_prefixes: list[str],
    shutdown: multiprocessing.Event,
    stats: multiprocessing.Array,
) -> None:
    """Python fallback worker — used when Go binary is not available."""
    import signal as _signal
    _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
    _signal.signal(_signal.SIGTERM, _signal.SIG_IGN)

    from compaction_stress.producer import StressProducer, make_producer

    worker_rate = max(1024, config.rate_limit_bps // num_workers)
    per_topic_rate = max(1024, worker_rate // max(len(topics), 1))
    producers: list[StressProducer] = []

    for topic, prefix in zip(topics, key_prefixes):
        p = make_producer(cluster)
        sp = StressProducer(
            producer=p,
            topic=topic,
            key_prefix=prefix,
            key_count=config.key_count,
            msg_size=config.msg_size,
            rate_limit_bps=per_topic_rate,
            tombstone_probability=config.tombstone_probability,
        )
        producers.append(sp)

    label = f"{name}/w{worker_id}" if num_workers > 1 else name
    print(f"[{label}] Starting Python producer to {len(topics)} topic(s) "
          f"at {per_topic_rate // (1024*1024)} MB/s per topic", flush=True)

    while not shutdown.is_set():
        for sp in producers:
            sp.produce_batch(batch_size=10000)
        _update_stats(stats, producers)

    print(f"[{label}] Shutting down, flushing producers...", flush=True)
    for sp in producers:
        sp.flush(timeout=5.0)
    _update_stats(stats, producers)


# ── ScenarioHandle: aggregates stats from multiple workers ───────────


class ScenarioHandle:
    """Main-process handle for reading stats from one or more worker processes."""

    def __init__(self, name: str, num_topics: int):
        self.name = name
        self.num_topics = num_topics
        self._stats_arrays: list = []
        self._prev_bytes = 0.0
        self._prev_time = time.monotonic()

    def add_worker_stats(self, stats: multiprocessing.Array) -> None:
        self._stats_arrays.append(stats)

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        records = 0
        total_bytes = 0.0
        errors = 0
        tombstones = 0
        for sa in self._stats_arrays:
            records += int(sa[_RECORDS])
            total_bytes += sa[_BYTES]
            errors += int(sa[_ERRORS])
            tombstones += int(sa[_TOMBSTONES])

        elapsed = now - self._prev_time
        bps = (total_bytes - self._prev_bytes) / elapsed if elapsed > 0 else 0.0
        self._prev_bytes = total_bytes
        self._prev_time = now

        return {
            "records": records,
            "bytes_per_sec": bps,
            "errors": errors,
            "tombstones": tombstones,
            "num_topics": self.num_topics,
        }
