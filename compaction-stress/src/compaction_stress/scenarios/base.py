"""Base scenario and multiprocessing worker."""

from __future__ import annotations

import multiprocessing
import time
from typing import Any

from compaction_stress.config import ClusterConfig, ScenarioConfig


# Shared stats array indices
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
    """Entry point for each scenario producer process.

    Creates its own producers and runs the produce loop until shutdown
    is signalled. Updates shared stats array with cumulative counters.

    When num_workers > 1, the rate limit is split evenly and key prefixes
    include the worker_id to ensure all workers produce the same key space
    (important for compaction — same keys from different producers means
    more data to deduplicate).
    """
    # Reset signal handlers — only the main process should handle signals.
    # Child processes respond to the shared shutdown event instead.
    import signal as _signal
    _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
    _signal.signal(_signal.SIGTERM, _signal.SIG_IGN)

    from compaction_stress.producer import StressProducer, make_producer

    # Split rate evenly across workers, then across topics within this worker
    worker_rate = max(1024, config.rate_limit_bps // num_workers)
    per_topic_rate = max(1024, worker_rate // max(len(topics), 1))
    producers: list[StressProducer] = []

    for topic, prefix in zip(topics, key_prefixes):
        p = make_producer(cluster)
        sp = StressProducer(
            producer=p,
            topic=topic,
            # All workers use the same key prefix so they write to the same
            # key space — this maximizes dedup work for compaction.
            key_prefix=prefix,
            key_count=config.key_count,
            msg_size=config.msg_size,
            rate_limit_bps=per_topic_rate,
            tombstone_probability=config.tombstone_probability,
        )
        producers.append(sp)

    label = f"{name}/w{worker_id}" if num_workers > 1 else name
    print(f"[{label}] Starting produce to {len(topics)} topic(s) "
          f"at {per_topic_rate // (1024*1024)} MB/s per topic", flush=True)

    while not shutdown.is_set():
        for sp in producers:
            sp.produce_batch(batch_size=1000)
        _update_stats(stats, producers)

    print(f"[{label}] Shutting down, flushing producers...", flush=True)
    for sp in producers:
        sp.flush(timeout=5.0)
    _update_stats(stats, producers)


class ScenarioHandle:
    """Main-process handle for reading stats from one or more worker processes."""

    def __init__(
        self,
        name: str,
        num_topics: int,
    ):
        self.name = name
        self.num_topics = num_topics
        self._stats_arrays: list[multiprocessing.Array] = []
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
