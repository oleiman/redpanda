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
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topics: list[str],
    key_prefixes: list[str],
    shutdown: multiprocessing.Event,
    stats: multiprocessing.Array,
) -> None:
    """Entry point for each scenario process.

    Creates its own producers and runs the produce loop until shutdown
    is signalled. Updates shared stats array with cumulative counters.
    """
    # Reset signal handlers — only the main process should handle signals.
    # Child processes respond to the shared shutdown event instead.
    import signal as _signal
    _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
    _signal.signal(_signal.SIGTERM, _signal.SIG_IGN)

    from compaction_stress.producer import StressProducer, make_producer

    per_topic_rate = max(1024, config.rate_limit_bps // max(len(topics), 1))
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

    print(f"[{name}] Starting produce to {len(topics)} topic(s)", flush=True)

    while not shutdown.is_set():
        for sp in producers:
            sp.produce_batch(batch_size=1000)
        _update_stats(stats, producers)

    print(f"[{name}] Shutting down, flushing producers...", flush=True)
    for sp in producers:
        sp.flush(timeout=5.0)
    _update_stats(stats, producers)


class ScenarioHandle:
    """Main-process handle for reading stats from a running scenario process."""

    def __init__(
        self,
        name: str,
        num_topics: int,
        stats: multiprocessing.Array,
    ):
        self.name = name
        self.num_topics = num_topics
        self._stats = stats
        self._prev_bytes = 0.0
        self._prev_time = time.monotonic()

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        total_bytes = self._stats[_BYTES]
        elapsed = now - self._prev_time
        bps = (total_bytes - self._prev_bytes) / elapsed if elapsed > 0 else 0.0
        self._prev_bytes = total_bytes
        self._prev_time = now

        return {
            "records": int(self._stats[_RECORDS]),
            "bytes_per_sec": bps,
            "errors": int(self._stats[_ERRORS]),
            "tombstones": int(self._stats[_TOMBSTONES]),
            "num_topics": self.num_topics,
        }
