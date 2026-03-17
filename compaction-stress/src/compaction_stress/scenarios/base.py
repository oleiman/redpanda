"""Base scenario class."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from compaction_stress.config import ClusterConfig, ScenarioConfig
    from compaction_stress.logging import DualLogger
    from compaction_stress.producer import StressProducer


class BaseScenario:
    name: str = "base"

    def __init__(
        self,
        cluster: ClusterConfig,
        config: ScenarioConfig,
        topics: list[str],
        logger: DualLogger,
    ):
        self.cluster = cluster
        self.config = config
        self.topics = topics
        self.logger = logger
        self.producers: list[StressProducer] = []

    def run(self, shutdown: threading.Event) -> None:
        """Main produce loop. Override for custom behavior."""
        from compaction_stress.producer import StressProducer, make_producer

        for topic in self.topics:
            p = make_producer(self.cluster)
            sp = StressProducer(
                producer=p,
                topic=topic,
                key_prefix=self._key_prefix(topic),
                key_count=self.config.key_count,
                msg_size=self.config.msg_size,
                rate_limit_bps=self._per_topic_rate(),
                tombstone_probability=self.config.tombstone_probability,
            )
            self.producers.append(sp)

        self.logger.info(f"[{self.name}] Starting produce to {len(self.topics)} topic(s)")

        while not shutdown.is_set():
            for sp in self.producers:
                sp.produce_batch(batch_size=1000)

        self.logger.info(f"[{self.name}] Shutting down, flushing producers...")
        for sp in self.producers:
            sp.flush()

    def get_stats(self) -> dict[str, Any]:
        combined: dict[str, Any] = {
            "records": 0,
            "bytes_per_sec": 0.0,
            "errors": 0,
            "tombstones": 0,
            "num_topics": len(self.topics),
        }
        for sp in self.producers:
            stats = sp.get_stats()
            combined["records"] += stats["records"]
            combined["bytes_per_sec"] += stats["bytes_per_sec"]
            combined["errors"] += stats["errors"]
            combined["tombstones"] += stats["tombstones"]
        return combined

    def _key_prefix(self, topic: str) -> str:
        return self.name[:2]

    def _per_topic_rate(self) -> int:
        n = len(self.topics) if self.topics else 1
        return max(1024, self.config.rate_limit_bps // n)
