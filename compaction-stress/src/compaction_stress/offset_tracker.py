"""Track partition offsets and record counts to detect compaction progress."""

from __future__ import annotations

import threading
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from compaction_stress.config import ClusterConfig


def _make_consumer(cluster: ClusterConfig) -> Consumer:
    conf: dict[str, Any] = {
        "bootstrap.servers": cluster.brokers,
        "group.id": "compaction-stress-tracker",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "log_level": "0",  # suppress librdkafka connection noise
    }
    if cluster.sasl_mechanism and cluster.sasl_user:
        conf["security.protocol"] = (
            "SASL_SSL" if cluster.tls_enabled else "SASL_PLAINTEXT"
        )
        conf["sasl.mechanism"] = cluster.sasl_mechanism
        conf["sasl.username"] = cluster.sasl_user
        conf["sasl.password"] = cluster.sasl_password or ""
    elif cluster.tls_enabled:
        conf["security.protocol"] = "SSL"
    return Consumer(conf)


class OffsetTracker:
    """Periodically counts actual records per topic to measure compaction.

    Uses watermark offsets for the offset range and a consume-and-count
    pass to determine how many records actually remain. The ratio
    (remaining / offset_range) shows compaction effectiveness — a ratio
    approaching (unique_keys / total_produced) means compaction is working.
    """

    def __init__(
        self,
        cluster: ClusterConfig,
        topics: list[str],
        interval: float = 60.0,
    ):
        self._cluster = cluster
        self._topics = topics
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._latest: dict[str, dict[str, Any]] = {}
        self._warn_fn: Any = None

    def set_warn_callback(self, cb: Any) -> None:
        self._warn_fn = cb

    def start(self) -> None:
        if not self._topics:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def signal_stop(self) -> None:
        """Signal the tracker to stop. Non-blocking — the thread is a daemon
        and will be killed on process exit if it's stuck in blocking I/O."""
        self._stop.set()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def get_stats(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._latest)

    def _run(self) -> None:
        # Short delay to let initial data land, then check immediately
        self._stop.wait(5)

        while not self._stop.is_set():
            try:
                import time as _time
                t0 = _time.monotonic()
                stats = self._check_all_topics()
                elapsed = _time.monotonic() - t0
                with self._lock:
                    self._latest = stats
                if self._warn_fn and elapsed > 10:
                    self._warn_fn(
                        f"Offset tracker scan took {elapsed:.1f}s"
                    )
            except Exception as e:
                if self._warn_fn:
                    self._warn_fn(f"Offset tracker error: {e}")
            self._stop.wait(self._interval)

    def _check_all_topics(self) -> dict[str, dict[str, Any]]:
        consumer = _make_consumer(self._cluster)
        try:
            result: dict[str, dict[str, Any]] = {}
            for topic in self._topics:
                result[topic] = self._check_topic(consumer, topic)
            return result
        finally:
            consumer.close()

    def _check_topic(
        self,
        consumer: Consumer,
        topic: str,
    ) -> dict[str, Any]:
        # Get partition count via metadata
        md = consumer.list_topics(topic, timeout=5)
        topic_md = md.topics.get(topic)
        if not topic_md or topic_md.error is not None:
            return {"error": f"topic metadata unavailable"}

        partitions = sorted(topic_md.partitions.keys())
        total_offset_range = 0
        total_records = 0

        for pid in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=10,
            )
            offset_range = high - low
            total_offset_range += offset_range

            if offset_range == 0:
                continue

            # Consume and count actual records in this partition
            tp = TopicPartition(topic, pid, low)
            consumer.assign([tp])

            count = 0
            while True:
                msg = consumer.poll(timeout=2.0)
                if msg is None:
                    break
                if msg.error():
                    break
                count += 1
                if msg.offset() >= high - 1:
                    break

            total_records += count

        consumer.assign([])

        if total_offset_range == 0:
            ratio = 0.0
        else:
            ratio = total_records / total_offset_range

        return {
            "offset_range": total_offset_range,
            "records_remaining": total_records,
            "compaction_ratio": ratio,
            "partitions": len(partitions),
        }
