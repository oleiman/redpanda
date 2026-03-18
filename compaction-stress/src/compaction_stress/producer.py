"""Rate-limited Kafka producer with key distribution control."""

from __future__ import annotations

import os
import random
import time
from typing import Any

from confluent_kafka import KafkaError, Producer

from compaction_stress.config import ClusterConfig


def make_producer(cluster: ClusterConfig, extra_conf: dict[str, Any] | None = None) -> Producer:
    conf: dict[str, Any] = {
        "bootstrap.servers": cluster.brokers,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 256 * 1024,
        "acks": "all",
    }
    if cluster.sasl_mechanism and cluster.sasl_user:
        conf["security.protocol"] = "SASL_SSL" if cluster.tls_enabled else "SASL_PLAINTEXT"
        conf["sasl.mechanism"] = cluster.sasl_mechanism
        conf["sasl.username"] = cluster.sasl_user
        conf["sasl.password"] = cluster.sasl_password or ""
    elif cluster.tls_enabled:
        conf["security.protocol"] = "SSL"
    if extra_conf:
        conf.update(extra_conf)
    return Producer(conf)


class StressProducer:
    """Rate-limited producer with key cycling and tombstone injection.

    Designed to run in its own process (no locks). Rate limiting is done
    at the batch level: produce a full batch, then sleep to match the
    target throughput.
    """

    def __init__(
        self,
        producer: Producer,
        topic: str,
        key_prefix: str,
        key_count: int,
        msg_size: int,
        rate_limit_bps: int,
        tombstone_probability: float = 0.0,
    ):
        self._producer = producer
        self._topic = topic
        self._key_prefix = key_prefix
        self._key_count = key_count
        self._msg_size = msg_size
        self._rate_limit_bps = rate_limit_bps
        self._tombstone_prob = tombstone_probability

        self._value = os.urandom(msg_size)
        self._counter = 0

        # Stats — no lock needed, single process writes, main process
        # reads via shared memory (slightly stale is fine for monitoring).
        self.records = 0
        self.total_bytes = 0
        self.errors = 0
        self.tombstones = 0

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        if err:
            self.errors += 1

    def _next_key(self) -> bytes:
        key = f"{self._key_prefix}-{self._counter % self._key_count}"
        self._counter += 1
        return key.encode()

    def _should_tombstone(self) -> bool:
        return self._tombstone_prob > 0 and random.random() < self._tombstone_prob

    def produce_batch(self, batch_size: int = 1000) -> None:
        """Produce a batch of messages with batch-level rate limiting."""
        batch_start = time.monotonic()
        batch_bytes = 0

        for _ in range(batch_size):
            key = self._next_key()
            is_tombstone = self._should_tombstone()
            value = None if is_tombstone else self._value
            msg_bytes = len(key) + (0 if is_tombstone else self._msg_size)
            batch_bytes += msg_bytes

            while True:
                try:
                    self._producer.produce(
                        self._topic,
                        key=key,
                        value=value,
                        callback=self._delivery_callback,
                    )
                    break
                except BufferError:
                    self._producer.poll(0.5)
                except Exception:
                    self.errors += 1
                    break

            self.records += 1
            self.total_bytes += msg_bytes
            if is_tombstone:
                self.tombstones += 1

        self._producer.poll(0)

        # Batch-level rate limiting: sleep to match target throughput
        elapsed = time.monotonic() - batch_start
        expected = batch_bytes / self._rate_limit_bps
        if elapsed < expected:
            time.sleep(expected - elapsed)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)
