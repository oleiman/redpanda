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
        "linger.ms": 100,
        "batch.num.messages": 50000,
        "queue.buffering.max.messages": 1000000,
        "queue.buffering.max.kbytes": 512 * 1024,
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
    """High-throughput producer with key cycling and tombstone injection.

    Optimized for maximum produce rate per process:
    - All keys pre-encoded at init (no per-message string formatting)
    - Tombstone positions pre-generated per batch (no per-message random())
    - Large batches (10K default) with single rate-limit sleep at end
    - Delivery callback only counts errors (common path is no-op)
    - Stats updated once per batch, not per message
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
        self._key_count = key_count
        self._msg_size = msg_size
        self._rate_limit_bps = rate_limit_bps
        self._tombstone_prob = tombstone_probability

        # Pre-encode all keys once at startup
        self._keys = [f"{key_prefix}-{i}".encode() for i in range(key_count)]
        self._value = os.urandom(msg_size)
        self._counter = 0

        # Pre-compute average key length for byte accounting
        self._avg_key_len = sum(len(k) for k in self._keys) / max(len(self._keys), 1)

        # Stats — no lock needed, single process owns this object.
        self.records = 0
        self.total_bytes = 0
        self.errors = 0
        self.tombstones = 0

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        if err:
            self.errors += 1

    def produce_batch(self, batch_size: int = 10000) -> None:
        """Produce a batch of messages with batch-level rate limiting."""
        batch_start = time.monotonic()
        produce = self._producer.produce
        topic = self._topic
        value = self._value
        keys = self._keys
        key_count = self._key_count
        callback = self._delivery_callback
        counter = self._counter
        tombstone_count = 0

        # Pre-generate tombstone indices for this batch
        tombstone_set: set[int] | None = None
        if self._tombstone_prob > 0:
            n_tombstones = int(batch_size * self._tombstone_prob)
            if n_tombstones > 0:
                tombstone_set = set(random.sample(range(batch_size), n_tombstones))
                tombstone_count = n_tombstones

        if tombstone_set:
            for i in range(batch_size):
                key = keys[counter % key_count]
                counter += 1
                if i in tombstone_set:
                    val = None
                else:
                    val = value
                while True:
                    try:
                        produce(topic, key=key, value=val, callback=callback)
                        break
                    except BufferError:
                        self._producer.poll(0.5)
                    except Exception:
                        self.errors += 1
                        break
        else:
            # Fast path — no tombstones, tightest possible loop
            for _ in range(batch_size):
                key = keys[counter % key_count]
                counter += 1
                while True:
                    try:
                        produce(topic, key=key, value=value, callback=callback)
                        break
                    except BufferError:
                        self._producer.poll(0.5)
                    except Exception:
                        self.errors += 1
                        break

        self._counter = counter

        # Batch stats update (once, not per-message)
        non_tombstone = batch_size - tombstone_count
        batch_bytes = int(
            non_tombstone * (self._avg_key_len + self._msg_size)
            + tombstone_count * self._avg_key_len
        )
        self.records += batch_size
        self.total_bytes += batch_bytes
        self.tombstones += tombstone_count

        self._producer.poll(0)

        # Batch-level rate limiting
        elapsed = time.monotonic() - batch_start
        expected = batch_bytes / self._rate_limit_bps
        if elapsed < expected:
            time.sleep(expected - elapsed)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)
