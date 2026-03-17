"""Rate-limited Kafka producer with key distribution control."""

from __future__ import annotations

import os
import random
import threading
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
    """Rate-limited producer with key cycling and tombstone injection."""

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

        self._lock = threading.Lock()
        self._records = 0
        self._bytes = 0
        self._errors = 0
        self._tombstones = 0
        self._last_report_records = 0
        self._last_report_bytes = 0
        self._last_report_time = time.monotonic()

        self._bucket_tokens = float(rate_limit_bps)
        self._bucket_last = time.monotonic()

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        if err:
            with self._lock:
                self._errors += 1

    def _next_key(self) -> bytes:
        key = f"{self._key_prefix}-{self._counter % self._key_count}"
        self._counter += 1
        return key.encode()

    def _should_tombstone(self) -> bool:
        return self._tombstone_prob > 0 and random.random() < self._tombstone_prob

    def _wait_for_rate_limit(self, msg_bytes: int) -> None:
        now = time.monotonic()
        elapsed = now - self._bucket_last
        self._bucket_tokens += elapsed * self._rate_limit_bps
        if self._bucket_tokens > self._rate_limit_bps:
            self._bucket_tokens = float(self._rate_limit_bps)
        self._bucket_last = now

        self._bucket_tokens -= msg_bytes
        if self._bucket_tokens < 0:
            sleep_time = -self._bucket_tokens / self._rate_limit_bps
            time.sleep(sleep_time)
            self._bucket_tokens = 0
            self._bucket_last = time.monotonic()

    def produce_batch(self, batch_size: int = 1000) -> None:
        """Produce a batch of messages, respecting rate limits."""
        for _ in range(batch_size):
            key = self._next_key()
            is_tombstone = self._should_tombstone()
            value = None if is_tombstone else self._value
            msg_bytes = len(key) + (0 if is_tombstone else self._msg_size)

            self._wait_for_rate_limit(msg_bytes)

            try:
                self._producer.produce(
                    self._topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
            except BufferError:
                self._producer.poll(0.1)
                self._producer.produce(
                    self._topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )

            with self._lock:
                self._records += 1
                self._bytes += msg_bytes
                if is_tombstone:
                    self._tombstones += 1

        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)

    def get_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            elapsed = now - self._last_report_time
            bytes_delta = self._bytes - self._last_report_bytes
            bps = bytes_delta / elapsed if elapsed > 0 else 0
            stats = {
                "records": self._records,
                "bytes_per_sec": bps,
                "errors": self._errors,
                "tombstones": self._tombstones,
            }
            self._last_report_records = self._records
            self._last_report_bytes = self._bytes
            self._last_report_time = now
            return stats
