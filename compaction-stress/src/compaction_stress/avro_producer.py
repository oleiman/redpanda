"""Avro-serializing producer for iceberg schema-based translation.

Produces records with the Confluent wire format (magic byte + schema ID +
Avro binary), which Redpanda's iceberg translator reads in
value_schema_id_prefix mode.
"""

from __future__ import annotations

import multiprocessing
import os
import random
import signal
import string
import time
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)

from compaction_stress.config import ClusterConfig, ScenarioConfig

# Moderately complex Avro schema — nested record, array, map, various types
STRESS_SCHEMA = """{
  "type": "record",
  "name": "StressRecord",
  "namespace": "com.redpanda.stress",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "timestamp_ms", "type": "long"},
    {"name": "sensor_id", "type": "string"},
    {"name": "temperature", "type": "double"},
    {"name": "humidity", "type": "double"},
    {"name": "pressure", "type": "double"},
    {"name": "location", "type": {
      "type": "record",
      "name": "Location",
      "fields": [
        {"name": "lat", "type": "double"},
        {"name": "lon", "type": "double"},
        {"name": "altitude", "type": "float"}
      ]
    }},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "metadata", "type": {"type": "map", "values": "string"}},
    {"name": "payload", "type": "bytes"}
  ]
}"""

# Shared stats array indices
_RECORDS = 0
_BYTES = 1
_ERRORS = 2
AVRO_STATS_SIZE = 3


def _random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def _generate_record(counter: int, key_count: int) -> dict[str, Any]:
    """Generate a random StressRecord."""
    return {
        "id": counter,
        "timestamp_ms": int(time.time() * 1000),
        "sensor_id": f"sensor-{counter % key_count}",
        "temperature": random.uniform(-40.0, 60.0),
        "humidity": random.uniform(0.0, 100.0),
        "pressure": random.uniform(950.0, 1050.0),
        "location": {
            "lat": random.uniform(-90.0, 90.0),
            "lon": random.uniform(-180.0, 180.0),
            "altitude": random.uniform(0.0, 5000.0),
        },
        "tags": [_random_string(8) for _ in range(random.randint(1, 5))],
        "metadata": {_random_string(6): _random_string(12) for _ in range(random.randint(1, 4))},
        "payload": os.urandom(256),
    }


def avro_worker(
    name: str,
    worker_id: int,
    num_workers: int,
    cluster: ClusterConfig,
    config: ScenarioConfig,
    topic: str,
    shutdown: multiprocessing.Event,
    stats: multiprocessing.Array,
) -> None:
    """Produce Avro-encoded records to a topic.

    Runs in its own process. Uses confluent-kafka AvroSerializer with
    the schema registry for proper wire format encoding.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    # Schema registry client
    sr_conf: dict[str, Any] = {"url": cluster.schema_registry_url}
    if cluster.sasl_user and cluster.sasl_password:
        sr_conf["basic.auth.user.info"] = f"{cluster.sasl_user}:{cluster.sasl_password}"
    sr_client = SchemaRegistryClient(sr_conf)

    avro_serializer = AvroSerializer(sr_client, STRESS_SCHEMA)

    # Kafka producer
    producer_conf: dict[str, Any] = {
        "bootstrap.servers": cluster.brokers,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 256 * 1024,
        "acks": "all",
    }
    if cluster.sasl_mechanism and cluster.sasl_user:
        producer_conf["security.protocol"] = "SASL_SSL" if cluster.tls_enabled else "SASL_PLAINTEXT"
        producer_conf["sasl.mechanism"] = cluster.sasl_mechanism
        producer_conf["sasl.username"] = cluster.sasl_user
        producer_conf["sasl.password"] = cluster.sasl_password or ""
    elif cluster.tls_enabled:
        producer_conf["security.protocol"] = "SSL"

    producer = Producer(producer_conf)

    worker_rate = max(1024, config.rate_limit_bps // num_workers)
    counter = worker_id * 1_000_000_000  # offset so workers don't overlap
    key_count = config.key_count
    records = 0
    total_bytes = 0
    errors = 0

    label = f"{name}/w{worker_id}" if num_workers > 1 else name
    print(f"[{label}] Starting Avro producer to {topic} at "
          f"{worker_rate // (1024*1024)} MB/s", flush=True)

    def delivery_cb(err, msg):
        nonlocal errors
        if err:
            errors += 1

    batch_size = 500  # smaller batches — Avro serialization is heavier
    while not shutdown.is_set():
        batch_start = time.monotonic()
        batch_bytes = 0

        for _ in range(batch_size):
            record = _generate_record(counter, key_count)
            key = f"sensor-{counter % key_count}"
            counter += 1

            ctx = SerializationContext(topic, MessageField.VALUE)
            try:
                value = avro_serializer(record, ctx)
            except Exception:
                errors += 1
                continue

            msg_bytes = len(key) + len(value)
            batch_bytes += msg_bytes

            while True:
                try:
                    producer.produce(
                        topic,
                        key=key.encode(),
                        value=value,
                        callback=delivery_cb,
                    )
                    break
                except BufferError:
                    producer.poll(0.5)
                except Exception:
                    errors += 1
                    break

            records += 1
            total_bytes += msg_bytes

        producer.poll(0)

        # Update shared stats
        stats[_RECORDS] = records
        stats[_BYTES] = total_bytes
        stats[_ERRORS] = errors

        # Batch-level rate limiting
        elapsed = time.monotonic() - batch_start
        if worker_rate > 0 and batch_bytes > 0:
            expected = batch_bytes / worker_rate
            if elapsed < expected:
                time.sleep(expected - elapsed)

    print(f"[{label}] Shutting down, flushing...", flush=True)
    producer.flush(timeout=10.0)
    stats[_RECORDS] = records
    stats[_BYTES] = total_bytes
    stats[_ERRORS] = errors
