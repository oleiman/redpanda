# Compaction Stress Tool — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:executing-plans to implement this plan task-by-task.

**Goal:** Build a self-contained Python CLI tool that generates sustained, tunable Kafka workloads to pressure cloud topics compaction on a live Redpanda cluster.

**Architecture:** A Python package (`compaction-stress/`) with `confluent-kafka` for producing, `rpk` subprocess calls for cluster/topic setup, optional Prometheus metrics scraping via `requests`, and a scenario-based runner that executes workloads in concurrent threads. Config via YAML file + CLI overrides.

**Tech Stack:** Python 3.11+, confluent-kafka, PyYAML, requests, argparse

**Design doc:** `docs/plans/2026-03-17-compaction-stress-tool-design.md`

---

## Task 0: Scaffold project and packaging

**Files:**
- Create: `compaction-stress/pyproject.toml`
- Create: `compaction-stress/src/compaction_stress/__init__.py`

**Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "compaction-stress"
version = "0.1.0"
description = "Cloud topics compaction stress workload generator for Redpanda"
requires-python = ">=3.11"
dependencies = [
    "confluent-kafka>=2.3.0",
    "pyyaml>=6.0",
    "requests>=2.31.0",
]

[project.scripts]
compaction-stress = "compaction_stress.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

**Step 2: Create __init__.py**

```python
"""Cloud topics compaction stress workload generator."""
```

**Step 3: Verify packaging**

Run: `cd compaction-stress && python3 -m venv .venv && source .venv/bin/activate && pip install -e . 2>&1 | tail -5`
Expected: `Successfully installed compaction-stress-0.1.0` (or similar)

**Step 4: Commit**

```bash
git add compaction-stress/pyproject.toml compaction-stress/src/compaction_stress/__init__.py
git commit -m "compaction-stress: scaffold project and packaging"
```

---

## Task 1: Config dataclasses and YAML loading

**Files:**
- Create: `compaction-stress/src/compaction_stress/config.py`
- Create: `compaction-stress/config.example.yaml`

**Step 1: Write config.py**

Define dataclasses for all configuration. Each scenario config has a common
base (key_count, msg_size, rate_limit_bps, partitions, replicas, topic_config)
plus scenario-specific fields (tombstone_probability, num_topics). A top-level
`Config` dataclass holds cluster connection info, cluster_config overrides,
scenario configs, and defaults.

```python
"""Configuration dataclasses and YAML loading."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ClusterConfig:
    brokers: str = "localhost:9092"
    sasl_mechanism: str | None = None
    sasl_user: str | None = None
    sasl_password: str | None = None
    tls_enabled: bool = False
    admin_hosts: list[str] = field(default_factory=list)


@dataclass
class ScenarioConfig:
    enabled: bool = True
    key_count: int = 10000
    msg_size: int = 512
    rate_limit_bps: int = 10 * 1024 * 1024  # 10 MB/s
    partitions: int = 1
    replicas: int = 3
    num_topics: int = 1
    tombstone_probability: float = 0.0
    topic_config: dict[str, str] = field(default_factory=dict)


# Built-in defaults per scenario (merged on top of global defaults)
SCENARIO_DEFAULTS: dict[str, dict[str, Any]] = {
    "key_cardinality": {
        "key_count": 200_000,
        "msg_size": 512,
        "rate_limit_bps": 10 * 1024 * 1024,
        "partitions": 1,
        "topic_config": {"min.cleanable.dirty.ratio": "0.0"},
    },
    "extreme_dedup": {
        "key_count": 10,
        "msg_size": 256,
        "rate_limit_bps": 5 * 1024 * 1024,
        "partitions": 1,
    },
    "continuous_write": {
        "key_count": 50_000,
        "msg_size": 512,
        "rate_limit_bps": 10 * 1024 * 1024,
        "partitions": 4,
        "topic_config": {"min.compaction.lag.ms": "30000"},
    },
    "tombstone": {
        "key_count": 10_000,
        "msg_size": 256,
        "tombstone_probability": 0.15,
        "rate_limit_bps": 5 * 1024 * 1024,
        "partitions": 2,
        "topic_config": {"delete.retention.ms": "60000"},
    },
    "multi_partition": {
        "key_count": 20_000,
        "msg_size": 512,
        "rate_limit_bps": 20 * 1024 * 1024,
        "num_topics": 4,
        "partitions": 8,
    },
}

ALL_SCENARIOS = list(SCENARIO_DEFAULTS.keys())


@dataclass
class Config:
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    cluster_config: dict[str, Any] = field(default_factory=lambda: {
        "cloud_topics_compaction_interval_ms": 5000,
        "cloud_topics_compaction_max_object_size": 128 * 1024 * 1024,
        "cloud_topics_compaction_key_map_memory": 128 * 1024 * 1024,
    })
    scenarios: dict[str, ScenarioConfig] = field(default_factory=dict)
    duration: str | None = "1h"  # None = indefinite
    no_setup: bool = False
    log_dir: str = "./logs"
    report_interval: int = 30  # seconds between stdout reports

    def get_scenario(self, name: str) -> ScenarioConfig:
        if name in self.scenarios:
            return self.scenarios[name]
        # Build from defaults
        return _build_scenario_config(name, {})

    def enabled_scenarios(self, selected: str | None) -> list[str]:
        if selected and selected != "kitchen_sink":
            return [selected]
        return [
            name for name in ALL_SCENARIOS
            if self.get_scenario(name).enabled
        ]


def _build_scenario_config(
    name: str,
    overrides: dict[str, Any],
) -> ScenarioConfig:
    merged: dict[str, Any] = {}
    if name in SCENARIO_DEFAULTS:
        merged.update(SCENARIO_DEFAULTS[name])
    merged.update(overrides)
    # Separate topic_config from ScenarioConfig fields
    topic_config = merged.pop("topic_config", {})
    sc = ScenarioConfig(**{
        k: v for k, v in merged.items()
        if k in ScenarioConfig.__dataclass_fields__
    })
    sc.topic_config = {
        **{"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.01"},
        **topic_config,
    }
    return sc


def load_config(
    config_path: str | None,
    cli_overrides: dict[str, Any],
) -> Config:
    raw: dict[str, Any] = {}
    if config_path:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    # Build ClusterConfig
    cluster_raw = raw.get("cluster", {})
    # CLI/env overrides for brokers
    if cli_overrides.get("brokers"):
        cluster_raw["brokers"] = cli_overrides["brokers"]
    elif os.environ.get("REDPANDA_BROKERS"):
        cluster_raw["brokers"] = os.environ["REDPANDA_BROKERS"]
    if cli_overrides.get("admin_hosts"):
        cluster_raw["admin_hosts"] = cli_overrides["admin_hosts"]

    # Env overrides for SASL
    if os.environ.get("REDPANDA_SASL_USER"):
        cluster_raw["sasl_user"] = os.environ["REDPANDA_SASL_USER"]
    if os.environ.get("REDPANDA_SASL_PASSWORD"):
        cluster_raw["sasl_password"] = os.environ["REDPANDA_SASL_PASSWORD"]
    if os.environ.get("REDPANDA_SASL_MECHANISM"):
        cluster_raw["sasl_mechanism"] = os.environ["REDPANDA_SASL_MECHANISM"]

    cluster = ClusterConfig(**{
        k: v for k, v in cluster_raw.items()
        if k in ClusterConfig.__dataclass_fields__
    })

    # Build scenario configs
    scenarios_raw = raw.get("scenarios", {})
    defaults_raw = raw.get("defaults", {})
    scenarios: dict[str, ScenarioConfig] = {}
    for name in ALL_SCENARIOS:
        scenario_overrides = {**defaults_raw, **scenarios_raw.get(name, {})}
        scenarios[name] = _build_scenario_config(name, scenario_overrides)

    # Build Config
    config = Config(
        cluster=cluster,
        cluster_config=raw.get("cluster_config", Config.cluster_config),
        scenarios=scenarios,
        duration=cli_overrides.get("duration", raw.get("duration", "1h")),
        no_setup=cli_overrides.get("no_setup", False),
        log_dir=cli_overrides.get("log_dir", raw.get("log_dir", "./logs")),
        report_interval=raw.get("report_interval", 30),
    )

    return config


def parse_duration(duration_str: str) -> float | None:
    """Parse duration string like '1h', '30m', '2h30m', '45s' to seconds.
    Returns None for 'indefinite' or empty string."""
    if not duration_str or duration_str.lower() == "indefinite":
        return None
    total = 0.0
    current = ""
    for ch in duration_str:
        if ch.isdigit() or ch == ".":
            current += ch
        elif ch == "h":
            total += float(current) * 3600
            current = ""
        elif ch == "m":
            total += float(current) * 60
            current = ""
        elif ch == "s":
            total += float(current)
            current = ""
    if current:
        total += float(current)
    return total if total > 0 else None
```

**Step 2: Write config.example.yaml**

Copy the config YAML from the design doc into `compaction-stress/config.example.yaml`.
This is the reference config file users copy and edit.

```yaml
# Compaction Stress Tool — Example Configuration
# Copy to config.yaml and edit for your cluster.

cluster:
  brokers: "seed1:9092,seed2:9092,seed3:9092"
  # sasl_mechanism: "SCRAM-SHA-256"
  # sasl_user: "admin"
  # sasl_password: "secret"
  # tls_enabled: false
  admin_hosts:
    - "node1:9644"
    - "node2:9644"
    - "node3:9644"
    - "node4:9644"
    - "node5:9644"
    - "node6:9644"

# Cluster-level configs set via rpk at startup (skipped with --no-setup)
cluster_config:
  cloud_topics_compaction_interval_ms: 5000
  cloud_topics_compaction_max_object_size: 134217728   # 128 MB
  cloud_topics_compaction_key_map_memory: 134217728    # 128 MB

scenarios:
  key_cardinality:
    enabled: true
    key_count: 200000
    msg_size: 512
    rate_limit_bps: 10485760       # 10 MB/s
    partitions: 1
    topic_config:
      "min.cleanable.dirty.ratio": "0.0"

  extreme_dedup:
    enabled: true
    key_count: 10
    msg_size: 256
    rate_limit_bps: 5242880        # 5 MB/s
    partitions: 1

  continuous_write:
    enabled: true
    key_count: 50000
    msg_size: 512
    rate_limit_bps: 10485760       # 10 MB/s
    partitions: 4
    topic_config:
      "min.compaction.lag.ms": "30000"

  tombstone:
    enabled: true
    key_count: 10000
    msg_size: 256
    tombstone_probability: 0.15
    rate_limit_bps: 5242880        # 5 MB/s
    partitions: 2
    topic_config:
      "delete.retention.ms": "60000"

  multi_partition:
    enabled: true
    key_count: 20000
    msg_size: 512
    rate_limit_bps: 20971520       # 20 MB/s
    num_topics: 4
    partitions: 8

# Duration: "1h", "30m", "12h", or "indefinite"
duration: "1h"

log_dir: "./logs"
report_interval: 30
```

**Step 3: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.config import load_config, parse_duration; print('OK'); print(parse_duration('2h30m'))"`
Expected: `OK` then `9000.0`

**Step 4: Commit**

```bash
git add compaction-stress/src/compaction_stress/config.py compaction-stress/config.example.yaml
git commit -m "compaction-stress: add config dataclasses and YAML loading"
```

---

## Task 2: Dual logger (stdout + JSON file)

**Files:**
- Create: `compaction-stress/src/compaction_stress/logging.py`

**Step 1: Write logging.py**

Provides two things:
1. A `DualLogger` class that writes human-readable summaries to stdout and
   JSON lines to a file.
2. Helper functions for formatting numbers (e.g., `4200000` → `4.2M`).

```python
"""Dual logger: human-readable stdout + JSON lines file."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def format_count(n: int | float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def format_bytes_per_sec(bps: float) -> str:
    if bps >= 1024 * 1024:
        return f"{bps / (1024 * 1024):.1f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


def format_elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class DualLogger:
    def __init__(self, log_dir: str):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._json_path = self._log_dir / f"compaction-stress-{ts}.jsonl"
        self._json_file = open(self._json_path, "a")
        self.info(f"JSON log: {self._json_path}")

    def info(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)

    def warn(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] WARN: {msg}", file=sys.stderr, flush=True)

    def error(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] ERROR: {msg}", file=sys.stderr, flush=True)

    def json_event(self, event: dict[str, Any]) -> None:
        event["ts"] = datetime.now(timezone.utc).isoformat()
        self._json_file.write(json.dumps(event) + "\n")
        self._json_file.flush()

    def report(
        self,
        elapsed: float,
        scenario_stats: dict[str, dict[str, Any]],
        cluster_metrics: dict[str, Any] | None,
    ) -> None:
        header = f"── {format_elapsed(elapsed)} elapsed "
        print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {header:─<66}")

        for name, stats in sorted(scenario_stats.items()):
            records = format_count(stats.get("records", 0))
            bps = format_bytes_per_sec(stats.get("bytes_per_sec", 0))
            errors = stats.get("errors", 0)
            tombstones = stats.get("tombstones", 0)
            extra = ""
            if tombstones:
                extra = f" │ {format_count(tombstones)} tombstones"
            if stats.get("num_topics", 1) > 1:
                extra += f" ({stats['num_topics']} topics)"
            print(f"  {name:<20s}: {records:>8s} records │ {bps:>10s} │ {errors} errors{extra}")

        if cluster_metrics:
            print("  ── cluster compaction ──")
            rounds = format_count(cluster_metrics.get("compaction_rounds", 0))
            queue = cluster_metrics.get("queue_depth", 0)
            removed = format_count(cluster_metrics.get("records_removed", 0))
            tombs = format_count(cluster_metrics.get("tombstones_removed", 0))
            print(f"  compaction rounds: {rounds} │ queue depth: {queue} │ records removed: {removed} │ tombstones removed: {tombs}")

        print(flush=True)

        # Also write to JSON
        self.json_event({
            "type": "report",
            "elapsed": elapsed,
            "scenarios": scenario_stats,
            "cluster_metrics": cluster_metrics,
        })

    def close(self) -> None:
        self._json_file.close()
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.logging import format_count, format_elapsed, parse_duration; print(format_count(4200000), format_elapsed(8100)); assert format_count(4200000) == '4.2M'"`

Note: `parse_duration` is in config.py, not logging.py. The verify command should be:

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.logging import format_count, format_elapsed; print(format_count(4200000), format_elapsed(8100))"`
Expected: `4.2M 2h15m`

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/logging.py
git commit -m "compaction-stress: add dual logger (stdout + JSON file)"
```

---

## Task 3: Producer wrapper with rate limiting

**Files:**
- Create: `compaction-stress/src/compaction_stress/producer.py`

**Step 1: Write producer.py**

Wraps `confluent-kafka.Producer` with:
- Key generation (cycling keys with a configurable prefix and count)
- Value generation (random bytes, reused per batch)
- Token-bucket rate limiting
- Tombstone injection (null values with configurable probability)
- Stats tracking (records produced, bytes, errors, tombstones)

```python
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

        # Pre-generate a value payload (reused, not per-record)
        self._value = os.urandom(msg_size)
        self._counter = 0

        # Stats (accessed from reporting thread, use lock)
        self._lock = threading.Lock()
        self._records = 0
        self._bytes = 0
        self._errors = 0
        self._tombstones = 0
        self._last_report_records = 0
        self._last_report_bytes = 0
        self._last_report_time = time.monotonic()

        # Rate limiter state
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
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.producer import StressProducer, make_producer; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/producer.py
git commit -m "compaction-stress: add rate-limited producer with key cycling"
```

---

## Task 4: Metrics scraper

**Files:**
- Create: `compaction-stress/src/compaction_stress/metrics.py`

**Step 1: Write metrics.py**

Background thread that polls Redpanda `/metrics` endpoints and parses
Prometheus text format. Extracts compaction-related counters/gauges.
Degrades gracefully if nodes are unreachable.

```python
"""Optional Prometheus metrics scraper for Redpanda compaction stats."""

from __future__ import annotations

import re
import threading
import time
from typing import Any

import requests

# Metrics we care about (summed across all nodes)
METRICS_OF_INTEREST = [
    "vectorized_cloud_topics_compaction_scheduler_log_compactions",
    "vectorized_cloud_topics_compaction_scheduler_compaction_queue_length",
    "vectorized_cloud_topics_compaction_worker_records_removed",
    "vectorized_cloud_topics_compaction_worker_tombstones_removed",
    "vectorized_cloud_topics_compaction_worker_compaction_duration_seconds",
]

# Maps metric name to a friendlier key
METRIC_KEY_MAP = {
    "vectorized_cloud_topics_compaction_scheduler_log_compactions": "compaction_rounds",
    "vectorized_cloud_topics_compaction_scheduler_compaction_queue_length": "queue_depth",
    "vectorized_cloud_topics_compaction_worker_records_removed": "records_removed",
    "vectorized_cloud_topics_compaction_worker_tombstones_removed": "tombstones_removed",
    "vectorized_cloud_topics_compaction_worker_compaction_duration_seconds": "compaction_duration_s",
}

_METRIC_LINE_RE = re.compile(
    r'^(\w+)(?:\{[^}]*\})?\s+([\d.eE+\-]+)',
)


def _parse_metrics(text: str) -> dict[str, float]:
    """Parse Prometheus text format, return metric_name -> sum of values."""
    result: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue
        name, val_str = m.group(1), m.group(2)
        if name in METRICS_OF_INTEREST:
            result[name] = result.get(name, 0.0) + float(val_str)
    return result


class MetricsScraper:
    """Background thread that periodically scrapes Redpanda metrics."""

    def __init__(
        self,
        admin_hosts: list[str],
        interval: float = 10.0,
        timeout: float = 5.0,
    ):
        self._hosts = admin_hosts
        self._interval = interval
        self._timeout = timeout
        self._lock = threading.Lock()
        self._latest: dict[str, float] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._warn_callback: Any = None

    def set_warn_callback(self, cb: Any) -> None:
        self._warn_callback = cb

    def start(self) -> None:
        if not self._hosts:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def get_metrics(self) -> dict[str, Any] | None:
        if not self._hosts:
            return None
        with self._lock:
            if not self._latest:
                return None
            return {
                METRIC_KEY_MAP.get(k, k): v
                for k, v in self._latest.items()
            }

    def _run(self) -> None:
        while not self._stop.is_set():
            aggregated: dict[str, float] = {}
            for host in self._hosts:
                try:
                    url = f"http://{host}/metrics"
                    resp = requests.get(url, timeout=self._timeout)
                    resp.raise_for_status()
                    node_metrics = _parse_metrics(resp.text)
                    for k, v in node_metrics.items():
                        aggregated[k] = aggregated.get(k, 0.0) + v
                except Exception as e:
                    if self._warn_callback:
                        self._warn_callback(f"Failed to scrape {host}: {e}")
            with self._lock:
                self._latest = aggregated
            self._stop.wait(self._interval)
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.metrics import MetricsScraper, _parse_metrics; r = _parse_metrics('vectorized_cloud_topics_compaction_scheduler_log_compactions{shard=\"0\"} 42\n'); print(r)"`
Expected: dict with the metric name mapped to `42.0`

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/metrics.py
git commit -m "compaction-stress: add Prometheus metrics scraper"
```

---

## Task 5: rpk-based setup module

**Files:**
- Create: `compaction-stress/src/compaction_stress/setup.py`

**Step 1: Write setup.py**

Shells out to `rpk` for cluster config and topic creation. Builds rpk
command lines from the cluster config (brokers, SASL flags). Idempotent:
existing topics log a warning and continue.

```python
"""rpk-based cluster and topic setup."""

from __future__ import annotations

import subprocess
from typing import Any

from compaction_stress.config import ClusterConfig, Config, ScenarioConfig


def _rpk_base_args(cluster: ClusterConfig) -> list[str]:
    args = ["rpk", "--brokers", cluster.brokers]
    if cluster.sasl_mechanism and cluster.sasl_user:
        args += [
            "--sasl-mechanism", cluster.sasl_mechanism,
            "--user", cluster.sasl_user,
            "--password", cluster.sasl_password or "",
        ]
    if cluster.tls_enabled:
        args += ["--tls-enabled"]
    return args


def _run_rpk(args: list[str], warn_fn: Any = None) -> bool:
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            if "already exists" in stderr.lower() or "TOPIC_ALREADY_EXISTS" in stderr:
                if warn_fn:
                    warn_fn(f"Topic already exists (continuing): {stderr}")
                return True
            if warn_fn:
                warn_fn(f"rpk command failed: {' '.join(args)}\n{stderr}")
            return False
        return True
    except FileNotFoundError:
        if warn_fn:
            warn_fn("rpk not found on PATH — skipping setup")
        return False
    except subprocess.TimeoutExpired:
        if warn_fn:
            warn_fn(f"rpk command timed out: {' '.join(args)}")
        return False


def set_cluster_configs(
    cluster: ClusterConfig,
    configs: dict[str, Any],
    log_fn: Any = None,
    warn_fn: Any = None,
) -> None:
    base = _rpk_base_args(cluster)
    for key, value in configs.items():
        if log_fn:
            log_fn(f"Setting cluster config: {key}={value}")
        args = base + ["cluster", "config", "set", key, str(value)]
        _run_rpk(args, warn_fn=warn_fn)


def topic_name(scenario: str, index: int = 0) -> str:
    return f"ct-stress-{scenario}-{index}"


def create_topics(
    cluster: ClusterConfig,
    scenario_name: str,
    scenario_config: ScenarioConfig,
    log_fn: Any = None,
    warn_fn: Any = None,
) -> list[str]:
    base = _rpk_base_args(cluster)
    topics: list[str] = []
    num_topics = scenario_config.num_topics

    for i in range(num_topics):
        name = topic_name(scenario_name, i)
        topics.append(name)

        args = base + [
            "topic", "create", name,
            "--partitions", str(scenario_config.partitions),
            "--replicas", str(scenario_config.replicas),
        ]
        # Add topic configs
        all_topic_config = {
            "cleanup.policy": "compact",
            **scenario_config.topic_config,
        }
        for k, v in all_topic_config.items():
            args += ["-c", f"{k}={v}"]

        if log_fn:
            log_fn(f"Creating topic: {name} (partitions={scenario_config.partitions}, replicas={scenario_config.replicas})")
        _run_rpk(args, warn_fn=warn_fn)

    return topics


def run_setup(
    config: Config,
    enabled_scenarios: list[str],
    log_fn: Any = None,
    warn_fn: Any = None,
) -> dict[str, list[str]]:
    """Run full setup: cluster configs + topics. Returns scenario -> topic names."""
    if config.no_setup:
        if log_fn:
            log_fn("Skipping setup (--no-setup)")
        # Return expected topic names even without creating them
        result = {}
        for name in enabled_scenarios:
            sc = config.get_scenario(name)
            result[name] = [topic_name(name, i) for i in range(sc.num_topics)]
        return result

    set_cluster_configs(
        config.cluster, config.cluster_config,
        log_fn=log_fn, warn_fn=warn_fn,
    )

    result: dict[str, list[str]] = {}
    for name in enabled_scenarios:
        sc = config.get_scenario(name)
        result[name] = create_topics(
            config.cluster, name, sc,
            log_fn=log_fn, warn_fn=warn_fn,
        )
    return result
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.setup import topic_name; print(topic_name('continuous_write', 0))"`
Expected: `ct-stress-continuous_write-0`

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/setup.py
git commit -m "compaction-stress: add rpk-based setup module"
```

---

## Task 6: Scenario base class and all scenarios

**Files:**
- Create: `compaction-stress/src/compaction_stress/scenarios/__init__.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/base.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/key_cardinality.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/extreme_dedup.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/continuous_write.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/tombstone.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/multi_partition.py`
- Create: `compaction-stress/src/compaction_stress/scenarios/kitchen_sink.py`

**Step 1: Write base.py**

```python
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

        # Create a producer per topic
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
```

**Step 2: Write each scenario file**

Each scenario inherits from `BaseScenario` and only overrides what's different.
Most just set `name` and `_key_prefix`. The kitchen_sink scenario is different —
it composes and runs others.

`key_cardinality.py`:
```python
"""Key cardinality overflow scenario."""

from compaction_stress.scenarios.base import BaseScenario


class KeyCardinalityScenario(BaseScenario):
    name = "key_cardinality"

    def _key_prefix(self, topic: str) -> str:
        return "kc"
```

`extreme_dedup.py`:
```python
"""Extreme dedup ratio scenario."""

from compaction_stress.scenarios.base import BaseScenario


class ExtremeDedupScenario(BaseScenario):
    name = "extreme_dedup"

    def _key_prefix(self, topic: str) -> str:
        return "ed"
```

`continuous_write.py`:
```python
"""Continuous write pressure scenario."""

from compaction_stress.scenarios.base import BaseScenario


class ContinuousWriteScenario(BaseScenario):
    name = "continuous_write"

    def _key_prefix(self, topic: str) -> str:
        return "cw"
```

`tombstone.py`:
```python
"""Tombstone pressure scenario."""

from compaction_stress.scenarios.base import BaseScenario


class TombstoneScenario(BaseScenario):
    name = "tombstone"

    def _key_prefix(self, topic: str) -> str:
        return "ts"
```

`multi_partition.py`:
```python
"""Multi-partition fan-out scenario."""

from compaction_stress.scenarios.base import BaseScenario


class MultiPartitionScenario(BaseScenario):
    name = "multi_partition"

    def _key_prefix(self, topic: str) -> str:
        # Include topic index to spread keys across topics
        idx = self.topics.index(topic) if topic in self.topics else 0
        return f"mp{idx}"
```

`kitchen_sink.py`:
```python
"""Kitchen sink: runs all enabled scenarios concurrently."""

from __future__ import annotations

import threading
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from compaction_stress.config import ClusterConfig, Config
    from compaction_stress.logging import DualLogger

from compaction_stress.scenarios.base import BaseScenario
from compaction_stress.scenarios.key_cardinality import KeyCardinalityScenario
from compaction_stress.scenarios.extreme_dedup import ExtremeDedupScenario
from compaction_stress.scenarios.continuous_write import ContinuousWriteScenario
from compaction_stress.scenarios.tombstone import TombstoneScenario
from compaction_stress.scenarios.multi_partition import MultiPartitionScenario

SCENARIO_CLASSES: dict[str, type[BaseScenario]] = {
    "key_cardinality": KeyCardinalityScenario,
    "extreme_dedup": ExtremeDedupScenario,
    "continuous_write": ContinuousWriteScenario,
    "tombstone": TombstoneScenario,
    "multi_partition": MultiPartitionScenario,
}


def create_scenario(
    name: str,
    cluster: "ClusterConfig",
    config: "Config",
    topics: list[str],
    logger: "DualLogger",
) -> BaseScenario:
    cls = SCENARIO_CLASSES.get(name, BaseScenario)
    return cls(
        cluster=cluster,
        config=config.get_scenario(name),
        topics=topics,
        logger=logger,
    )
```

`__init__.py`:
```python
"""Scenario registry."""

from compaction_stress.scenarios.kitchen_sink import SCENARIO_CLASSES, create_scenario

__all__ = ["SCENARIO_CLASSES", "create_scenario"]
```

**Step 3: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.scenarios import SCENARIO_CLASSES, create_scenario; print(list(SCENARIO_CLASSES.keys()))"`
Expected: list of all 5 scenario names

**Step 4: Commit**

```bash
git add compaction-stress/src/compaction_stress/scenarios/
git commit -m "compaction-stress: add scenario classes (5 scenarios + kitchen sink)"
```

---

## Task 7: Runner (orchestrator)

**Files:**
- Create: `compaction-stress/src/compaction_stress/runner.py`

**Step 1: Write runner.py**

The runner:
1. Runs setup (unless --no-setup)
2. Creates scenario instances
3. Starts each in a thread
4. Runs a reporting loop
5. On shutdown signal, sets the event and joins threads

```python
"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import signal
import threading
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.logging import DualLogger
from compaction_stress.metrics import MetricsScraper
from compaction_stress.scenarios import create_scenario
from compaction_stress.scenarios.base import BaseScenario
from compaction_stress.setup import run_setup


class Runner:
    def __init__(self, config: Config, scenario_name: str | None = None):
        self.config = config
        self.scenario_name = scenario_name
        self.logger = DualLogger(config.log_dir)
        self.shutdown = threading.Event()
        self.scenarios: list[BaseScenario] = []
        self.threads: list[threading.Thread] = []
        self.scraper: MetricsScraper | None = None

    def run(self) -> None:
        # Install signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        enabled = self.config.enabled_scenarios(self.scenario_name)
        self.logger.info(f"Enabled scenarios: {', '.join(enabled)}")

        # Setup
        topic_map = run_setup(
            self.config, enabled,
            log_fn=self.logger.info,
            warn_fn=self.logger.warn,
        )

        # Start metrics scraper
        if self.config.cluster.admin_hosts:
            self.scraper = MetricsScraper(self.config.cluster.admin_hosts)
            self.scraper.set_warn_callback(self.logger.warn)
            self.scraper.start()
            self.logger.info(f"Metrics scraper started ({len(self.config.cluster.admin_hosts)} nodes)")

        # Create and start scenarios
        for name in enabled:
            topics = topic_map.get(name, [])
            scenario = create_scenario(
                name=name,
                cluster=self.config.cluster,
                config=self.config,
                topics=topics,
                logger=self.logger,
            )
            self.scenarios.append(scenario)
            t = threading.Thread(
                target=self._run_scenario,
                args=(scenario,),
                name=f"scenario-{name}",
                daemon=True,
            )
            self.threads.append(t)
            t.start()

        # Reporting loop
        duration = parse_duration(self.config.duration) if self.config.duration else None
        start_time = time.monotonic()

        self.logger.info(
            f"Running {'indefinitely' if duration is None else f'for {self.config.duration}'}"
        )

        while not self.shutdown.is_set():
            elapsed = time.monotonic() - start_time
            if duration and elapsed >= duration:
                self.logger.info("Duration reached, shutting down...")
                self.shutdown.set()
                break
            self.shutdown.wait(self.config.report_interval)
            if not self.shutdown.is_set():
                self._report(time.monotonic() - start_time)

        # Final report
        self._report(time.monotonic() - start_time)

        # Shutdown
        self.logger.info("Waiting for scenarios to finish...")
        for t in self.threads:
            t.join(timeout=60)

        if self.scraper:
            self.scraper.stop()

        self.logger.info("Done.")
        self.logger.close()

    def _run_scenario(self, scenario: BaseScenario) -> None:
        try:
            scenario.run(self.shutdown)
        except Exception as e:
            self.logger.error(f"[{scenario.name}] Error: {e}")

    def _report(self, elapsed: float) -> None:
        scenario_stats: dict[str, dict[str, Any]] = {}
        for s in self.scenarios:
            scenario_stats[s.name] = s.get_stats()

        cluster_metrics = self.scraper.get_metrics() if self.scraper else None
        self.logger.report(elapsed, scenario_stats, cluster_metrics)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown.set()
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "from compaction_stress.runner import Runner; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/runner.py
git commit -m "compaction-stress: add runner orchestrator"
```

---

## Task 8: CLI entry point

**Files:**
- Create: `compaction-stress/src/compaction_stress/cli.py`

**Step 1: Write cli.py**

```python
"""CLI entry point."""

from __future__ import annotations

import argparse
import sys

from compaction_stress.config import ALL_SCENARIOS, load_config
from compaction_stress.runner import Runner


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="compaction-stress",
        description="Cloud topics compaction stress workload generator for Redpanda",
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--brokers", "-b",
        help="Kafka broker addresses (comma-separated), overrides config file",
    )
    parser.add_argument(
        "--admin-hosts",
        help="Admin API hosts for metrics scraping (comma-separated)",
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=ALL_SCENARIOS + ["kitchen_sink"],
        default="kitchen_sink",
        help="Scenario to run (default: kitchen_sink = all enabled)",
    )
    parser.add_argument(
        "--duration", "-d",
        help="Run duration (e.g., '1h', '30m', '12h'). Default from config or '1h'",
    )
    parser.add_argument(
        "--indefinite",
        action="store_true",
        help="Run indefinitely until Ctrl-C",
    )
    parser.add_argument(
        "--no-setup",
        action="store_true",
        help="Skip cluster config and topic creation",
    )
    parser.add_argument(
        "--log-dir",
        help="Directory for JSON log output (default: ./logs)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    cli_overrides: dict = {}
    if args.brokers:
        cli_overrides["brokers"] = args.brokers
    if args.admin_hosts:
        cli_overrides["admin_hosts"] = [h.strip() for h in args.admin_hosts.split(",")]
    if args.indefinite:
        cli_overrides["duration"] = "indefinite"
    elif args.duration:
        cli_overrides["duration"] = args.duration
    if args.no_setup:
        cli_overrides["no_setup"] = True
    if args.log_dir:
        cli_overrides["log_dir"] = args.log_dir

    config = load_config(args.config, cli_overrides)

    scenario = args.scenario if args.scenario != "kitchen_sink" else None
    runner = Runner(config, scenario_name=scenario)
    runner.run()


if __name__ == "__main__":
    main()
```

**Step 2: Verify**

Run: `cd compaction-stress && source .venv/bin/activate && compaction-stress --help`
Expected: help text showing all flags

**Step 3: Commit**

```bash
git add compaction-stress/src/compaction_stress/cli.py
git commit -m "compaction-stress: add CLI entry point"
```

---

## Task 9: End-to-end smoke test

This is a manual verification step. Run the tool against a local broker
(or with `--no-setup` if no broker is available) to verify everything
wires together.

**Step 1: Test --help works**

Run: `cd compaction-stress && source .venv/bin/activate && compaction-stress --help`

**Step 2: Test config loading**

Run: `cd compaction-stress && source .venv/bin/activate && python3 -c "
from compaction_stress.config import load_config
c = load_config('config.example.yaml', {})
print(f'brokers: {c.cluster.brokers}')
print(f'scenarios: {list(c.scenarios.keys())}')
print(f'duration: {c.duration}')
for name, sc in c.scenarios.items():
    print(f'  {name}: keys={sc.key_count} partitions={sc.partitions} tombstone_prob={sc.tombstone_probability}')
"`

Expected: prints all scenario configs with correct values from the example config.

**Step 3: Test dry run (no broker needed)**

Run: `cd compaction-stress && source .venv/bin/activate && compaction-stress --config config.example.yaml --no-setup --scenario continuous_write --duration 5s --log-dir /tmp/ct-stress-test 2>&1 || true`

This will fail to connect to brokers but should get past config loading,
setup skipping, and scenario creation. If it fails on producer creation
(connection refused), that's expected and fine — verify the error is a
Kafka connection error, not a Python import/config error.

**Step 4: Check JSON log was created**

Run: `ls /tmp/ct-stress-test/compaction-stress-*.jsonl`

**Step 5: Commit (if any fixes were needed)**

```bash
git add -u compaction-stress/
git commit -m "compaction-stress: fix issues found in smoke test"
```

---

## Task 10: README

**Files:**
- Create: `compaction-stress/README.md`

**Step 1: Write README**

A concise quick-start guide covering:
- What the tool does (1 paragraph)
- Prerequisites (Python 3.11+, rpk on PATH)
- Install: `python3 -m venv .venv && source .venv/bin/activate && pip install -e .`
- Configure: `cp config.example.yaml config.yaml && vi config.yaml`
- Run examples:
  - Kitchen sink: `compaction-stress --config config.yaml --duration 4h`
  - Single scenario: `compaction-stress --config config.yaml --scenario continuous_write --indefinite`
  - No setup: `compaction-stress --config config.yaml --no-setup --duration 1h`
- Scenario descriptions (one line each)
- Config reference: point to `config.example.yaml`
- Deploying to remote machine: tar + scp instructions

**Step 2: Commit**

```bash
git add compaction-stress/README.md
git commit -m "compaction-stress: add README"
```
