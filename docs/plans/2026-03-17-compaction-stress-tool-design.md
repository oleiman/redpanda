# Cloud Topics Compaction Stress Tool — Design Document

**Date:** 2026-03-17
**Goal:** A self-contained, portable Python tool that generates sustained, tunable
workloads to pressure cloud topics compaction on a live Redpanda cluster.

---

## Overview

The tool produces Kafka traffic with specific key distribution patterns that
stress different aspects of cloud topics L1 compaction: key-offset map overflow,
extreme deduplication ratios, sustained write pressure, tombstone handling, and
multi-partition fan-out. It runs from a client machine against an existing
cluster, reports progress to stdout, and writes structured JSON logs for
post-hoc analysis.

## Project Structure

```
compaction-stress/
├── pyproject.toml
├── config.example.yaml
├── README.md
└── src/
    └── compaction_stress/
        ├── __init__.py
        ├── cli.py              # argparse CLI, config loading, main entry point
        ├── config.py           # Config dataclasses
        ├── runner.py           # Orchestrator: setup → run scenarios → teardown
        ├── setup.py            # rpk-based topic creation and cluster config
        ├── producer.py         # confluent-kafka producer with key distribution
        ├── metrics.py          # Optional Prometheus metrics scraper
        ├── logging.py          # Dual logger: human stdout + JSON file
        └── scenarios/
            ├── __init__.py
            ├── base.py
            ├── key_cardinality.py
            ├── extreme_dedup.py
            ├── continuous_write.py
            ├── tombstone.py
            ├── multi_partition.py
            └── kitchen_sink.py
```

**Dependencies** (minimal):
- `confluent-kafka` — producer/consumer
- `pyyaml` — config file parsing
- `requests` — metrics scraping

## Configuration System

### Precedence

CLI flags > environment variables > config file > built-in defaults.

### Config file (`config.yaml`)

```yaml
cluster:
  brokers: "seed1:9092,seed2:9092,seed3:9092"
  sasl_mechanism: "SCRAM-SHA-256"   # optional
  sasl_user: "admin"                # optional
  sasl_password: "secret"           # optional
  tls_enabled: false                # optional
  admin_hosts:                      # optional, for metrics scraping
    - "node1:9644"
    - "node2:9644"

cluster_config:
  cloud_topics_compaction_interval_ms: 5000
  cloud_topics_compaction_max_object_size: 134217728
  cloud_topics_compaction_key_map_memory: 134217728

scenarios:
  key_cardinality:
    enabled: true
    key_count: 200000
    msg_size: 512
    rate_limit_bps: 10485760
    partitions: 1
    topic_config:
      "min.cleanable.dirty.ratio": "0.0"

  extreme_dedup:
    enabled: true
    key_count: 10
    msg_size: 256
    rate_limit_bps: 5242880
    partitions: 1

  continuous_write:
    enabled: true
    key_count: 50000
    msg_size: 512
    rate_limit_bps: 10485760
    partitions: 4
    topic_config:
      "min.compaction.lag.ms": "30000"

  tombstone:
    enabled: true
    key_count: 10000
    msg_size: 256
    tombstone_probability: 0.15
    rate_limit_bps: 5242880
    partitions: 2
    topic_config:
      "delete.retention.ms": "60000"

  multi_partition:
    enabled: true
    key_count: 20000
    msg_size: 512
    rate_limit_bps: 20971520
    num_topics: 4
    partitions: 8
    topic_config: {}

defaults:
  msg_size: 512
  rate_limit_bps: 10485760
  partitions: 1
  replicas: 3
  topic_config:
    "cleanup.policy": "compact"
    "min.cleanable.dirty.ratio": "0.01"
```

### CLI

```
compaction-stress \
  --config config.yaml \
  --brokers seed1:9092 \
  --scenario continuous_write \   # default: kitchen_sink (all enabled)
  --duration 4h \                 # or --indefinite
  --no-setup \                    # skip topic/cluster config
  --log-dir ./logs \
  --admin-hosts node1:9644,node2:9644
```

## Scenarios

### Base interface

Each scenario is a class with three methods:

```python
class BaseScenario:
    name: str
    topics: list[TopicSpec]

    def setup(self, rpk, config) -> None: ...
    def run(self, producer, logger, shutdown_event) -> None: ...
    def teardown(self, rpk) -> None: ...
```

### 1. Key Cardinality Overflow (`key_cardinality`)

Produces data with more unique keys than the key-offset map can hold (default:
200K keys vs ~25K map capacity at 1MB budget). Forces multi-pass compaction —
each pass can only deduplicate keys that fit in the map, requiring ~8 passes
for full convergence.

- Keys: `f"kc-{i % key_count}"`, uniform cycling
- Default: 200K keys, 512-byte values, 10 MB/s, 1 partition

### 2. Extreme Dedup Ratio (`extreme_dedup`)

Very few unique keys with massive update counts (default: 10 keys). After
compaction, nearly all records should be removed. Tests deduplication
correctness and filtering throughput at extreme ratios (100K:1+).

- Keys: `f"ed-{i % key_count}"`, 10 keys
- Default: 256-byte values, 5 MB/s, 1 partition

### 3. Continuous Write Pressure (`continuous_write`)

Sustained produce throughput so the dirty frontier keeps advancing. Combined
with `min.compaction.lag.ms=30s`, recent data is always excluded from
compaction. The compaction scheduler must make incremental progress against
a moving target.

- Keys: `f"cw-{i % key_count}"`, 50K keys
- Default: 512-byte values, 10 MB/s, 4 partitions

### 4. Tombstone Pressure (`tombstone`)

High tombstone rate interacting with `delete.retention.ms`. A configurable
fraction of records are tombstones (null values). Tests that tombstone
removal respects retention and that compaction correctly handles interleaved
tombstones and live records.

- Keys: `f"ts-{i % key_count}"`, 10K keys
- Tombstone probability: 15% by default
- Default: 256-byte values, 5 MB/s, 2 partitions
- `delete.retention.ms`: 60s (aggressive, for faster turnover)

### 5. Multi-Partition Fan-Out (`multi_partition`)

Many partitions across multiple topics competing for scheduler slots. Tests
that the compaction scheduler fairly distributes work and doesn't starve
any partition.

- Keys: `f"mp-{topic_idx}-{i % key_count}"`, 20K keys per topic
- Default: 4 topics × 8 partitions = 32 partitions, 20 MB/s aggregate

### 6. Kitchen Sink (`kitchen_sink`)

Runs all enabled scenarios concurrently in separate threads. Each scenario
gets its own producer instance and topics (prefixed `ct-stress-<scenario>-<n>`).
A shared `threading.Event` coordinates graceful shutdown.

## Key Distribution & Rate Limiting

**Key generation**: Keys cycle uniformly: `f"{prefix}-{counter % key_count}"`.
This ensures every key gets repeated updates at a predictable rate.

**Values**: Random bytes of `msg_size` length, generated once per batch (not
per-record) for efficiency.

**Rate limiting**: Token bucket at the producer level. Track bytes produced per
second, sleep in the `poll()` loop when over budget. Gives smooth throughput
rather than bursty produce-then-wait.

## Metrics & Logging

### Metrics scraper (optional)

When `--admin-hosts` is provided, a background thread polls each node's
`/metrics` endpoint every 10 seconds. Parses Prometheus text format with
simple regex (no dependency). Extracts and sums across nodes:

- `vectorized_cloud_topics_compaction_scheduler_log_compactions`
- `vectorized_cloud_topics_compaction_scheduler_compaction_queue_length`
- `vectorized_cloud_topics_compaction_worker_records_removed`
- `vectorized_cloud_topics_compaction_worker_tombstones_removed`
- `vectorized_cloud_topics_compaction_worker_compaction_duration_seconds`

Unreachable nodes are skipped with a warning.

### Stdout

Periodic summary every 30 seconds:

```
[03:42:15] ── 2h12m elapsed ──────────────────────────────────────────
  continuous_write : 4.2M records │ 9.8 MB/s │ 0 errors
  key_cardinality  : 1.8M records │ 4.9 MB/s │ 0 errors
  tombstone        : 890K records │ 2.4 MB/s │ 12K tombstones
  extreme_dedup    : 3.1M records │ 4.7 MB/s │ 0 errors
  multi_partition  : 2.5M records │ 18.2 MB/s │ 0 errors (4 topics)
  ── cluster compaction (6 nodes) ──
  compaction rounds: 847 │ queue depth: 3 │ records removed: 2.1M │ tombstones removed: 4.2K
```

### JSON log file

One JSON object per line to `--log-dir/compaction-stress-<timestamp>.jsonl`:

```json
{"ts": "2026-03-17T03:42:15Z", "type": "producer", "scenario": "continuous_write", "records": 4200000, "bytes_per_sec": 10276044, "errors": 0}
{"ts": "2026-03-17T03:42:15Z", "type": "metrics", "compaction_rounds": 847, "queue_depth": 3, "records_removed": 2100000, "tombstones_removed": 4200}
```

## Setup & Teardown

### Startup (unless `--no-setup`)

1. Set cluster configs via `rpk cluster config set` for each `cluster_config` entry
2. Create topics via `rpk topic create` with per-scenario topic configs
3. Wait for partition metadata propagation

Idempotent: existing topics log a warning and continue.

### Graceful shutdown

`SIGINT`/`SIGTERM` sets a shared shutdown event. Each scenario's `run()` loop
checks it, drains its producer (`flush()`), and returns. Metrics scraper stops.
JSON logs are finalized. Exit code 0 on clean shutdown.

## Deployment

```bash
# Package and deploy
cd compaction-stress/
tar czf compaction-stress.tar.gz .
scp compaction-stress.tar.gz remote-host:~/compaction-stress.tar.gz

# On remote
mkdir compaction-stress && cd compaction-stress
tar xzf ~/compaction-stress.tar.gz
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
cp config.example.yaml config.yaml && vi config.yaml
compaction-stress --config config.yaml --duration 12h
```

### Target machine requirements

- Python 3.11+
- `rpk` on PATH (for setup; skip with `--no-setup`)
- Network access to Kafka ports (9092) and optionally admin ports (9644)

## Tuning Knobs Summary

| Knob | Where | Effect on compaction |
|------|-------|---------------------|
| `cloud_topics_compaction_interval_ms` | cluster config | How often scheduler triggers |
| `cloud_topics_compaction_max_object_size` | cluster config | L1 object size ceiling |
| `cloud_topics_compaction_key_map_memory` | cluster config (restart) | Map capacity → multi-pass threshold |
| `min.cleanable.dirty.ratio` | topic config | Dirty ratio trigger threshold |
| `min.compaction.lag.ms` | topic config | Excludes recent data from compaction |
| `max.compaction.lag.ms` | topic config | Forces compaction after age threshold |
| `delete.retention.ms` | topic config | Tombstone retention before removal |
| `key_count` | scenario param | Key cardinality → map pressure |
| `rate_limit_bps` | scenario param | Produce throughput → dirty data rate |
| `tombstone_probability` | scenario param | Tombstone frequency |
| `partitions` / `num_topics` | scenario param | Scheduler fan-out pressure |
