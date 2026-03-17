# compaction-stress

Generates sustained Kafka workloads that pressure cloud topics compaction on a
live Redpanda cluster. Supports multiple scenarios targeting different compaction
bottlenecks, with tunable parameters for key cardinality, produce rate, tombstone
frequency, and more.

## Prerequisites

- Python 3.11+
- `rpk` on PATH (for cluster/topic setup; skip with `--no-setup`)
- Network access to Kafka (9092) and optionally admin API (9644) ports

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configure

```bash
cp config.example.yaml config.yaml
vi config.yaml  # set brokers, credentials, scenario params
```

Or use CLI flags / environment variables (see below).

## Run

```bash
# Kitchen sink (all scenarios), 4 hours
compaction-stress --config config.yaml --duration 4h

# Single scenario, indefinite
compaction-stress --config config.yaml --scenario continuous_write --indefinite

# With metrics scraping
compaction-stress --config config.yaml --admin-hosts node1:9644,node2:9644

# Skip setup (topics/configs already exist)
compaction-stress --config config.yaml --no-setup --duration 1h

# Minimal (CLI only, no config file)
compaction-stress --brokers seed1:9092 --scenario extreme_dedup --duration 30m --no-setup
```

## Scenarios

| Scenario | What it stresses |
|---|---|
| `key_cardinality` | More unique keys than the key-offset map can hold, forcing multi-pass compaction |
| `extreme_dedup` | Very few keys with massive updates (100K:1 dedup ratio) |
| `continuous_write` | Sustained produce so compaction can never fully catch up |
| `tombstone` | High tombstone rate interacting with `delete.retention.ms` |
| `multi_partition` | Many partitions/topics competing for scheduler slots |
| `kitchen_sink` | All enabled scenarios concurrently (default) |

## Config reference

See `config.example.yaml` for all options. Key tuning knobs:

**Cluster configs** (set via rpk at startup):
- `cloud_topics_compaction_interval_ms` — scheduler trigger frequency
- `cloud_topics_compaction_max_object_size` — L1 object size ceiling
- `cloud_topics_compaction_key_map_memory` — map capacity (requires restart)

**Topic configs** (per-scenario):
- `min.cleanable.dirty.ratio` — dirty ratio trigger
- `min.compaction.lag.ms` / `max.compaction.lag.ms` — age-based gating
- `delete.retention.ms` — tombstone retention

**Scenario params**: `key_count`, `msg_size`, `rate_limit_bps`, `partitions`,
`tombstone_probability`, `num_topics`

## Environment variables

- `REDPANDA_BROKERS` — broker addresses
- `REDPANDA_SASL_USER`, `REDPANDA_SASL_PASSWORD`, `REDPANDA_SASL_MECHANISM` — SASL auth

## Deploy to remote

```bash
tar czf compaction-stress.tar.gz -C /path/to/repo compaction-stress/
scp compaction-stress.tar.gz remote:~/
ssh remote 'cd compaction-stress && python3 -m venv .venv && source .venv/bin/activate && pip install -e .'
```
