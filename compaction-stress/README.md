# compaction-stress

Generates sustained Kafka workloads that pressure cloud topics compaction on a
live Redpanda cluster. Each scenario runs in its own process (no GIL contention),
with tunable parameters for key cardinality, produce rate, tombstone frequency,
and partition fan-out.

## Prerequisites

- Python 3.11+
- `rpk` on PATH (for topic creation; skip with `--no-setup`)
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

At minimum, set `cluster.brokers` to a single seed broker (librdkafka
discovers the rest). Everything else has sensible defaults.

## Run

```bash
# Kitchen sink (all scenarios), 4 hours
compaction-stress --config config.yaml --duration 4h

# Single scenario, indefinite (Ctrl-C to stop)
compaction-stress -c config.yaml -s continuous_write --indefinite

# With metrics scraping from cluster nodes
compaction-stress -c config.yaml --admin-hosts node1:9644,node2:9644

# Skip setup (topics/configs already exist)
compaction-stress -c config.yaml --no-setup --duration 1h

# Delete and recreate topics on startup
compaction-stress -c config.yaml --delete-existing-topics --duration 2h

# Set cluster configs via admin API at startup
compaction-stress -c config.yaml --set-cluster-config --duration 4h

# Minimal (CLI only, no config file)
compaction-stress -b seed1:9092 -s extreme_dedup -d 30m --no-setup
```

## Scenarios

| Scenario | What it stresses | Default rate |
|---|---|---|
| `key_cardinality` | More unique keys than the key-offset map can hold, forcing multi-pass compaction | 10 MB/s |
| `extreme_dedup` | Very few keys (10) with massive updates (100K:1 dedup ratio) | 5 MB/s |
| `continuous_write` | Sustained produce with `min.compaction.lag.ms=30s` so compaction can never fully catch up | 10 MB/s |
| `tombstone` | 15% tombstone rate interacting with `delete.retention.ms=60s` | 5 MB/s |
| `multi_partition` | 4 topics x 8 partitions competing for compaction scheduler slots | 20 MB/s |
| `kitchen_sink` | All enabled scenarios concurrently (default) | ~50 MB/s |

Each scenario runs in its own process. In kitchen sink mode all 5 run in
parallel, each with independent librdkafka instances.

## Output

Periodic human-readable summaries to stdout:

```
[03:42:15] ── 2h12m elapsed ──────────────────────────────────────────
  continuous_write    :   4.2M records │   9.8 MB/s │ 0 errors
  key_cardinality     :   1.8M records │   4.9 MB/s │ 0 errors
  tombstone           :  890.0K records │   2.4 MB/s │ 0 errors │ 133.5K tombstones
  ── compaction progress ──
  ct-stress-continuous_write-0: 770.0K remaining / 4.1M offsets │ 3.4M removed │ ratio 0.19
```

Structured JSON lines to `--log-dir` (default `./logs/`):

```json
{"ts":"2026-03-17T03:42:15Z","type":"report","elapsed":7935.0,"scenarios":{...},"cluster_metrics":{...}}
```

When `--admin-hosts` is provided, cluster compaction metrics are scraped from
each node's `/metrics` endpoint and included in reports.

## Config reference

See `config.example.yaml` for all options with comments.

**Cluster configs** (set via rpk with `--set-cluster-config`):
- `cloud_topics_compaction_interval_ms` — scheduler trigger frequency
- `cloud_topics_compaction_max_object_size` — L1 object size ceiling
- `cloud_topics_compaction_key_map_memory` — map capacity (requires restart)

**Topic configs** (per-scenario in `topic_config:`):
- `min.cleanable.dirty.ratio` — dirty ratio trigger
- `min.compaction.lag.ms` / `max.compaction.lag.ms` — age-based gating
- `delete.retention.ms` — tombstone retention

**Scenario params**: `key_count`, `msg_size`, `rate_limit_bps`, `partitions`,
`tombstone_probability`, `num_topics`

## Environment variables

- `REDPANDA_BROKERS` — broker address(es)
- `REDPANDA_SASL_USER`, `REDPANDA_SASL_PASSWORD`, `REDPANDA_SASL_MECHANISM` — SASL auth

Config precedence: CLI flags > environment variables > config file > built-in defaults.

## Deploy to remote

```bash
# Package (exclude venv and logs)
tar czf compaction-stress.tar.gz \
  --exclude='.venv' --exclude='logs' --exclude='__pycache__' \
  -C /path/to/repo compaction-stress/

scp compaction-stress.tar.gz remote:~/

# On remote
ssh remote
tar xzf compaction-stress.tar.gz
cd compaction-stress
python3 -m venv .venv && source .venv/bin/activate && pip install -e .
cp config.example.yaml config.yaml && vi config.yaml
compaction-stress -c config.yaml --duration 12h
```

## Architecture

- **One process per scenario** — each gets its own Python interpreter and
  librdkafka instance, no GIL contention
- **Batch-level rate limiting** — one sleep per 1000 messages, not per-message
- **Graceful shutdown** — Ctrl-C/SIGTERM sets a shared event, child processes
  flush and exit, stuck processes are terminated after 10s
- **Optional metrics scraping** — background thread polls `/metrics` from each
  admin host, degrades gracefully if unreachable
- **Offset tracking** — background thread counts actual records per topic
  partition to measure compaction effectiveness (remaining/total ratio)
