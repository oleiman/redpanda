# compaction-stress

Generates sustained Kafka workloads that pressure cloud topics compaction on a
live Redpanda cluster. Multiple producer processes per scenario, with tunable
parameters for key cardinality, produce rate, tombstone frequency, and partition
fan-out.

Uses a Go producer binary (`ct-producer`) for high throughput (~200+ MB/s per
process). Falls back to Python producers (~50-60 MB/s per process) if the Go
binary isn't found.

## Prerequisites

- Python 3.11+
- Go 1.22+ (to build `ct-producer`)
- `rpk` on PATH (for topic creation; skip with `--no-setup`)
- Network access to Kafka (9092) and optionally admin API (9644) ports

## Install

```bash
# Build the Go producer
cd ct-producer && go build -o ct-producer . && cd ..

# Install the Python orchestrator
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

The Python runner auto-detects `ct-producer/ct-producer` at startup. If not
found, it falls back to Python multiprocessing producers (slower).

## Configure

```bash
cp config.example.yaml config.yaml
vi config.yaml  # set brokers, credentials, scenario params
```

At minimum, set `cluster.brokers` to a single seed broker (franz-go / librdkafka
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

# Scale all rates (2x = double throughput)
compaction-stress -c config.yaml --rate-multiplier 2.0 --duration 4h

# Minimal (CLI only, no config file)
compaction-stress -b seed1:9092 -s extreme_dedup -d 30m --no-setup
```

## Scenarios

| Scenario | What it stresses | Default rate | Producers | Partitions |
|---|---|---|---|---|
| `key_cardinality` | 500K keys forcing multi-pass compaction | 200 MB/s | 3 | 8 |
| `extreme_dedup` | 100 keys, 1KB values, massive dedup ratio | 200 MB/s | 3 | 4 |
| `continuous_write` | 200K keys + 15s compaction lag, moving frontier | 200 MB/s | 3 | 12 |
| `tombstone` | 25% tombstones, 30s retention | 150 MB/s | 2 | 8 |
| `multi_partition` | 6 topics x 16 partitions = 96 partitions | 300 MB/s | 4 | 96 |
| `kitchen_sink` | All above concurrently (default) | ~1 GB/s | 15 | 128 |

Each scenario spawns `num_producers` worker processes. All workers for a
scenario write to the same key space, maximizing dedup work for compaction.

## Output

Periodic human-readable summaries to stdout:

```
[03:42:15] ── 2h12m elapsed ──────────────────────────────────────────
  continuous_write    :   4.2M records │ 198.0 MB/s │ 0 errors
  key_cardinality     :   1.8M records │ 195.0 MB/s │ 0 errors
  tombstone           :  890.0K records │ 142.0 MB/s │ 0 errors │ 220.0K tombstones
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
- `cloud_topics_compaction_key_map_memory` — map capacity (requires restart; shrink to 16MB to force multi-pass)

**Topic configs** (per-scenario in `topic_config:`):
- `min.cleanable.dirty.ratio` — dirty ratio trigger
- `min.compaction.lag.ms` / `max.compaction.lag.ms` — age-based gating
- `delete.retention.ms` — tombstone retention

**Scenario params**: `key_count`, `msg_size`, `rate_limit_bps`, `partitions`,
`num_producers`, `tombstone_probability`, `num_topics`

## Environment variables

- `REDPANDA_BROKERS` — broker address(es)
- `REDPANDA_SASL_USER`, `REDPANDA_SASL_PASSWORD`, `REDPANDA_SASL_MECHANISM` — SASL auth

Config precedence: CLI flags > environment variables > config file > built-in defaults.

## Deploy to remote

```bash
# Build Go binary
cd compaction-stress/ct-producer && go build -o ct-producer . && cd ../..

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

If Go isn't available on the remote, build on your local machine with
cross-compilation:

```bash
GOOS=linux GOARCH=amd64 go build -o ct-producer/ct-producer ./ct-producer/
```

## Architecture

- **Go producer** (`ct-producer/`) — high-throughput franz-go binary, one per
  (worker, topic) pair. Prints JSON stats to stdout every second.
- **Python orchestrator** — config, topic setup (via rpk), metrics scraping,
  offset tracking, reporting, graceful shutdown
- **Multiple producers per scenario** — `num_producers` Go processes share the
  same key space, multiplying throughput and dedup pressure
- **Graceful shutdown** — Ctrl-C sends SIGTERM to Go processes; second Ctrl-C
  force-kills everything
- **Optional metrics scraping** — background thread polls `/metrics` from each
  admin host, degrades gracefully if unreachable
- **Offset tracking** — background thread counts actual records per topic
  partition to measure compaction effectiveness (remaining/total ratio)
