# compaction-stress

Generates sustained Kafka workloads that pressure cloud topics compaction on a
live Redpanda cluster. Uses [kgo-repeater](https://github.com/redpanda-data/kgo-verifier)
(from the kgo-verifier repo) as the produce/consume engine — battle-tested
against BYOC clusters with proper TLS/SASL/connection handling.

Each scenario runs as one `kgo-repeater` process with configurable in-process
parallelism (`num_producers` maps to `--workers`). The Python orchestrator
handles topic setup, config, metrics scraping, and reporting.

## Prerequisites

- Python 3.11+
- Go 1.22+ (to build `kgo-repeater`)
- `rpk` on PATH (for topic creation; skip with `--no-setup`)
- A checkout of [kgo-verifier](https://github.com/redpanda-data/kgo-verifier)

## Setup

### 1. Build kgo-repeater

```bash
# Clone if you don't have it already
git clone https://github.com/redpanda-data/kgo-verifier.git ~/co/kgo-verifier

# Build
cd ~/co/kgo-verifier
go build -o kgo-repeater ./cmd/kgo-repeater/
```

The Python runner auto-detects the binary at `~/co/kgo-verifier/kgo-repeater`.
Alternatively, place it anywhere on your `PATH`.

### 2. Install the Python orchestrator

```bash
cd compaction-stress
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 3. Configure

```bash
cp config.example.yaml config.yaml
vi config.yaml
```

At minimum, set:
- `cluster.brokers` — a single seed broker is sufficient
- `cluster.sasl_user` / `cluster.sasl_password` / `cluster.sasl_mechanism` — if your cluster uses SASL
- `cluster.tls_enabled: true` — if your cluster uses TLS (e.g., BYOC)

Everything else has sensible defaults.

## Run

```bash
# Kitchen sink (all scenarios), 4 hours
compaction-stress -c config.yaml -d 4h

# Single scenario, indefinite (Ctrl-C to stop)
compaction-stress -c config.yaml -s continuous_write --indefinite

# With metrics scraping from cluster admin API
compaction-stress -c config.yaml --admin-hosts node1:9644,node2:9644

# Skip topic creation (topics already exist)
compaction-stress -c config.yaml --no-setup -d 1h

# Delete and recreate topics for a clean start
compaction-stress -c config.yaml --delete-existing-topics -d 2h

# Scale all rates (2x = double throughput)
compaction-stress -c config.yaml --rate-multiplier 2.0 -d 4h

# Set cluster configs via rpk at startup
compaction-stress -c config.yaml --set-cluster-config -d 4h
```

Ctrl-C triggers graceful shutdown (kgo-repeater flushes and exits).
Second Ctrl-C force-kills everything.

## Scenarios

| Scenario | What it stresses | Default rate | Workers | Partitions |
|---|---|---|---|---|
| `key_cardinality` | 500K keys forcing multi-pass compaction | 200 MB/s | 3 | 8 |
| `extreme_dedup` | 100 keys, 1KB values, massive dedup ratio | 200 MB/s | 3 | 4 |
| `continuous_write` | 200K keys + 15s compaction lag, moving frontier | 200 MB/s | 3 | 12 |
| `tombstone` | 25% tombstones, 30s retention | 150 MB/s | 2 | 8 |
| `multi_partition` | 6 topics x 16 partitions = 96 partitions | 300 MB/s | 4 | 96 |
| `kitchen_sink` | All above concurrently (default) | ~1 GB/s | 15 | 128 |

Each scenario runs as one `kgo-repeater` process. `num_producers` in the config
maps to kgo-repeater's `--workers` flag (in-process parallelism). All workers
share the same key space, maximizing dedup work for compaction.

## Output

Periodic human-readable summaries to stdout:

```
[03:42:15] ── 2h12m elapsed ──────────────────────────────────────────
  continuous_write    :   4.2M records │ 198.0 MB/s │ 0 errors
  tombstone           :  890.0K records │ 142.0 MB/s │ 0 errors
  ── compaction progress ──
  ct-stress-continuous_write-0: 770.0K remaining / 4.1M offsets │ 3.4M removed │ ratio 0.19
```

Structured JSON lines to `--log-dir` (default `./logs/`):

```json
{"ts":"2026-03-17T03:42:15Z","type":"report","elapsed":7935.0,"scenarios":{...}}
```

When `--admin-hosts` is provided, cluster compaction metrics are included.

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
# 1. Build kgo-repeater (on remote, or cross-compile locally)
cd ~/co/kgo-verifier
go build -o kgo-repeater ./cmd/kgo-repeater/
# Or cross-compile: GOOS=linux GOARCH=amd64 go build -o kgo-repeater ./cmd/kgo-repeater/

# 2. Package compaction-stress
tar czf compaction-stress.tar.gz \
  --exclude='.venv' --exclude='logs' --exclude='__pycache__' \
  -C /path/to/repo compaction-stress/

# 3. Copy both to remote
scp compaction-stress.tar.gz remote:~/
scp ~/co/kgo-verifier/kgo-repeater remote:~/co/kgo-verifier/kgo-repeater
# (or scp to anywhere on PATH)

# 4. On remote
ssh remote
tar xzf compaction-stress.tar.gz
cd compaction-stress
python3 -m venv .venv && source .venv/bin/activate && pip install -e .
cp config.example.yaml config.yaml && vi config.yaml
compaction-stress -c config.yaml -d 12h
```

## Architecture

- **kgo-repeater** — one process per scenario, handles produce+consume with
  in-process `--workers` parallelism and auto-tuned data in flight
- **Python orchestrator** — config loading, topic setup (via rpk), launching
  kgo-repeater processes, polling `/status` for stats, periodic reporting,
  optional Prometheus metrics scraping, graceful shutdown
- **Consumer groups** — each scenario gets its own group (`ct-stress-<name>`),
  kgo-repeater verifies produce/consume round-trips
- **Offset tracking** — background thread counts actual records per partition
  to measure compaction effectiveness (remaining/total ratio)
