# compaction-stress

Generates sustained Kafka workloads that pressure cloud topics compaction on a
live Redpanda cluster. Uses [kgo-verifier](https://github.com/redpanda-data/kgo-verifier)
as the produce engine — battle-tested against BYOC clusters with proper
TLS/SASL/connection handling.

The Python orchestrator handles topic setup, config, iceberg table tracking,
and reporting.

## Prerequisites

- Python 3.11+
- Go 1.22+ (to build `kgo-verifier`)
- `rpk` on PATH (for topic creation; skip with `--no-setup`)
- A checkout of [kgo-verifier](https://github.com/redpanda-data/kgo-verifier)
- For iceberg tracking: GCS credentials (`gcloud auth application-default login`)

## Setup

### 1. Build kgo-verifier

```bash
git clone https://github.com/redpanda-data/kgo-verifier.git ~/co/kgo-verifier
cd ~/co/kgo-verifier
go build -o kgo-verifier ./cmd/kgo-verifier/
```

The Python runner auto-detects the binary at `~/co/kgo-verifier/kgo-verifier`.
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
- `cluster.sasl_user` / `cluster.sasl_password` / `cluster.sasl_mechanism` — if SASL
- `cluster.tls_enabled: true` — if TLS (e.g., BYOC)

For iceberg table tracking:
- `cluster.gcs_bucket` — your Redpanda cloud storage bucket name

### 4. GCS authentication (for iceberg tracking)

The iceberg tracker reads table metadata directly from GCS. It uses
application default credentials:

```bash
# Authenticate
gcloud auth application-default login

# Verify it works (replace with your bucket/path)
python3 -c "
import gcsfs
fs = gcsfs.GCSFileSystem()
print(fs.ls('your-bucket-name/redpanda-iceberg-catalog/redpanda/')[:5])
"
```

If you see a list of table directories, you're authenticated. If you get
a permission error, check your IAM roles on the bucket.

## Run

```bash
# Kitchen sink (all scenarios), 4 hours
compaction-stress -c config.yaml -d 4h

# Single scenario, indefinite (Ctrl-C to stop)
compaction-stress -c config.yaml -s continuous_write --indefinite

# Iceberg scenario (requires iceberg_enabled=true on cluster)
compaction-stress -c config.yaml -s iceberg -d 4h

# Disable the compaction progress checker (avoids slow consumer reads)
compaction-stress -c config.yaml --no-offset-tracker -d 4h

# Skip topic creation (topics already exist)
compaction-stress -c config.yaml --no-setup -d 1h

# Delete and recreate topics for a clean start
compaction-stress -c config.yaml --delete-existing-topics -d 2h

# Scale all rates (2x = double throughput)
compaction-stress -c config.yaml --rate-multiplier 2.0 -d 4h

# With admin API metrics scraping
compaction-stress -c config.yaml --admin-hosts node1:9644,node2:9644

# Set cluster configs via rpk at startup
compaction-stress -c config.yaml --set-cluster-config -d 4h
```

Ctrl-C triggers graceful shutdown. Second Ctrl-C force-kills everything.

## Scenarios

| Scenario | What it stresses | Default rate | Workers | Partitions |
|---|---|---|---|---|
| `key_cardinality` | 500K keys forcing multi-pass compaction | 200 MB/s | 3 | 8 |
| `extreme_dedup` | 100 keys, 16KB values, massive dedup ratio | 200 MB/s | 3 | 4 |
| `continuous_write` | 200K keys + 15s compaction lag, moving frontier | 200 MB/s | 3 | 12 |
| `tombstone` | 5K keys, high churn + 30s delete retention | 150 MB/s | 3 | 8 |
| `multi_partition` | 6 topics x 16 partitions = 96 partitions | 300 MB/s | 1 | 96 |
| `iceberg` | 100K keys + iceberg translation racing compaction | 200 MB/s | 1 | 8 |
| `kitchen_sink` | All above except iceberg (default) | ~1 GB/s | — | 128 |

The iceberg scenario is disabled by default in kitchen sink mode (requires
`iceberg_enabled=true` cluster config + restart). Run it explicitly with
`--scenario iceberg` or enable it in your config.

## Output

Periodic human-readable summaries to stdout:

```
[03:42:15] ── 2h12m elapsed ──────────────────────────────────────────
  iceberg             :   191.8K records │ 199.8 MB/s │ 0 errors
  ── iceberg tables ──
  ct-stress-iceberg-0: 125.8K rows │ 6 files │ 6 snapshots │ v6
```

Structured JSON lines to `--log-dir` (default `./logs/`).

## Config reference

See `config.example.yaml` for all options with comments.

**Cluster configs** (set via rpk with `--set-cluster-config`):
- `cloud_topics_compaction_interval_ms` — scheduler trigger frequency
- `cloud_topics_compaction_max_object_size` — L1 object size ceiling
- `cloud_topics_compaction_key_map_memory` — map capacity (requires restart; shrink to 16MB to force multi-pass)
- `iceberg_enabled` — enable iceberg translation (requires restart)
- `iceberg_catalog_commit_interval_ms` — how often to commit to catalog
- `iceberg_target_lag_ms` — target translation lag

**Topic configs** (per-scenario in `topic_config:`):
- `min.cleanable.dirty.ratio` — dirty ratio trigger
- `min.compaction.lag.ms` / `max.compaction.lag.ms` — age-based gating
- `delete.retention.ms` — tombstone retention
- `redpanda.iceberg.mode` — iceberg translation mode (`key_value`, etc.)

**Scenario params**: `key_count`, `msg_size`, `rate_limit_bps`, `partitions`,
`num_producers`, `num_topics`

## Environment variables

- `REDPANDA_BROKERS` — broker address(es)
- `REDPANDA_SASL_USER`, `REDPANDA_SASL_PASSWORD`, `REDPANDA_SASL_MECHANISM` — SASL auth

Config precedence: CLI flags > environment variables > config file > built-in defaults.

## Deploy to remote

```bash
# 1. Build kgo-verifier (on remote, or cross-compile locally)
cd ~/co/kgo-verifier
go build -o kgo-verifier ./cmd/kgo-verifier/
# Or cross-compile: GOOS=linux GOARCH=amd64 go build -o kgo-verifier ./cmd/kgo-verifier/

# 2. Package compaction-stress
tar czf compaction-stress.tar.gz \
  --exclude='.venv' --exclude='logs' --exclude='__pycache__' \
  -C /path/to/repo compaction-stress/

# 3. Copy both to remote
scp compaction-stress.tar.gz remote:~/
scp ~/co/kgo-verifier/kgo-verifier remote:~/co/kgo-verifier/kgo-verifier

# 4. On remote
ssh remote
tar xzf compaction-stress.tar.gz
cd compaction-stress
python3 -m venv .venv && source .venv/bin/activate && pip install -e .
cp config.example.yaml config.yaml && vi config.yaml

# 5. For iceberg tracking, authenticate GCS
gcloud auth application-default login

compaction-stress -c config.yaml -d 12h
```

## Architecture

- **kgo-verifier** — one process per topic, produce-only mode with configurable
  rate limiting and key cardinality
- **Python orchestrator** — config, topic setup (via rpk), launching kgo-verifier
  processes, polling `/status` for stats, periodic reporting, graceful shutdown
- **Offset tracker** — background consumer counts records per partition to measure
  compaction progress (disable with `--no-offset-tracker` on BYOC)
- **Iceberg tracker** — reads table metadata directly from GCS bucket via `gcsfs`,
  reports row counts, file counts, and snapshot versions
