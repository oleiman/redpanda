# L0 GC machinery: how the code works

Reference notes for blog drafting. Not blog text.

## Object naming and the prefix/epoch namespace

Every L0 data object has an `object_id` with three components: a random
3-digit prefix (0-999), a cluster epoch (18-digit zero-padded int64), and
a UUIDv4 name. The on-disk path is:

    level_zero/data/{prefix:03d}/{epoch:018d}/{uuid}

e.g. `level_zero/data/042/000000000000000099/abcdef...`

The prefix is assigned randomly at creation time. It serves two purposes:
(1) spreading objects across the prefix keyspace so that GC listing and
deletion can be partitioned across shards, and (2) avoiding object storage
hot-spotting on the cloud provider side. The epoch is the cluster epoch at
the time the object was created. Lexicographic ordering of the path means
listing returns objects in (prefix, epoch) order within each prefix.

## GC at a glance

Each GC round has two steps:

1. Determine the maximum epoch M that is safe to collect.
2. List objects and delete those with epoch <= M (subject to age filter).

## Determining the safe epoch (epoch_source)

The `epoch_source` interface has one key method: `max_gc_eligible_epoch()`.
The default implementation (`epoch_source_impl`) works as follows:

### Step 1: snapshot partitions from the topic table

Query the topic table for all cloud topic partitions. Record the
controller STM's last applied offset as the snapshot revision (a
`cluster_epoch`). This revision is the ceiling: the result can never
exceed it.

### Step 2: collect per-partition GC epochs from health reports

Query the health monitor for the cluster health report. Each partition
replica reports a `cloud_topic_max_gc_eligible_epoch` field. For each
partition, take `max(across all replicas)` as the most optimistic safe
value. (Rationale: reported values are valid at and forever after the
moment they are reported; leadership is a lagging signal; max is the
tightest correct bound.)

If all replicas report no epoch for a partition, that partition is omitted
from the result. This covers bootstrapping (the partition hasn't
initialized its STM yet) and non-cloud-topics.

### Step 3: reduce to a single cluster-wide epoch

```
result = snapshot_revision   // ceiling
for each partition P in the snapshot:
    if P not in health_report_epochs:
        return error  // partition exists but has no reported epoch
    result = min(result, health_report_epochs[P])
return result
```

A single partition with a stale or missing epoch holds back the entire
cluster. This is conservative by design.

## The cluster epoch service

The cluster epoch service (`cluster_epoch_service`) is a sharded service
that provides a monotonically increasing cluster-wide epoch to writers.
The epoch is derived from the committed offset of the controller log
(raft0), so it can be correlated with cluster-level operations like
partition creation.

The raft0 leader periodically "freezes" the epoch by replicating an empty
checkpoint batch to the controller log. The committed offset of that batch
becomes the new epoch. This is done on a configurable interval
(`cloud_topics_epoch_service_epoch_increment_interval`) so that random
cluster operations don't cause unpredictable epoch churn.

Non-leader shards cache the epoch locally. The cache has two staleness
thresholds:
- Expired: triggers a background refresh (non-blocking).
- Needs update: blocks callers until the epoch is refreshed from shard0
  (which in turn fetches from the raft0 leader, possibly via RPC).

### Cache invalidation

The write path can invalidate the epoch cache when it detects a
monotonicity violation. This happens in two places in
`frontend.cc`:

1. Before uploading: the frontend computes a `min_epoch` from the
   partition's current seen/applied window. If the cached epoch is below
   this minimum, `invalidate_epoch_below(min_epoch)` forces a cache
   refresh across all shards.

2. After a failed epoch fence: if the partition rejects the batch's epoch
   as below its window, the frontend invalidates the cache on the upload
   shard specifically.

The invalidation is guarded: you must pass the epoch that caused the
violation, and if the local cache has already advanced past it the call
is a no-op. This prevents invalidation storms.

## The per-partition epoch fence algorithm

(This is the mechanism that produces the per-partition
`cloud_topic_max_gc_eligible_epoch` value that health reports carry.
Described in `docs/plans/epoch_fence_and_window/epoch-fence-algorithm.md`
and implemented in `ctp_stm_state`.)

Each partition maintains a sliding window of applied epochs:

- `[previous_applied_epoch, max_applied_epoch]`: tracks committed
  placeholder epochs
- `min_epoch_lower_bound`: conservative floor, advanced by the reconciler
- `current_epoch_window_offset`: log offset where `max_applied_epoch` last
  changed

The window slides when a strictly higher epoch is committed. The floor
advances when the reconciler's last reconciled log offset (LRLO) passes
`current_epoch_window_offset`, proving that all placeholders at the
previous epoch have been reconciled to L1.

The estimate: `estimate_inactive_epoch() = prev(min_epoch_lower_bound)`.
One-epoch safety margin: the floor itself may still have inflight data.

Write-path fencing (the "seen window") prevents stale epochs from entering
the replication pipeline. Resets on leadership change; falls back to the
persisted applied window.

Idle partitions: the housekeeper writes `advance_epoch_cmd` +
`sync_to_next_placeholder` to slide the window and advance LRLO, unblocking
the floor.

## The GC worker loop

`level_zero_gc_t::worker()` is a coroutine that runs for the lifetime of
the GC instance:

```
loop:
    wait until should_run_ or should_shutdown_
    if shutdown: exit

    if safety_monitor says not ok:
        sleep(throttle_no_progress), continue

    if skip_backoff_: clear it; else sleep(backoff)

    outcome = try_to_collect()

    backoff = f(outcome):
        progress      -> throttle_progress (short)
        at_capacity   -> throttle_progress
        epoch_inelig  -> throttle_no_progress (longer)
        age_inelig    -> exact time until oldest object ages out
        empty         -> deletion_grace_period (longest)
        error         -> throttle_no_progress

    backoff += jitter(backoff)  // +[0, 10%], capped at 2min
```

The worker can be started, paused, reset, and stopped. Reset drains
in-flight work and clears pagination state without a full stop/start.

## Collection: try_to_collect and do_try_to_collect

`try_to_collect()` drives pages through the delete worker until either all
prefixes are exhausted or the delete worker is at capacity:

```
while delete_worker has capacity:
    page_outcome = do_try_to_collect(cached_max_gc_epoch)
    if no more pages: break
    merge page_outcome into overall outcome
    if page had no eligible objects and we're not tracking age: break
    else: sleep briefly between pages (jittered)
```

`do_try_to_collect()` processes a single page:

1. `delete_worker_->next_page()` to get a page of listed objects.
2. If `max_gc_epoch` not yet computed this round, call
   `epoch_source_->max_gc_eligible_epoch()` once and cache it.
3. For each object in the page:
   - Parse epoch and prefix from object key.
   - Detect non-lexicographic ordering (log error if listing is unsorted).
   - If `object_epoch > max_gc_epoch`: skip, mark epoch_ineligible.
   - If `object.last_modified > now - grace_period`: skip, mark
     age_ineligible, track oldest ineligible timestamp.
   - Otherwise: add to eligible set.
4. Submit eligible objects to `delete_worker_->delete_objects()`.

## The list/delete worker

`list_delete_worker` is an inner class that owns the object_storage and
drives listing and deletion.

### Prefix iteration

On each `next_page()` call:
1. If a continuation token exists from a previous truncated listing,
   continue listing the current prefix.
2. Otherwise, get the next prefix from `prefix_compressor`.
3. If all prefixes exhausted, return empty (signals end of round).

The prefix_compressor is initialized from `compute_prefix_range()` which
assigns this shard its slice of [0,999]. The trie compresses the range
into minimal listing prefixes (e.g. shard owning [100,199] lists with
just prefix "1" instead of 100 individual prefixes).

### Listing

`do_next_page()` calls `storage_->list_objects(prefix, continuation_token)`
with max_keys=1000. If the result is truncated, caches the continuation
token for the next call. If a page returns no objects, moves to the next
prefix.

### Deletion

`delete_objects()` takes a batch of eligible objects and submits them to
an `ssx::work_queue`. The actual delete runs asynchronously: it acquires
a delete semaphore unit (max 5 concurrent deletes), then spawns the
`storage_->delete_objects()` call inside a gate.

## Prefix partitioning: compute_prefix_range

Distributes the [0,999] prefix space across all shards in the cluster:

```
total_shards = sum of cores across all nodes
shard_idx = sum of cores on nodes with id < self + this_shard_id

stride = 1000 / total_shards
remainder = 1000 % total_shards
width = stride + (shard_idx < remainder ? 1 : 0)
min = shard_idx * stride + min(shard_idx, remainder)
max = min + width - 1
```

Example: 10 total shards -> each gets 100 prefixes. 9 total shards ->
first shard gets [0,111] (112 prefixes), last gets [888,999].

Every shard on every node runs its own `level_zero_gc` instance, each
responsible for its slice of the prefix space.

## Prefix compression (trie)

The prefix_compressor uses a trie to compress a contiguous range of
3-digit prefixes into a minimal set of listing prefixes:

- [0, 999] -> "" (empty string, matches everything)
- [100, 199] -> "1"
- [0, 12] -> {"00", "010", "011", "012"}
- [89, 300] -> {"089", "09", "1", "2", "300"}

This minimizes the number of LIST requests needed to cover a shard's
prefix range. The trie inserts all 3-digit values in the range, then
prunes saturated subtrees (where all children are present) into shorter
prefixes.

## Safety monitor

`cluster_safety_monitor` polls the health monitor in a background loop.
`can_proceed()` returns a cached bool. If the cluster is unhealthy, GC
pauses regardless of operator start/pause state. This prevents GC from
removing data during incidents where the cluster might not be fully
functional.

## State machine

States: paused, running, safety_blocked, resetting, stopping, stopped.

- Construction -> paused
- start() -> running (or safety_blocked if monitor says no)
- pause() -> paused
- reset() -> drains in-flight work, clears prefix/pagination state,
  resumes if was running
- stop() -> stopping -> stopped (terminal)

`skip_backoff_` flag ensures the first round after start() or reset()
runs immediately without waiting.

## Config knobs

- `deletion_grace_period`: minimum age before an epoch-eligible object
  can be deleted (recovery safety net)
- `throttle_progress`: sleep between rounds when making progress
- `throttle_no_progress`: sleep between rounds when nothing to do
- `cloud_topics_gc_health_check_interval`: how often the safety monitor
  polls cluster health

## Why the random prefix matters: cloud provider object sharding

Cloud object stores internally partition (shard) their key index to spread
load across backend servers. The partition boundaries are derived from the
lexicographic structure of object keys. When many objects share a common
prefix and are written or listed at high rates, they land on the same
internal partition, creating a "hot spot" that triggers throttling. The
random 3-digit prefix at the front of every L0 path is the primary
defense against this.

### AWS S3

S3 partitions its key index by prefix. Each partitioned prefix supports
3,500 PUT/COPY/POST/DELETE and 5,500 GET/HEAD requests per second. S3
automatically detects sustained high request rates on a prefix and
re-partitions behind the scenes, but this takes 30-60 minutes and you
may see 503 Slow Down errors during the transition. Before 2018, AWS
explicitly recommended prepending a random hash to object keys to avoid
hot partitions. In 2018 they announced automatic prefix partitioning and
dropped that guidance — but the underlying mechanics haven't changed: a
workload that concentrates all keys under a single prefix still starts
with one partition's worth of throughput and must wait for auto-scaling.
Spreading keys across prefixes up front avoids the ramp-up entirely. With
1000 prefixes the theoretical aggregate is 3.5M PUT/s and 5.5M GET/s.

### Google Cloud Storage

GCS starts each bucket at ~1,000 writes/s and ~5,000 reads/s and
auto-scales upward. It detects hotspotting (writes concentrated in a
narrow lexicographic range) and redistributes load, but this takes on the
order of minutes. Sequential key names (timestamps, counters) make
redistribution harder because the hot range keeps shifting. Google's
guidance is to prepend a hash prefix; even a single random hex character
enables scaling to ~80,000 reads/s and ~16,000 writes/s. The L0 prefix
space (000-999) provides ~3 hex-equivalent digits of entropy, well above
this threshold.

### Azure Blob Storage

Azure partitions blobs by their full name (container + blob name). A
single partition supports ~500 requests/s per blob and up to 20,000
requests/s per storage account. Sequential or timestamp-prefixed names
concentrate traffic on one partition server, triggering 503 Server Busy
errors. Azure's guidance mirrors S3/GCS: prepend a hash or random prefix
to distribute load. Unlike S3, Azure does not publicly document automatic
re-partitioning on the same timeline, making up-front key distribution
more important.

### What this means for L0 GC

L0 object creation rate scales proportionally with ingress. Without the
prefix, all objects and GC LIST/DELETE requests would land under a single
key prefix. The random 3-digit prefix fans them across 1,000
sub-namespaces, keeping per-prefix request rates well within provider
limits and letting GC issue parallel LIST requests across shards without
contending on the same backend partition.
