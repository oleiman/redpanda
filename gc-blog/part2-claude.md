# Part 2: The GC Pipeline

## Recap / bridge from Part 1
- Part 1: how we derive M (safe-to-GC epoch) from per-partition windows, health reports, clusterwide min-reduction
- Part 2: given M, how the system actually finds and deletes L0 objects from object storage
- Two subproblems: efficient discovery (which objects?), safe deletion (rate-limited, distributed, graceful)

## Statelessness via lexicographic ordering
- Natural approach: persist a high water mark, resume from there
  - Requires internal topic, custom state machine, operational overhead
- L0 GC avoids all persistent state
- L0 object key structure: `level_zero/data/{prefix:03d}/{epoch:018d}/{uuid}`
  - Epoch zero-padded to 18 digits -- lexicographic order = epoch order within a prefix
  - All major object stores (S3, GCS, ABS) return LIST results in lexicographic order
- Deleted objects vanish from future listings; next LIST picks up where the last left off
- Bucket listing itself is the progress pointer -- no bookkeeping needed

## Prefix partitioning: distributing work across the cluster
- Random 3-digit prefix (000-999) on every L0 object
  - Purpose 1: spread object store backend load (avoid hot-spotting)
  - Purpose 2: partition GC work across shards
- Every shard on every node runs its own `level_zero_gc` instance
- `compute_prefix_range(shard_idx, total_shards)` assigns each shard a contiguous slice of [0,999]
  - Even division with remainder distributed one-per-shard to the first N shards
  - No gaps, no overlaps
  - Example: 10 total shards -> 100 prefixes each; 9 shards -> first gets 112, last gets 112

### Prefix compression (trie)
- Naive: issue one LIST per 3-digit prefix in the range -> up to hundreds of requests
- Trie-based compression: insert all prefixes, prune saturated subtrees into shorter strings
  - [0,999] -> "" (one LIST covers everything)
  - [100,199] -> "1"
  - [89,300] -> {"089", "09", "1", "2", "300"}
- Minimizes LIST requests per shard

## Anatomy of a collection round [gc-round.svg]
- Worker loop is a long-lived coroutine; one per shard
- Each iteration:
  1. Wait for `should_run_` signal (CV)
  2. Check safety monitor -- if cluster unhealthy, sleep and retry
  3. Unless `skip_backoff_`, sleep for computed backoff
  4. Call `try_to_collect()` -- drives pages through the list/delete pipeline
  5. Compute next backoff from outcome

## The list/delete pipeline
- Two-stage: list pages of objects, filter, submit eligible batch to async delete worker

### Listing (next_page)
- Iterates over compressed prefixes from `prefix_compressor`
- For each prefix: LIST with max_keys=1000, cache continuation token if truncated
- Empty page -> advance to next prefix
- All prefixes exhausted -> round complete

### Filtering (do_try_to_collect)
- Epoch queried once per round from `epoch_source`, cached as `max_gc_epoch`
- For each object in the page:
  - Parse epoch and prefix from key
  - Detect non-lexicographic ordering (rate-limited error log)
  - epoch > M -> skip, mark `epoch_ineligible`
  - last_modified too recent (within grace period) -> skip, mark `age_ineligible`, track oldest
  - Otherwise -> eligible, add to batch

### Deletion (delete_objects)
- Eligible batch submitted to `ssx::work_queue` (async)
- Delete semaphore: max 5 concurrent delete ops
- Page semaphore (~1 MiB): bounds memory held by queued eligible pages
- Actual delete: `storage_->delete_objects()` inside a gate
- Decoupled from listing -- listing can race ahead while deletes drain

## Multi-page collection loop (try_to_collect)
- Outer loop: keep calling `do_try_to_collect` while delete worker has capacity
- Page with no eligible objects:
  - If tracking age-ineligible: keep scanning (need full picture for accurate `age_backoff`)
  - Otherwise (epoch-ineligible or empty): stop early
- Jittered inter-page sleep to avoid hammering the object store

## The deletion grace period
- Objects are not deleted the instant they become epoch-eligible
- `deletion_grace_period` config: minimum age before deletion
- Safety net: protects against accidental deletion, gives time for recovery
- `age_backoff`: computes exact sleep until oldest too-young object ages past the threshold
  - Avoids polling -- wake up precisely when the next object becomes eligible

## Adaptive backoff
- Worker sleep between rounds depends on outcome of the last round:
  - `progress` / `at_capacity` -> short (`throttle_progress`)
  - `epoch_ineligible` -> longer (`throttle_no_progress`) -- can't predict when M advances
  - `age_ineligible` -> exact duration until oldest object ages out
  - `empty` -> longest (`deletion_grace_period`) -- nothing in storage
  - error -> `throttle_no_progress`
- Positive jitter added: +[0, 10%] of backoff, capped at 2 min
  - Despreads wakeups across shards

## Safety monitor
- `cluster_safety_monitor`: background poller on cluster health overview
- `can_proceed()` returns cached bool -- synchronous, cheap
- GC pauses automatically when cluster unhealthy, regardless of admin start/pause
- Prevents cascading failures: don't delete data during incidents

## State machine
- States: paused -> running <-> safety_blocked -> stopping -> stopped
- `start()` / `pause()` / `reset()` / `stop()`
- `reset()`: drains in-flight deletes, clears prefix/pagination state, resumes if was running
  - Used when cluster membership changes (prefix ranges shift)
- `skip_backoff_` flag: first round after start() or reset() runs immediately
- start/pause block while a reset is in progress

## Observability
- Per-shard Prometheus metrics via `level_zero_gc_probe`:
  - objects_deleted, bytes_deleted, objects_listed
  - objects_skipped_not_eligible, objects_skipped_too_young
  - collection_rounds, list/delete requests, list/delete errors
  - backpressure_seconds (time spent sleeping)
  - safety_blocked_rounds
  - epoch lag (max_gc_eligible - min_deletion_epoch)
- Rate-limited warnings for non-lexicographic listings

## Cloud provider considerations (sidebar / box)
- Why the random prefix matters at scale:
  - S3: prefix-based partitioning, 3500 PUT/s and 5500 GET/s per partition; auto-scales but takes 30-60 min
  - GCS: starts at ~1000 writes/s, auto-scales, but sequential keys impede redistribution
  - ABS: per-blob partitioning, 500 req/s per blob, 20k/s per account; no public auto-repart docs
- 1000 prefixes keep per-prefix rates well within limits at any realistic ingress rate
