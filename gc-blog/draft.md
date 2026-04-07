# Under the Hood: Garbage Collection in Cloud Topics

In a [previous post](https://www.redpanda.com/blog/cloud-topics-architecture),
we described the Cloud Topics architecture: how producer data is batched into L0
objects and uploaded to object storage, how metadata flows through Raft, and how
the reconciler continuously reorganizes L0 data into optimized L1 files for
efficient historical reads.

What happens to L0 objects after the reconciler is done with them? At
1 GiB/s ingress the write path generates ~300 objects per second. These
objects are the source of truth for the reconciler, but once an L0 object's
data is fully reconciled it is dead weight.

## Determining the safe-to-collect epoch

### Epochs and the object namespace

Every L0 data object is stamped with a *cluster epoch*, a monotonically
increasing, cluster-global counter, that appears directly in the object's
storage key:

    level_zero/data/{prefix}/{epoch}/{uuid}

for example:

    level_zero/data/042/000000000000000099/a1b2c3...

The following sections explain how we combine this object-level metadata
and the overall bucket listing to make safe decisions about GC and act on
them at scale.

### The sliding window: per-partition epoch tracking

<!-- epoch-fence-diagram.svg goes here -->

Each cloud topic partition tracks a sliding epoch window as follows:

| Field | Meaning | Advances when |
|-------|---------|---------------|
| `max_applied_epoch` | highest committed placeholder epoch | strictly higher epoch is committed |
| `previous_applied_epoch` | former `max_applied_epoch` | window slides forward |
| `min_epoch_lower_bound` | writes below this epoch are rejected | reconciler catches up to the window |

The window `[previous_applied_epoch, max_applied_epoch]` represents the
range of epochs currently active on this partition. Once the reconciler
catches the window's upper bound, `previous_applied_epoch` becomes the
new floor. Anything below that is fenced on the write path  and becomes
safe to GC _with respect to that partition_:

    estimate_inactive_epoch = prev(min_epoch_lower_bound)

### Cluster-wide aggregation

Per-partition estimates are surfaced through the health reporting system and
aggregated by each shard's garbage collector into a minimum across all
partitions. The result is an epoch `M` that is safe to GC, cluster-wide.

### Formal verification

The safety argument for the epoch fence involves the interplay of the sliding
window, reconciler progress, write-path fencing, leadership changes, and stale
epoch caches. To gain confidence beyond hand-written proofs, we built a TLA+
model of the algorithm and exhaustively checked it with the TLC model checker.

Rather than modeling reconciliation, the invariants are stated against LRLO
position: if the fence claims epoch E is safe, LRLO must have advanced past
every placeholder at E. The model allows LRLO to advance in any order, so this
holds for all possible reconciler behaviors.

Since L0 objects can span partitions but are deleted atomically, the model
covers multiple partitions with independent windows, shared epoch caches, and
per-partition leadership changes. TLC exhaustively verified three invariants
across ~443 million distinct states:

1. **SafetyInactiveEpochEstimate.** If a partition claims epoch E is
   safe, LRLO has advanced past every placeholder at epoch E.
2. **SafetyFloorMonotone.** No partition's `min_epoch_lower_bound` ever
   regresses.
3. **SafetyClusterWideGC.** The cluster-wide GC epoch (the minimum
   across all partitions) is safe for every partition simultaneously.

## The garbage collection machinery

<!-- gc-round.svg goes here -->

Given a safe epoch M, the GC machinery needs to find and delete L0 objects at
epochs ≤ M. A modest goal, but there's more than one way to crack an egg. We'll
start with some high level design goals and discuss how they apply to a concrete
implementation.

### Stateless: exploiting lexicographic ordering

A natural approach to GC would be to persist a high-water mark - "I've
cleaned through epoch X" - and resume from there on the next round. This
would require an internal topic or similar metadata store, adding
operational complexity.

L0 GC avoids this entirely. Recall the object key format:

    level_zero/data/{prefix}/{epoch}/{uuid}

The epoch is zero-padded to 18 digits, which means lexicographic ordering
of keys corresponds to epoch ordering. Object stores return listings in
lexicographic order. So when GC lists objects, it naturally sees the
lowest epochs first. Objects deleted in previous rounds no longer appear
in listings. The next listing picks up where the last one left off —
no bookkeeping required.

### Minimal object store traffic

GC issues LIST and DELETE requests against object storage. Both cost money
and are rate-limited by the provider. Some choices that help keep request
volume low:

- **Prefix compression.** Each shard's assigned prefix range is
  compressed into a minimal set of listing prefixes (see below).
- **Batch deletes.**  S3 supports up to 1,000 keys per batch delete.
- **Pagination.** Each LIST returns a page of results with a continuation
  token. Each LIST operation spawns one or more batch DELETEs in the
  background.
- **The random prefix itself** distributes objects across the provider's
  internal partition space, avoiding hot-spotting that would trigger
  throttling.

### Distributed and isolated: prefix partitioning

The three-digit prefix in each object key (the `{prefix}` in
`level_zero/data/{prefix}/{epoch}/{uuid}`) is assigned randomly at
creation time, spreading objects uniformly across 1,000 sub-namespaces.

GC divides this prefix space across every core in the cluster. Each
shard computes its assigned range based on its position in a global
ordering of all shards across all nodes. A shard owning prefixes
[100, 199] only lists and deletes objects under those prefixes - no
coordination with other shards needed.

The prefix ranges are compressed into minimal listing prefixes using a
trie. A shard owning [100, 199] can issue a single LIST with prefix "1"
instead of 100 separate requests. A shard owning [89, 300] needs only
{"089", "09", "1", "2", "300"} - five requests instead of 212.

### No wasted work: adaptive backoff and safety gating

Not every GC round finds something to delete. An object may be at an epochs
above  M, or be younger than the *deletion grace period* - a configurable
minimum age that gives operators a recovery window before objects are 
permanently removed. GC adapts its polling interval to each outcome:

| Outcome | Backoff |
|---------|---------|
| Deleted objects | Short - poll again soon |
| Objects exist but epoch too high | Longer - epoch advancement is out of GC's hands |
| Objects exist but too young | Precise - sleep until the oldest one ages out |
| Nothing listed | Longest - sleep for the full grace period |
| Cluster unhealthy | GC pauses entirely until health is restored |

The health check is independent of operator start/pause - even if an
operator starts GC, the safety monitor can hold it back. This prevents
GC from running during incidents where the cluster may not have an
accurate picture of reconciliation progress.
