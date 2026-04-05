# Under the hood: garbage collection in Cloud Topics

## Introduction
[Why L0 GC matters. Brief recap of the L0/L1 tiering model for readers
familiar with the Cloud Topics architecture blog. Frame the two problems
GC must solve: knowing *what* is safe to delete, and *how* to delete it
efficiently at scale.]

## Pillar 1: Determining the safe-to-collect epoch

### Epochs and the object namespace
[Every L0 data object carries a cluster epoch in its name. This is the
primitive that makes epoch-based GC possible. Brief on how epochs are
assigned and how the naming scheme supports ordered listing.]

### The sliding window: per-partition epoch tracking
[The applied window (`[previous_applied_epoch, max_applied_epoch]`),
how it advances when new placeholders land, and why only strictly-higher
epochs slide it. The role of `min_epoch_lower_bound` as a conservative
floor.]

### Reconciliation advances the floor
[How the reconciler's progress (LRLO) gates floor advancement. The key
invariant: the floor only moves forward once the reconciler has processed
everything up to the window-transition offset.]

### The GC estimate
[`estimate_inactive_epoch()` = `prev(min_epoch_lower_bound)`. Why the
one-epoch safety margin exists. How conservatism here is a feature.]

### Write-path fencing
[The seen window and its role in preventing stale-epoch writes from
entering the replication pipeline. Leadership change handling.]

### Cluster-wide aggregation
[Per-partition estimates surfaced via health reports. The
`max(across replicas)` then `min(across all partitions)` reduction.
A single stale partition blocks the cluster.]

### Idle partition handling
[Why idle partitions would stall GC indefinitely without intervention.
The housekeeper's `advance_epoch_cmd` + `sync_to_next_placeholder`
mechanism to unstick them.]

## Pillar 2: The garbage collection machinery

### How L0 GC works, end to end
[Visual/conceptual summary of the full GC pipeline with gc-round.svg.
Now that the reader understands how the safe epoch is computed, walk
through a collection round.]

### Design goals
[Stateless, incremental, distributed across shards. No additional
internal topic for tracking GC progress.]

### Exploiting lexicographic ordering
[L0 object names are epoch-prefixed so that lexicographic listing order
= epoch order. This lets GC be stateless: deleted objects vanish from
future listings, and the next listing starts where the last left off.]

### The list/delete pipeline
[Two-stage pipeline: list pages of objects, filter by epoch and age
eligibility, submit eligible objects to an async delete worker.]

### Prefix partitioning across shards
[How the [0,999] prefix space is divided across all shards in the
cluster so every core participates in GC. `compute_prefix_range` and
how it ensures no gaps or overlaps.]

### The deletion grace period
[Why objects aren't deleted immediately upon epoch eligibility. The
age-based filter as a recovery safety net. How `age_backoff` computes
precise sleep times.]

### Adaptive backoff
[How the worker loop adapts its polling interval to collection outcomes:
fast when making progress, slow when blocked by epoch or age, very slow
when storage is empty.]

### Safety gating
[The safety_monitor interface: GC pauses automatically when the cluster
is unhealthy, independent of operator start/pause. Why this matters for
avoiding cascading failures.]

## Looking ahead
[Placeholder for forward-looking content. Potential topics: the epoch
barrier protocol as a successor to health-report-based aggregation.]
