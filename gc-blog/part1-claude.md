## Why Not Just Track Every Object?

- naive approach: index every L0 object, ref count, delete when count hits zero
- thousands of L0s per second across a cluster — bookkeeping state becomes enormous, must itself be durable and replicated
- L0 objects span multiple partitions, led by different nodes — ref counting requires cross-node coordination
- coordination means round trips, retries, partial failures; leadership changes and restarts leave ref counts ambiguous
- per-object granularity: every object is a unique decision point, surface area grows linearly with object count
- want coarse-grained batch safety decisions, not per-object tracking

## Sliding Window Motivation

- each partition needs monotonically increasing epoch in its log
- first take: single max epoch, fence off anything below it
- worked for slow uploads, leadership transfers, stale caches
- stress testing: too aggressive — epoch transitions aren't instantaneous, rejecting everything below max caused unnecessary write failures
- loosened to a window `[prev, max]`, only the floor must increase monotonically
- tumbling advance: new epoch arrives → previous max becomes lower bound, new epoch becomes upper bound
- allows ingesting current epoch while draining previous epoch stragglers concurrently
- safety argument unchanged elsewhere


## Reference Counting?

Consider an L0 object `O` comprising data from partitions `{p0, p1, p3}`. To a first approximation, it resembles any other shared, read-only resource. We can think of each chunk of unreconciled data as a "reference" to the object, and the object is safe to delete only once the number of references goes to zero (i.e. the reconciler has lifted all the enclosed data to L1). Simple enough, but this framing belies an ocean of complexity.

First of all, we have to store these reference counts somewhere. Counts must be durable, so imagine an index service built on a Redpanda topic `Tindex`. When the reconciler, working on `p0`'s leader, needs to decrement the count on `O`, that decrement operation must make its way to `Tindex`’s leader, which might be a different shard or (more likely) a different node altogether.

Now consider how the reconciler itself makes progress, relying on state stored in each partition's Raft log to know where to start working. In this reference counting scheme, the reconciler must make two updates after processing a chunk of `O`: advance the per-partition Raft state AND decrement the ref count for `O`. But what if the Raft update is accepted and the decrement operation fails? More state, more coordination, more edge cases.

This is a good opportunity to mention that Redpanda does not track L0 objects this way. Instead, we assign an "epoch" to every object in L0 (think of it as a coarse-grained logical timestamp - only increasing, non-unique) and leverage carefully structured per-partition state to construct a global view of which L0 objects are safe to remove. No central index, no shared state, and no coordinated updates.

## Sliding Window Motivation

`M(p)` must satisfy two key invariants:

1. `M(p)` must increase monotonically
2. ``


