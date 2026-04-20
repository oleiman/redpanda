# Level Zero Garbage Collection

In any system, garbage collection must strike a balance between safety, timeliness, and scale. Waiting too long can incur unmanageable storage costs. Act too soon and you risk deleting customer data. Read on to learn how Redpanda tracks the flow of temporary objects through the Cloud Topics system and decides when they are safe to remove.

In a [previous post](https://www.redpanda.com/blog/cloud-topics-architecture) we introduced the architecture of Redpanda Cloud Topics. We covered how incoming data is batched into Level Zero (L0) objects, how metadata is persisted through the same Raft layer as non-cloud topics data, and how that data is reorganized into read-optimized Level One (L1) objects by the reconciler.

These L1 objects become the source of truth for historical reads. They are larger and offer per-partition spatial locality that's not available in the smaller, temporally batched L0 objects. The L0 objects themselves are temporary by design and should be removed as soon as the reconciler finishes with them but **no sooner**. After all, the simplest form of garbage collection routes everything to `/dev/null`. 

## Reference Counting?

Consider an L0 object `O` comprising data from partitions `{p0, p1, p3}`. To a first approximation, it resembles any other shared, read-only resource. We can think of each chunk of unreconciled data as a "reference" to the object, and the object is safe to delete only once the number of references goes to zero (i.e. the reconciler has lifted all the enclosed data to L1). Simple enough, but this framing belies an ocean of complexity.

First of all, we have to store these reference counts somewhere. Counts must be durable, so imagine an index service built on a Redpanda topic `Tindex`. When the reconciler, working on `p0`'s leader, needs to decrement the count on `O`, that decrement operation must make its way to `Tindex`’s leader, which might be a different shard or (more likely) a different node altogether.

Now consider how the reconciler itself makes progress, relying on state stored in each partition's Raft log to know where to start working. In this reference counting scheme, the reconciler must make two updates after processing a chunk of `O`: advance the per-partition Raft state AND decrement the ref count for `O`. But what if the Raft update is accepted and the decrement operation fails? More state, more coordination, more edge cases.

This is a good opportunity to mention that Redpanda does not track L0 objects this way. Instead, we assign an "epoch" to every object in L0 (think of it as a coarse-grained logical timestamp; only increasing, non-unique) and leverage carefully structured per-partition state to construct a global view of which L0 objects are safe to remove. No central index, no shared state, and no coordinated updates.

## Cluster Epochs

Derived from an offset in a central Raft log and accessible globally, the *cluster epoch* is a monotonically increasing counter that we embed in every L0 object ID at creation time. Since the epoch is updated periodically and only ever increases, for any given epoch `E` must eventually age out of the cluster. Once all objects with epoch `E` have been reconciled, it stands to reason that any L0 object with that epoch can be safely deleted. This is the observation upon which the rest of the GC system is built.

So we are left with a somewhat more refined goal: find an epoch `M` that is safe to GC. But this is still non-trivial. Redpanda is a distributed system. For example, if a partition's leadership changes, ingress might resume on a different node whose cached epoch could be out of date.

## Per-partition Epoch Tracking

Claim: Given an oracle `M(p)` that gives the safe-to-GC epoch for any cloud topic partition, we can construct an aggregate `M` that is globally safe-to-GC.

An informal proof follows by induction:

- Without loss of generality, assume `M(p') != M(p'')` for any two partitions.  
- Base case: If we have exactly one partition `p0`, then `M = M(p0)`.  
- Inductive step: Add another partition `p1`. Either `M(p0) < M(p1)` or `M(p0) > M(p1)`. It follows that `M = min(M(p) for p in {p0,p1})`.  
- Therefore `M = min(M(p) over p in P)` where `P` is the set of all cloud topic partitions.

This is an intuitive result: once an epoch is inactive everywhere, it is safe for GC anywhere. So all we need is a source of truth `M(p)` and some mechanism to apply a global minimum to all cloud topic partitions, and we can derive the desired global `M`.

## The Sliding Window

Short of coordinating in-progress writes at the cluster level, we should be able to track each partition's safe-to-GC epoch in the Raft log itself. To support the lazy aggregation scheme described above, the result should be both monotonic and *always* valid.

Our initial design tracked a single epoch, the max across all produced placeholder batches, and fenced off anything older on the replication path. This trivially supports both invariants, but it's too strict in practice. If partition leadership moves to a node with a stale epoch cache, we will fence off every new write until cache expiry, which could be minutes away. Not ideal.

![][image1]

Instead we can bake this epoch lag right into the algorithm. On each partition we maintain a sliding window of active epochs. When we see a new epoch for the first time, slide the window forward. We still get monotonicity by construction, but we gain some flexibility to accept writes that were in flight when the window moved.

![][image2]  
Each cloud topic partition maintains this sliding epoch window through a dedicated replicated state machine embedded in the partition’s Raft log. The most important bits of state are as follows:

| Field | Advance |
| :---- | :---- |
| `max_applied_epoch` | when a strictly greater epoch is committed |
| `previous_applied_epoch` | when we apply a new `max_applied_epoch` |
| `min_epoch_lower_bound` | when reconciler catches up to `max_applied_epoch` |

As discussed, `[previous_applied, max_applied]` describes the range of active epochs we expect to see, and anything below this range is rejected before entering the replication pipeline. `M(p)` is simply `prev(min_epoch_lower_bound)`. 

Note that the computation of `M(p)` is actually a bit stricter than what we described before; that’s because reconciler progress gives the final word on which epochs are safe to delete. So while the window itself slides forward as soon as a new epoch appears, we only advance the safe epoch once we’re sure all the L0 data up to that point has been reconciled into L1.

## Clusterwide Aggregate `M`

Now that every cloud topic partition `p` tracks an inactive epoch in its own Raft log, all that’s left is to combine these into a single, global `M`. This is actually trivial if we piggyback the value on Redpanda’s existing per-partition health reporting service, which is updated periodically and accessible from any shard.

Cluster health reports can become stale (updating them too frequently would be prohibitively expensive), but that’s fine. A nice side effect of epoch monotonicity is that once we prove some `M` is safe, it never becomes unsafe. Every epoch `<M` is gone forever. Or until int64 rollover.

## Formal Verification

Even in this somewhat simplified form, the safety argument for `M(p)` itself is not completely intuitive. Enumerating all the edge cases and failure modes would be too lengthy for a blog post. So, short of a hand-written proof, we built (and exhaustively checked) a TLA+ model of the epoch window algorithm in isolation.

Rather than modeling the write path directly, our model describes forward progress with respect to a partition's Last Reconciled Log (LRO), a real implementation detail of the underlying Cloud Topic state machine that lets us track the reconciler's progress as it moves across the metadata batches stored in a given partition.

L0 objects may contain data from several partitions; our model supports this, giving each partition its own fully independent epoch window, along with support for leadership changes and shared epoch caches (just like in a real cluster). The model checks the following invariants across hundreds of millions of possible states:

| Invariant | Meaning |
| :---- | :---- |
| SafetyInactiveEpochEstimate | If a partition claims epoch `E` is safe, reconciler has advanced past every placeholder with epoch `E` |
| SafetyFloorMonotone | No partition's `min_epoch_lower_bound` ever moves backwards |
| SafetyClusterWideGC | The global GC epoch (`M`) is safe for every partition |

As with any modelling task, we shouldn’t blindly accept any soundness claim. The model itself may be incomplete, or the checker scenario insufficient to surface every possible failure mode. However, we think the inclusion of formal methods is the cherry on top of the extensive validation efforts carried out by the Redpanda Storage and QE teams, not to mention a carefully tested implementation.

## Wrapping Up

With per-partition epoch tracking we've laid down the backbone of L0 Garbage Collection, but that's only part of the story. Once we know which epochs are safe to delete, we have to go and delete them. Stay tuned for Part 2 where we discuss how the design of the garbage collector itself allows us to continually delete thousands of L0 objects without any locally persistent state, explicit coordination, or wasted work.  



