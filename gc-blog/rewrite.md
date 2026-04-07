# Level Zero Garbage Collection

In a [previous post](https://www.redpanda.com/blog/cloud-topics-architecture) we introduced the architecture of Redpanda Cloud Topics. We covered how incoming data is batched into Level Zero (L0) objects, how metadata is persisted through the same Raft layer as non-cloud topics data, and how eventually that data is reorganized into read-optimized Level One (L1) objects by the reconciler.

These L1 objects become the source of truth for historical reads. They are larger and offer per-partition spatial locality that's not available in the smaller, temporally batched L0 objects. A fully reconciled L0 object is 100% junk. Garbage. Frass. At 1GiB/s ingress we accumulate ~300 such objects every second, and we should remove them as soon as possible after the reconciler finishes with them.

To that end, the design of L0 Garbage Collection must balance safety (never delete an unreconciled object), timeliness (don't waste storage costs on useless objects), and scale (don't tank the cluster spinning on tiny cloud requests).

## Cluster Epochs

Derived from an offset in a central Raft log and accessible globally, the *cluster epoch* is a monotonically increasing counter that we embed in every L0 object ID at creation time. Since the epoch is updated periodically and only ever increases, for a given epoch `E` there must come a time when no new object will _ever_ be created with epoch `E`. Once all objects with epoch `E` have been reconciled, it stands to reason that any L0 object with that epoch can be safely deleted. This is the observation upon which the rest of the GC system is built.

So we are left with a somewhat more refined goal: find an epoch `M` that is safe to GC. But this is still non-trivial. Redpanda is a distributed system. For example, if a partition's leadership changes, ingress might resume on a different node whose cached epoch could be out of date.

## Per-partition Epoch Tracking

Claim: Given an oracle `M(p)` that gives the safe-to-GC epoch for any cloud topic partition, we can construct an aggregate `M` that is globally safe-to-GC.

An informal proof follows by induction:

- Without loss of generality, assume `M(p') != M(p'')` for any two partitions.
- Base case: If we have exactly one partition `p0`, then `M = M(p0)`.
- Inductive step: Add another partition `p1`. Either `M(p0) < M(p1)` or `M(p0) > M(p1)`. It follows that `M = min(M(p) for p in {p0,p1})`.
- Therefore `M = min(M(p) over p in P)` where `P` is the set of all cloud topic partitions.

This allows us to refine our goal even further. All we need is a trusted oracle `M(p)` and some mechanism to apply a global minimum to all cloud topic partitions, and we can derive the desired global `M`. A nice side effect of epoch monotonicity is that the worst `M` can ever be is stale. Once we conclude that some `M` is safe, it never becomes unsafe. Every epoch `< M` is gone forever. Or until int64 rollover.

## The Sliding Window

Each cloud topic partition maintains a persistent sliding epoch window from which we can derive `M(p)`. The most important bits of state are as follows:

| Field | Meaning | Advance |
| ----- | ------- | ------- |
| `max_applied_epoch` | highest replicated epoch | when a strictly greater epoch is committed |
| `previous_applied_epoch` | last value of max | when we apply a new `max_applied_epoch` |
| `min_epoch_lower_bound` | minimum _acceptable_ epoch | reconciler catches up to `max_applied_epoch` |

Essentially `[prev, max]` represents the range of active epochs we expect to see. Anything below `lower_bound` is rejected _before_ entering the replication pipeline. This should occur rarely if we keep epoch caches up to date, but it is a necessary safeguard in a distributed system where communication disruptions are the norm.

`M(p)` is simply `prev(min_epoch_lower_bound)`. Every replicated L0 placeholder up to this point has been fully reconciled with respect to `p`, and any such placeholders still in flight will be fenced off on the write path.


## Formal Verification

Even in this somewhat simplified form, the safety argument for the oracle itself is not completely intuitive. Enumerating all the edge cases and failure modes would be too lengthy for a blog post. So, short of a hand-written proof, we built (and exhaustively checked) a TLA+ model of the epoch window algorithm in isolation.

Rather than modeling the write path directly, our model describes forward progress with respect to a partition's Last Reconciled Log Offset (LRLO), a real implementation detail of the underlying Cloud Topic state machine that lets us track the reconciler's progress as it moves across the metadata batches stored in a given partition.

L0 objects may contain data from several partitions; our model supports this, giving each partition its own fully independent epoch window, along with support for leadership changes and shared epoch caches (just like in a real cluster). The model checks the following invariants across hundreds of millions of possible states:

| Invariant | Meaning |
| --------- | ------- |
| SafetyInactiveEpochEstimate | If a partition claims epoch `E` is safe, LRLO has advanced past every placeholder with epoch `E` |
| SafetyFloorMonotone | No partition's `min_epoch_lower_bound` ever moves backwards |
| SafetyClusterWideGC | The global GC epoch (`M`) is safe for every partition |

As with any modelling task, our result comes with the usual caveats. The model itself may be incomplete, or the checker scenario insufficient to surface every possible failure mode. However, we think the inclusion of formal methods is the cherry on top of the extensive validation efforts carried out by the Redpanda Storage and QE teams, not to mention a carefully tested implementation.

## Wrapping Up

With per-partition epoch tracking we've laid down the backbone of L0 Garbage Collection, but that's only part of the story. Once we know which epochs are safe to delete, we have to go and delete them. This must occur continually, in a loop, with minimal cost and without impacting the performance of the rest of the cluster.

