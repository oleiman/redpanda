# Under the Hood: Garbage Collection in Cloud Topics

In a [previous post](https://www.redpanda.com/blog/cloud-topics-architecture),
we described the Cloud Topics architecture: how producer data is batched into L0
objects and uploaded to object storage, how metadata flows through Raft, and how
the reconciler continuously reorganizes L0 data into optimized L1 files for
efficient historical reads.

What happens to L0 objects after the reconciler is done with them? At
1 GiB/s ingress the write path generates ~300 objects per second — they
need to be cleaned up, but deleting too early means data loss.

L0 GC breaks down into two subproblems:

1. Determining which objects are safe to delete.
2. Deleting those objects in a timely, efficient manner, at scale.

## Determining the safe-to-collect epoch

### Epochs and the object namespace

Every L0 data object is stamped with a *cluster epoch* — a monotonically
increasing cluster-global counter. The epoch appears directly in the
object's storage key:

    level_zero/data/{prefix}/{epoch}/{uuid}

for example:

    level_zero/data/042/000000000000000099/a1b2c3...

### The sliding window: per-partition epoch tracking

<!-- epoch-fence-diagram.svg goes here -->

Each cloud topic partition tracks which epochs are safe for GC using a
sliding window. The window is defined by the following per-partition state:

| Field | Meaning | Advances when |
|-------|---------|---------------|
| `max_applied_epoch` | highest committed placeholder epoch | strictly higher epoch is committed |
| `previous_applied_epoch` | former `max_applied_epoch` | window slides forward |
| `min_epoch_lower_bound` | floor — writes below this epoch are rejected | reconciler catches up to the window |

The window `[previous_applied_epoch, max_applied_epoch]` represents the
range of epochs currently active on this partition. Same-epoch placeholders
do not slide the window.


### Reconciliation advances the floor

Once the reconciler catches up to the window, everything below it is
in L1 and `previous_applied_epoch` becomes the new floor. The
[write-path fence](#write-path-fencing) rejects epochs below the floor,
so no new data can land there.

### The GC estimate

Each partition's GC estimate is simply:

    estimate_inactive_epoch = prev(min_epoch_lower_bound)

The floor is the lower bound of the active window, which means the
[write-path fence](#write-path-fencing) still accepts new writes at that epoch.
So while all *existing* data at the floor epoch has been reconciled, new data
could still arrive.

### Write-path fencing

<!-- write-fence-flowchart.svg goes here -->
<!-- write-fence-examples.svg goes here -->

The applied window is persisted and survives leadership changes. But
between entering the replication pipeline and being committed, a write's
epoch is in flight. If a stale epoch were replicated, it could violate
the assumption that all epochs below the floor have been reconciled.

The *seen window* prevents this. It's a volatile, leader-only range of
epochs currently in the pipeline. Writes within or above the window
proceed (the window slides forward as needed); writes below it are
rejected. On leadership change, the seen window resets to the persisted
applied window.

### Cluster-wide aggregation

Per-partition estimates are surfaced through the health reporting system.
The GC epoch source reduces them to a single cluster-wide safe epoch M:
the minimum across all partitions. Any partition that hasn't finished
reconciling holds back the entire cluster.
