# cloud_io::scheduler design notes

> Status: design accompanying the `min_share_policy` PR. Not a
> long-term document; intended to brief reviewers. May or may not
> survive merge.

## Summary

`cloud_io::scheduler` is an admission gate between cloud_io
callers and the shared cloud client pool. It exists to prevent
**producer collapse under sustained consumer load**: a failure
mode where a high-rate consumer drain monopolizes the client
pool and producer throughput collapses (pubRate floor 20% vs
target ≥95%).

The current policy is `min_share_policy`: a two-tier slot
allocator. Each caller group has a reservation lane sized to
`target_reserved`, plus a common pool any group can draw from.
Idle reservations past the dwell window are reclaimed to the
common pool; under-target active groups refill from quiet
common-pool releases. Dispatch prefers under-target waiters with
FIFO as fallback.

## Problem

Three caller groups compete for the cloud client pool:

| Group | Workload | Per-shard demand |
|---|---|---|
| `producer_upload` | L0 batcher S3 PUTs | ~0.5 concurrent, latency-critical |
| `consumer_fetch` | Tiered fetch reads | up to ~16 concurrent during drain |
| `default_group` | Archival, hydration, housekeeping | bursty, latency-tolerant |

During a sustained drain, `consumer_fetch` saturates the pool
with high-latency S3 GETs. `producer_upload` admits arrive at
~3/s against ~250ms ack time, so pu never accumulates a queue
and gets back-of-line treatment behind cf's deep queue. Observed
(null_policy baseline, `ctflex-20260519-164636`):

| t | L0 PUTs/s | producer S3 out | OMB pubRate |
|---|---|---|---|
| consumer-start | 14 /s | 110 MiB/s | 12.2k |
| +90s | 0.2 /s | 2.6 MiB/s | 0 |

pubRate floor 20.2%; SLA target ≥95%.

## Design

Per shard, with cluster client pool capacity `N`:

```
  _groups[g].reserved_sem  per-group reservation lane (target_reserved)
  _shared                  common pool (size = N - sum(target_reserved))
```

`admit(group)` draws from the group's reservation lane first,
then the common pool; on failure, queues a waiter against the
group. `release(group)` returns the slot to the same lane it
came from.

The producer guarantee lives at the semaphore layer, not the
dispatch layer. Pu never races against cf; it draws from its own
lane until full.

### Mechanisms

- **Per-group reservation.** Each group has its own
  `reserved_sem` of size `target_reserved`, reachable only by
  that group.
- **Lane-aware release.** A reserved-lane slot returns to the
  lane; a common-pool slot returns to the common pool (modulo
  refill). Tracked on the in-flight slot.
- **Reclamation on dwell expiry.** A group idle for
  `default_dwell_duration` (5s) returns its idle reserved slots
  to the common pool. Triggered on the next admit.
- **Refill on quiet release.** A common-pool release with no
  queued waiter routes the slot into the most-under-target
  active group's reservation lane.
- **Dispatch.** A common-pool release with queued waiters picks
  the oldest seq from a group below its target_reserved.
  Otherwise picks the global oldest. Together these handle
  starvation and FIFO ordering under saturation.

### Cluster config

| Property | Default | Description |
|---|---|---|
| `cloud_io_scheduler_policy` | `null` | `null` disables admission; `min_share` enables. |
| `cloud_io_scheduler_min_share` | 2 per group | Vector of `{group_name, target_reserved}`. Unknown names ignored with warn. |

With pool capacity 20/shard, defaults give 6/shard reserved and
14/shard common.

## Empirical validation

### Reservation alone passes SLA

`ctflex-20260520-015247`, pu=2/cf=2/default=0, no reclaim or
refill:

| Metric | null baseline | reservation alone |
|---|---|---|
| pubRate floor | 20.2% | **98.1%** |
| pub p99 | 29.6 s | 511 ms |

The reservation lane is decoupled from the dispatch race. Pu
pulls from a dedicated cluster pool that cf cannot touch.

### Reclaim + refill recover dormant slots

`ctflex-20260520-050248`, same reservation config plus
dwell-driven reclaim and common-pool-release refill:

| Metric | reservation alone | + reclaim + refill |
|---|---|---|
| pubRate floor | 98.1% | 98.1% |
| Pool util during build-up | 91-93% | ~98% peak |

SLA holds; cf's reclaimed slots flow back to the common pool
during cf-dormant build-up and refill back to cf when its drain
begins.

### Common pool is load-bearing

`ctflex-20260521-005331`, all-reserved variant (pu=6/cf=7/
default=7, no common pool):

| Metric | min_share (2/2/2) | all-reserved (6/7/7) |
|---|---|---|
| pubRate floor | 98.0% | 98.1% |
| cf in_flight at drain plateau | 96 cluster | **42 (= 7×6)** |
| cf waiters at drain plateau | 10-20 | **~900** |
| consumeRate peak | 54.2k msg/s | 41.8k **(-23%)** |

Under sustained drain, refill never fires (every release goes to
a waiter via dispatch). cf is pinned at its reservation; without
a common pool, peak concurrency collapses to the reservation
size. **The two-tier structure is what lets any group burst
beyond its reservation under sustained pressure.**

## Alternatives considered

### null_policy

No admission control. Producer collapses under consumer drain.
Kept available as `cloud_io_scheduler_policy=null` for
deployments that don't need group isolation.

### Dispatch-time fair queueing

The first design attempt was dispatch-time fairness: a shared
semaphore plus a dispatcher that ranked queued waiters to favor
under-served groups. Four iterations didn't fix the SLA. The
blocking observation: pu's arrival rate (~3/s) against ~250ms
ack time means pu never accumulates a dispatch queue. **No
dispatch-time mechanism can help a group that doesn't enter the
dispatch race.** This motivated moving the guarantee pre-admit,
to the semaphore layer.

### All-reservation, no common pool

See "Common pool is load-bearing" above: validated for SLA, but
loses ~25% peak drain throughput because the common pool is what
lets any single group exceed its reservation under sustained
demand.

## Known limitations

- **Refill doesn't fire during sustained saturation.** Under a
  deep queue every common-pool release routes to a waiter, so
  the refill branch never executes. For the validated workload
  this is fine: reclaimed capacity stays accessible via the
  common pool, just not formally re-attributed to the group's
  reservation.
- **`current_reserved()` is a best-effort gauge.** Invariants
  are eventual, not instantaneous. The accessor is for
  diagnostics; `target_reserved` is the operator-facing contract.
- **3-group taxonomy is hardcoded.** Adding a group requires
  extending `group_id` and bumping `num_group_ids`. Per-group
  containers and the cluster property scale automatically.

## Future work

- Tuning the reservation/common split. Defaults (2/2/2 + 14
  common) are the bench-validated sweet spot; the cluster
  property makes this operator-tunable.
- Per-cluster instead of per-shard reservations. Could improve
  utilization on shard-asymmetric workloads at the cost of
  cross-shard coordination. Not motivated by current data.
