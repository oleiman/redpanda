# Epoch Barrier PoC

PoC: drive Cloud Topics L0 GC from the cluster-wide epoch barrier
(`gc_safe_epoch`) instead of the per-partition inactive-epoch fence
(`estimate_inactive_epoch`). Motivation: a cluster-wide epoch is easier to
reason about than the sliding-window fence and is likely required for
leaderless producers. The fence can stay underneath; removing it is out of
scope. Full design record: `~/co/notes/epoch-barrier/status.md`.

## Checklist to full PoC

- [x] GC consumes `gc_safe_epoch` (exclusive, via `prev_cluster_epoch`) instead
      of `estimate_inactive_epoch` -- `cluster/health build_partition_status`.
- [x] `replicate_at_offset` (cluster-link / mirror ingest) takes an
      `inflight_write_token`, so the barrier drain sees those L0 writes.
- [x] Full `l0_gc_test.py` suite passes under barrier-driven GC (S3 + ABS):
      idle housekeeping, data integrity, resilience, stress, safety-block,
      all-topics-deleted. No liveness stall.
- [x] `replicate_at_offset` GC-safe-epoch vs local window (traced + fixed
      2026-07-21). It fences via `fence_epoch` / `epoch_in_window`
      (frontend.cc:1496), so not a bypass -- but the fence admits at `max_seen`
      and `gc_safe_epoch` promotion was unclamped, so a mirror-target partition
      whose `max_seen` lagged the cluster epoch could have a fresh write at
      `max_seen < E` collected before it reconciled. Fixed by clamping
      `get_gc_safe_epoch()` to `max_applied`; `l0_gc` suite green.
- [x] tiered_cloud read floor (verified 2026-07-24, no code change). Feared
      barrier-driven GC could delete an L0 object a local read still resolves to
      after a cloud->tiered_v2 flip. Not barrier-specific: `gc_safe_epoch` and
      the fence's `estimate_inactive_epoch` are both LRLO-gated and floor-
      independent, and `f8ec01f731`'s reconciler floor-advance protects both --
      promotion waits for LRLO past the command offset, by which point the
      floor covers those placeholders (the clamp above covers post-command
      writes). Confirmed by `EndToEndCloudTopicsStorageModeToggleTest`
      `.test_toggle_storage_mode` (flip + aggressive GC + full-log read) green
      under barrier GC.
- [ ] liveness verification: 3-node and crash-liveness never converged in TLC;
      P-language / ducktape route.
