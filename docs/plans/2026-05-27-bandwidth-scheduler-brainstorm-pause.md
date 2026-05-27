# bandwidth_scheduler — brainstorm pause point

> Status: pause-point for the bandwidth_scheduler design conversation.
> Pick this up to resume. Brainstorming was active when paused; the next
> step is the scope question at the bottom of this doc.

## Branch

`ct/core-16225/cloud-io-sched-bytes`, sitting on top of the slot
templating fixup stack. Tip commit `c9394504a1 cloud_io: Add
bytes_resource_traits + bandwidth_scheduler` adds the structural
plumbing only — it is **not** a working bandwidth controller. See the
commit message for what's broken; condensed limitations in "Known
shortcomings of the plumbing commit" below.

## What's decided

- **Motivation: proactive symmetric coverage.** No bench-pinned
  bandwidth-side bottleneck. The design's job is to give bytes the
  same per-group attribution + protection structure that slots got,
  so the failure mode (one group monopolizes the resource) is closed
  on both axes. Not chasing a specific scenario; designing for
  symmetry with `slot_scheduler`.

## Known shortcomings of the plumbing commit (the things the real design must resolve)

1. **`admit()` slow path is broken for bytes.** The policy's slow
   path queues a waiter on `gs.waiters` and awaits its promise. The
   only thing that resolves that promise is `dispatch_next()`, called
   from `release()`. Bytes (`returnable=false`) never call `release`,
   so the slow path hangs until the abort_source fires. Production
   callers can only safely use `try_admit`.

2. **No dispatch mechanism for time-refilled tokens.** Slots wake
   waiters on caller-driven release events; bytes refill via the
   bucket's internal refresh timer with no corresponding hook into
   the policy's dispatch path. Three plausible designs (from the
   prior elaboration):
     - **(a) Timer-tied dispatch.** Wire the bucket's refresh tick
       into a callback that calls `dispatch_next()` on the policy.
       Preserves ranked dispatch (under-target preference); adds
       plumbing across the bucket↔policy abstraction boundary.
     - **(b) Bypass the policy queue.** For non-returnable traits,
       `admit()` delegates to `token_bucket::maybe_throttle(n, as)`
       and skips the waiter queue entirely. Loses ranked dispatch;
       zero new infrastructure.
     - **(c) External dispatch tick.** A separate timer on the
       policy that fires `dispatch_next()` periodically. Symmetric
       with slot in shape but adds a polling cadence.
   Empirical question: does ranked dispatch matter for bytes? The
   lane reservation alone gives structural fairness (each group's
   rate budget is protected); ranking only matters under common-pool
   contention.

3. **`admit()` API hardcodes `Traits::unit=1`.** Bytes need to carry
   the per-op byte count; without that, `try_admit` consumes one byte
   per call regardless of caller intent. The existing
   `throttle_download` stream-wrap charges variable chunk sizes via
   `maybe_throttle(buffer_size)` — that's the natural shape.

4. **`release()` is unguarded.** The policy's `release()` path calls
   `gs.on_release()` (decrements `in_flight`, may assert if 0) and
   then either dispatches a waiter, refills a lane, or grants back
   to common. For bytes, none of this is meaningful — but nothing
   stops a caller from invoking it. Needs `if constexpr
   (Traits::returnable)` guards (or a hard compile-error on bytes).

5. **Rate vs. instantaneous-token semantics.** The trait's
   `available(c)` returns the lane's configured **rate**
   (`token_bucket::rate()`), not its instantaneous token count. The
   policy uses `available` for both:
     - "Lane's runtime size" (refill/reclaim/transfer accounting) →
       wants rate.
     - "Can we admit right now?" implicitly via `try_acquire` → wants
       tokens.
   The slot trait conflates these (a semaphore has one count). The
   bytes trait splits them: `try_acquire` operates on tokens
   (via `try_throttle`), `available/grant/take` operate on rate (via
   `rate()`/`update_rate()`). This works as long as the policy never
   reads `available` expecting tokens. Verify the policy code's
   assumptions.

## Open questions (not yet answered)

The brainstorming flow had reached the scope question when we paused:

- **Scope:** download only (replace `_throughput_limit`) vs.
  download+upload vs. download-now-upload-later. The handoff doc
  (`2026-05-26-cloud-io-scheduler-generalization-handoff.md`)
  recommends download-only-now, upload as a third PR.

After scope, the questions queued up (rough order):
- **Dispatch mechanism** — pick from (a)/(b)/(c) above; depends on
  whether ranked dispatch matters for bytes.
- **Admission API** — amount-carrying `admit(g, n, as)` on the
  scheduler? Or keep `throttle_download` as a wrapper that calls
  `try_admit` internally and the bucket's blocking for slow path?
- **Conditional release path** — `if constexpr (Traits::returnable)`
  inside the policy, or a separate non-returnable policy class
  family.
- **Rate vs. tokens semantics** — audit the policy code to make sure
  `available` is only used in contexts that want rate.
- **Configuration** — second cluster property
  (`cloud_io_scheduler_bandwidth_reservation`) mirroring
  `cloud_io_scheduler_reservation`. Separate config struct
  (`bandwidth_reservation_config`) and bridge function.
- **Ownership** — where `bandwidth_scheduler` lives. Probably on
  `io_resources` (next to `_throughput_limit` today).
- **Validation** — bench scenario(s) to demonstrate the mechanism
  works. Even without a sharp pre-existing bottleneck, we want to
  show a synthetic workload where lane reservation provides
  measurable protection.

## What's in the working tree / repo state

- Top of branch: `c9394504a1` (the plumbing commit).
- No uncommitted work in the cloud_io / scheduler tree.
- Slot side templating fixups are below in the parent
  `ct/core-16225/cloud-io-sched-generalized` branch — still expected
  to be reviewed/landed independently of bytes work.

## Resume instructions

1. Re-read this doc + the in-tree design notes
   (`src/v/cloud_io/scheduler-design.md`) + the handoff
   (`docs/plans/2026-05-26-cloud-io-scheduler-generalization-handoff.md`).
2. Reload context on `bytes_resource_traits`
   (`src/v/cloud_io/scheduler_traits.h`) and the policy template
   (`src/v/cloud_io/reservation_policy.{h,cc}`).
3. Continue brainstorming from the scope question above. The active
   task list (from the brainstorming skill) had `Ask clarifying
   questions` as the current step.
