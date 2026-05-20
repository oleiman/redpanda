/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_io/fair_policy_types.h"
#include "cloud_io/scheduler_policy.h"
#include "cloud_io/scheduler_types.h"
#include "metrics/metrics.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <array>
#include <cstdint>
#include <functional>

namespace cloud_io {

/// \brief Weighted fair-share-by-occupancy admission policy.
///
/// Bounds simultaneous in-flight cloud_io ops to the configured
/// capacity. Under contention, slots are dispatched in proportion
/// to per-group weights (fair-share-by-occupancy discipline).
/// Fast-path admission is bounded by each group's weight-proportional
/// cap of the effective active set (groups currently active OR
/// within the dwell window). Work-conserving: an idle group's share
/// flows to demanding groups automatically.
///
/// Per-shard; not movable (metrics lambdas capture `this`).
class fair_policy final : public scheduler_policy {
public:
    explicit fair_policy(size_t capacity);
    fair_policy(const fair_policy&) = delete;
    fair_policy& operator=(const fair_policy&) = delete;
    fair_policy(fair_policy&&) = delete;
    fair_policy& operator=(fair_policy&&) = delete;
    ~fair_policy() noexcept override;

    [[nodiscard]] ss::future<> admit(group_id g, ss::abort_source& as) override;
    [[nodiscard]] bool try_admit(group_id g) noexcept override;
    void release(group_id g) noexcept override;
    ss::future<> stop() override;

    size_t in_flight(group_id) const noexcept override;
    size_t waiters(group_id) const noexcept override;
    size_t available_slots() const noexcept override;
    size_t total_capacity() const noexcept override;

    // ---- fair_policy-only (test-only) public API ----

    /// Current weight for a group. Not on the ABC; not exposed via
    /// the scheduler shell.
    uint32_t weight(group_id) const noexcept;

    /// Current sum of weights of groups in the effective active set.
    /// Diagnostic accessor; mirrors the Prometheus metric. Not on the
    /// ABC.
    int64_t effective_active_weight() const noexcept;

    /// Runtime weight mutator. Test-only — production never calls
    /// this. NOT on the ABC.
    void set_weight(group_id, uint32_t);

    /// Runtime capacity mutator. Test-only. NOT on the ABC.
    void set_total_slots(size_t);

    /// Runtime min_reserved mutator. Test-only — production reads
    /// from cluster config at construction. NOT on the ABC.
    void set_min_reserved(group_id, uint32_t);

    /// Current min_reserved floor for a group. Not on the ABC.
    uint32_t min_reserved(group_id) const noexcept;

    /// Test hook — override the clock function for deterministic
    /// dwell-window testing.
    using now_fn_t = std::function<ss::lowres_clock::time_point()>;
    void set_now_fn_for_test(now_fn_t);

private:
    /// Dispatch the next queued waiter using fair-share-by-occupancy.
    /// Returns true and transfers ownership of one slot to the
    /// dispatched waiter if any group had waiters; returns false if
    /// queues are empty and the slot should be returned to the
    /// semaphore.
    bool dispatch_next() noexcept;

    /// Walk groups; subtract from _effective_active_weight any group
    /// whose dwell window has expired since the last call. O(N).
    void refresh_dwell_expirations(ss::lowres_clock::time_point now);

    size_t _current_total_capacity{0};
    /// Slots not pre-reserved to any specific group. All groups draw
    /// from this as the second-choice fast-path source, and slow-path
    /// dispatch (dispatch_next) routes through this pool.
    ssx::semaphore _shared;
    /// Per-group dedicated slot pools sized to min_reserved[g]. Phase
    /// 1 hard reservation: only the owning group can consume these
    /// slots; cross-group lending is not permitted (Phase 2 concern).
    std::array<ssx::semaphore, num_group_ids> _reserved;

    std::array<fair_group_state, num_group_ids> _groups;

    /// Cached sum of weights of groups in the effective active set.
    /// Maintained incrementally on transitions; lazily refreshed in
    /// admit() for dwell expirations.
    int64_t _effective_active_weight = 0;

    /// Clock provider; defaults to ss::lowres_clock::now. Overridable
    /// via set_now_fn_for_test for deterministic unit tests.
    now_fn_t _now_fn;

    /// Monotonic counter assigned to each queued waiter for FIFO
    /// tie-breaking in the floor-preference branch of dispatch_next.
    uint64_t _waiter_seq_counter{0};

    /// Counts dispatch_next calls that had multiple groups with
    /// waiters. Used to throttle the periodic diagnostic log.
    uint64_t _multi_group_dispatch_counter = 0;

    // Declared last so metrics groups are unregistered before their
    // backing members are destroyed.
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_io
