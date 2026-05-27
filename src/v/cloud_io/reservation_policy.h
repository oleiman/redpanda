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
#include "cloud_io/reservation_policy_types.h"
#include "cloud_io/scheduler_policy.h"
#include "cloud_io/scheduler_traits.h"
#include "cloud_io/scheduler_types.h"
#include "metrics/metrics.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <cstdint>
#include <functional>
#include <optional>

namespace cloud_io {

/// \brief Reservation-based admission policy.
///
/// Bounds simultaneous in-flight cloud_io ops to the configured capacity.
/// Slots live in one of two places: each group's reservation lane (held by
/// the group's reserved_container inside reservation_group_state) or the
/// common pool (_shared, below). Reservations ebb and flow with demand:
/// refill builds a lane back up toward target_reserved while the group is
/// active; the policy reclaims idle reservations to the common pool after
/// the dwell window. See reservation_group_state for the mechanism details.
///
/// Templated on resource Traits: the slot scheduler uses
/// slot_resource_traits, which backs lanes with an ssx::semaphore. A
/// future bytes scheduler will use bytes_resource_traits backed by a
/// token_bucket. The policy logic is identical between the two; the
/// container primitives differ via the trait.
///
/// Per-shard; not movable (metrics lambdas capture `this`).
template<typename Traits>
class reservation_policy final : public scheduler_policy<Traits> {
public:
    using amount_t = typename Traits::amount_t;
    using container_t = typename Traits::container_t;
    using group_state_t = reservation_group_state<Traits>;

    explicit reservation_policy(
      amount_t capacity, reservation_policy_config = {});
    reservation_policy(const reservation_policy&) = delete;
    reservation_policy& operator=(const reservation_policy&) = delete;
    reservation_policy(reservation_policy&&) = delete;
    reservation_policy& operator=(reservation_policy&&) = delete;
    ~reservation_policy() noexcept override;

    ss::future<> admit(group_id g, ss::abort_source& as) override;
    [[nodiscard]] bool try_admit(group_id g) noexcept override;
    void release(group_id g) noexcept override;
    ss::future<> stop() override;

    size_t in_flight(group_id) const noexcept override;
    size_t waiters(group_id) const noexcept override;
    amount_t available_slots() const noexcept override;
    amount_t total_capacity() const noexcept override;

    // ---- reservation_policy-specific public API (not on ABC) ----

    /// Runtime target_reserved mutator. Construction installs initial
    /// targets via reservation_policy_config; this exists for runtime
    /// cluster reconfiguration after the fact (and for tests that
    /// mutate targets mid-scenario).
    void set_target_reserved(group_id, amount_t);

    /// Current target_reserved floor for a group.
    amount_t target_reserved(group_id) const noexcept;

    /// Runtime reservation size for a group, derived from the group's
    /// reservation container and reserved_in_flight count. See
    /// reservation_group_state::current_reserved for semantics.
    amount_t current_reserved(group_id) const noexcept;

    // ---- test-only public API ----

    /// Runtime capacity mutator. Production cluster config can't change
    /// capacity at runtime. Exists so tests can grow or shrink the
    /// common pool without rebuilding the policy.
    void set_total_slots(amount_t);

    // ---- observability accessors ----

    /// Lifetime admit() count for a group.
    uint64_t admit_total(group_id) const noexcept;

    /// Lifetime fast-path-admit count for a group.
    uint64_t admit_immediate_total(group_id) const noexcept;

    /// Total queued waiters across all groups.
    size_t total_waiters() const noexcept;

    /// Test hook: override the clock function for deterministic
    /// dwell-window testing.
    using now_fn_t = std::function<ss::lowres_clock::time_point()>;
    void set_now_fn_for_test(now_fn_t);

private:
    void setup_metrics();

    /// Dispatch the next queued waiter. Two-tier choice:
    ///   1. Under-target preference: among groups with in_flight <
    ///      target_reserved AND queued waiters, the oldest seq wins.
    ///   2. FIFO fallback: if no group is under target, the global
    ///      oldest-seq across all groups with waiters wins.
    /// Returns false only when no group has waiters. The caller then
    /// tries refill or returns the slot to the common pool.
    bool dispatch_next() noexcept;

    /// Walk groups and reclaim the reservation of any inactive group
    /// whose dwell window has elapsed. Reclaimed capacity returns to
    /// the common pool. O(N) where N is num_group_ids.
    void reclaim_idle_reservations(ss::lowres_clock::time_point now);

    /// Pick the group that should receive a common-pool slot as a refill.
    /// Eligibility: effective-active (likely to use the slot) AND
    /// current_reserved < target_reserved (has room to grow). Among
    /// eligible groups, the one most below its target wins. Returns
    /// nullopt if no group is eligible; the slot then returns to the
    /// common pool.
    std::optional<group_id> pick_refill_candidate() noexcept;

    amount_t _current_total_capacity{0};
    /// The common pool. Any group can claim from it; releases go back
    /// here unless refill diverts them into a reservation lane.
    container_t _shared;

    /// Per-group state including the reservation container. See
    /// reservation_group_state for the layout and invariants.
    per_group<group_state_t> _groups;

    /// Clock provider; defaults to ss::lowres_clock::now. Override
    /// via set_now_fn_for_test for deterministic unit tests.
    now_fn_t _now_fn;

    /// Policy-global monotonic counter for queued waiters to enforce FIFO
    /// in dispatch_next.
    uint64_t _waiter_seq_counter{0};

    /// Monotonically increments on every dispatch; modulo-throttles
    /// the periodic diagnostic log.
    uint64_t _dispatch_counter = 0;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

extern template class reservation_policy<slot_resource_traits>;

} // namespace cloud_io
