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
#include "cloud_io/scheduler_types.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <array>
#include <chrono>
#include <cstdint>

namespace cloud_io {

/// \brief Default per-group weights for fair-share-by-occupancy
/// dispatch, in `group_id` enum order.
///
/// producer_upload and consumer_fetch are latency-critical and
/// share equal weight. default_group is the catch-all (manifest I/O,
/// archival, hydration, housekeeping) — heavier so the catch-all
/// bucket does not get crushed by a single hot producer or fetch.
inline constexpr std::array<uint32_t, num_group_ids> default_weights{
  1000, // producer_upload
  1000, // consumer_fetch
  1500, // default_group
};

/// \brief Default dwell window — duration after a group goes idle
/// during which it remains in the scheduler's "effective active" set.
///
/// Preserves share reservations across activity gaps so sporadic
/// workloads (e.g., bursty producers competing with sustained
/// consumers) don't lose their share whenever they briefly go idle.
/// Falls out of the effective set after this duration of no activity.
inline constexpr std::chrono::seconds default_dwell_duration{5};

/// One queued admit() call. Lives on the caller's coroutine frame;
/// linked into the owning group's waiters list while queued.
struct fair_waiter {
    ss::promise<> p;
    intrusive_list_hook link;
    /// Monotonic insertion sequence assigned by fair_policy at
    /// queue time. Used for FIFO tie-breaking across floored groups
    /// in dispatch_next.
    uint64_t seq{0};
};

/// Per-group scheduler state. Sized to num_group_ids; indexed by
/// static_cast<size_t>(group_id).
struct fair_group_state {
    uint32_t weight = 1;
    /// Hard-reserved slot count for this group. Backed by a dedicated
    /// per-group sub-semaphore; admits draw from it first (Phase 1).
    uint32_t min_reserved = 0;
    /// Runtime reservation size. Starts equal to min_reserved on
    /// set_min_reserved. Decays toward 0 when the group goes idle
    /// past default_dwell_duration. Grows back via steal-back on
    /// subsequent shared-release events when the group is
    /// effective-active. Invariants: 0 <= current_reserved <=
    /// min_reserved.
    uint32_t current_reserved = 0;
    size_t in_flight = 0;
    /// Of the total in_flight, how many slots came from this group's
    /// reserved semaphore (vs the shared pool). Invariants:
    ///   reserved_in_flight <= in_flight
    ///   reserved_in_flight <= min_reserved
    size_t reserved_in_flight = 0;
    intrusive_list<fair_waiter, &fair_waiter::link> waiters;
    uint64_t admit_total = 0;
    uint64_t admit_immediate_total = 0;
    /// Updated on transition→inactive. Used with dwell window to
    /// compute "effective active" set for fast-path cap denominator.
    ss::lowres_clock::time_point last_active{};
};

} // namespace cloud_io
