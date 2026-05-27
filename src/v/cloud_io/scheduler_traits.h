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

#include "ssx/semaphore.h"

#include <seastar/core/sstring.hh>

#include <cstddef>
#include <utility>

namespace cloud_io {

/// \brief Resource traits for the slot scheduler.
///
/// Each lane and the common pool is an ssx::semaphore counting available
/// slots. Acquires are caller-driven 1-unit operations; releases pair
/// 1:1 with each acquire (a lease drop returns one slot to the lane the
/// acquire came from). This trait is the first of an intended pair: a
/// future bytes_resource_traits will instantiate the same reservation
/// policy over token_bucket-backed lanes, where refills are time-driven
/// rather than caller-driven (see `returnable` below).
struct slot_resource_traits {
    using container_t = ssx::semaphore;
    using amount_t = size_t;

    /// True if releases are paired 1:1 with acquires (caller-driven).
    /// The policy compiles in its release path only when this is true.
    static constexpr bool returnable = true;

    /// Implicit acquire/release amount. Slot operations are
    /// 1-unit-at-a-time.
    static constexpr amount_t unit = 1;

    /// Construct a container with the given initial count and name.
    /// Returned by value; relies on guaranteed copy elision to construct
    /// in place at the destination (ssx::semaphore is non-movable).
    static container_t make_container(amount_t initial, ss::sstring name) {
        return container_t(initial, std::move(name));
    }

    /// Non-blocking acquire. Returns true iff `n` units were
    /// immediately available and consumed.
    [[nodiscard]] static bool
    try_acquire(container_t& c, amount_t n) noexcept {
        return c.try_wait(n);
    }

    /// Add `n` units to the container.
    static void grant(container_t& c, amount_t n) noexcept { c.signal(n); }

    /// Consume `n` units from the container without blocking.
    /// Precondition: at least `n` units are currently available.
    static void take(container_t& c, amount_t n) noexcept { c.consume(n); }

    /// Current count of units immediately acquirable.
    [[nodiscard]] static amount_t available(const container_t& c) noexcept {
        return c.current();
    }
};

} // namespace cloud_io
