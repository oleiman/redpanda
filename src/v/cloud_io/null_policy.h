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

#include "cloud_io/scheduler_policy.h"

namespace cloud_io {

/// No-op admission policy.
///
/// When active, concurrency is bounded by the client pool's capacity alone.
class null_policy final : public scheduler_policy {
public:
    using scheduler_policy::scheduler_policy;

    ss::future<> admit(group_id, ss::abort_source&) override {
        return ss::now();
    }

    bool try_admit(group_id) noexcept override { return true; }

    void release(group_id) noexcept override {}

    size_t in_flight(group_id) const noexcept override { return 0; }
    size_t waiters(group_id) const noexcept override { return 0; }
    size_t available_slots() const noexcept override { return _capacity; }
    size_t total_capacity() const noexcept override { return _capacity; }
};

} // namespace cloud_io
