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

// NOTE: This header must be included via cloud_io/scheduler.h, not
// directly. It references scheduler::permit (a nested type) which
// requires scheduler.h to be parsed first.

#include "base/seastarx.h"
#include "cloud_io/group_id.h"
#include "metrics/metrics.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace cloud_io {

class scheduler;

/// \brief Abstract base for cloud_io::scheduler admission policies.
///
/// One concrete policy is selected at construction via the
/// cloud_io_scheduler_policy cluster property. The shell holds a
/// std::unique_ptr<scheduler_policy> and forwards calls.
class scheduler_policy {
public:
    scheduler_policy(scheduler& shell, size_t capacity) noexcept
      : _shell(shell)
      , _capacity(capacity) {}
    scheduler_policy(const scheduler_policy&) = delete;
    scheduler_policy& operator=(const scheduler_policy&) = delete;
    scheduler_policy(scheduler_policy&&) = delete;
    scheduler_policy& operator=(scheduler_policy&&) = delete;
    virtual ~scheduler_policy() noexcept = default;

    /// Admit an op tagged with the given group_id. Returns a RAII
    /// permit on success; throws ss::abort_requested_exception if
    /// the abort source fires during a slow-path wait.
    [[nodiscard]] virtual ss::future<scheduler::permit>
    admit(group_id, ss::abort_source&) = 0;

    /// Non-blocking admit. Returns nullopt if admit would queue.
    [[nodiscard]] virtual std::optional<scheduler::permit>
      try_admit(group_id) noexcept = 0;

    /// Called when a permit is destroyed.
    virtual void release(group_id) noexcept = 0;

    /// Observability getters. Policies that don't track per-group
    /// state should return 0 (or an aggregate for the global ones).
    virtual size_t in_flight(group_id) const noexcept = 0;
    virtual size_t waiters(group_id) const noexcept = 0;
    virtual size_t available_slots() const noexcept = 0;
    virtual size_t total_capacity() const noexcept = 0;

    /// Register the policy's own gauges with the shell-provided
    /// metric groups. Called during scheduler::start().
    virtual void register_metrics(
      metrics::internal_metric_groups&, metrics::public_metric_groups&) = 0;

    /// Optional lifecycle hook. Default no-op.
    virtual ss::future<> stop() { return ss::make_ready_future<>(); }

protected:
    scheduler& _shell;
    size_t _capacity;
};

} // namespace cloud_io
