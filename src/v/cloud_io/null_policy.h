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

// Include scheduler.h first so that scheduler::permit is fully defined
// before we reference it in our overrides.
#include "cloud_io/scheduler.h"
#include "ssx/semaphore.h"

namespace cloud_io {

/// \brief FIFO admission policy. Wraps a single ssx::semaphore of
/// capacity = pool size; admit = get_units(1); release = signal(1).
/// No per-group state. Equivalent to a condition_variable wait/signal
/// at the same capacity.
class null_policy final : public scheduler_policy {
public:
    null_policy(scheduler& shell, size_t capacity);

    [[nodiscard]] ss::future<scheduler::permit>
    admit(group_id g, ss::abort_source& as) override;

    [[nodiscard]] std::optional<scheduler::permit>
    try_admit(group_id g) noexcept override;

    void release(group_id) noexcept override;

    size_t in_flight(group_id) const noexcept override;
    size_t waiters(group_id) const noexcept override;
    size_t available_slots() const noexcept override;
    size_t total_capacity() const noexcept override;

    void register_metrics(
      metrics::internal_metric_groups&,
      metrics::public_metric_groups&) override;

    ss::future<> stop() override;

private:
    ssx::semaphore _slots;
};

} // namespace cloud_io
