/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/null_policy.h"

#include <seastar/core/coroutine.hh>

namespace cloud_io {

null_policy::null_policy(scheduler& shell, size_t capacity)
  : scheduler_policy(shell, capacity)
  , _slots(capacity, "cloud_io/scheduler/null/slots") {}

ss::future<scheduler::permit>
null_policy::admit(group_id g, ss::abort_source& as) {
    // ss::get_units honors the abort source.
    auto units = co_await ss::get_units(_slots, 1, as);
    units.release(); // we hand-manage release via the permit destructor
    co_return _shell.make_permit(g);
}

std::optional<scheduler::permit> null_policy::try_admit(group_id g) noexcept {
    if (_slots.try_wait(1)) {
        return _shell.make_permit(g);
    }
    return std::nullopt;
}

void null_policy::release(group_id) noexcept { _slots.signal(1); }

size_t null_policy::in_flight(group_id) const noexcept {
    return _capacity - _slots.available_units();
}

size_t null_policy::waiters(group_id) const noexcept {
    return _slots.waiters();
}

size_t null_policy::available_slots() const noexcept {
    return _slots.available_units();
}

size_t null_policy::total_capacity() const noexcept { return _capacity; }

void null_policy::register_metrics(
  metrics::internal_metric_groups& /*internal*/,
  metrics::public_metric_groups& /*public_*/) {
    // Null policy intentionally registers no metrics in PR1: per-pool
    // metrics need a discriminator label (multiple pool instances exist
    // in multi-application tests). Label-threading is deferred to PR2
    // where fair_share's per-group state makes the metrics actually
    // load-bearing.
}

ss::future<> null_policy::stop() {
    _slots.broken();
    co_return;
}

} // namespace cloud_io
