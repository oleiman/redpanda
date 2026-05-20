/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/scheduler.h"

#include "base/vlog.h"
#include "cloud_io/logger.h"
#include "cloud_io/min_share_policy.h"
#include "cloud_io/scheduler_policy.h"
#include "config/configuration.h"
#include "null_policy.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace cloud_io {

std::unique_ptr<scheduler_policy>
scheduler::make_policy(policy_type t, size_t capacity) {
    switch (t) {
    case policy_type::null:
        return std::make_unique<null_policy>(capacity);
    case policy_type::min_share: {
        min_share_policy_config policy_cfg;
        for (const auto& entry :
             config::shard_local_cfg().cloud_io_scheduler_min_share()) {
            bool matched = false;
            for (const auto g : all_group_ids) {
                if (entry.group_name == to_string_view(g)) {
                    policy_cfg.target_reserved[g] = entry.target_reserved;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                vlog(
                  log.warn,
                  "cloud_io_scheduler_min_share: ignoring unknown "
                  "group_name '{}'",
                  entry.group_name);
            }
        }
        return std::make_unique<min_share_policy>(capacity, policy_cfg);
    }
    }
    std::unreachable();
}

scheduler::scheduler(policy_type t, size_t capacity)
  : _policy(make_policy(t, capacity)) {}

scheduler::~scheduler() noexcept = default;

ss::future<> scheduler::stop() {
    _draining = true;
    co_await _policy->stop();
}

ss::future<> scheduler::admit(group_id g, ss::abort_source& as) {
    if (_draining) {
        throw ss::abort_requested_exception{};
    }
    co_await _policy->admit(g, as);
}

bool scheduler::try_admit(group_id g) {
    if (_draining) {
        return false;
    }
    return _policy->try_admit(g);
}

void scheduler::release(group_id g) { _policy->release(g); }

size_t scheduler::in_flight(group_id g) const { return _policy->in_flight(g); }
size_t scheduler::waiters(group_id g) const { return _policy->waiters(g); }
size_t scheduler::available_slots() const { return _policy->available_slots(); }
size_t scheduler::total_capacity() const { return _policy->total_capacity(); }
bool scheduler::has_waiters() const {
    for (uint8_t i = 0; i < num_group_ids; ++i) {
        if (_policy->waiters(static_cast<group_id>(i)) > 0) {
            return true;
        }
    }
    return false;
}

} // namespace cloud_io
