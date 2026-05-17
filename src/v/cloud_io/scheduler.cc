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

#include "cloud_io/scheduler_policy.h"

#include <seastar/core/coroutine.hh>

namespace cloud_io {

scheduler::scheduler(policy_type t, size_t capacity)
  : _policy(make_scheduler_policy(t, *this, capacity)) {}

scheduler::~scheduler() noexcept = default;

ss::future<> scheduler::start() {
    _policy->register_metrics(_metrics, _public_metrics);
    // Note: a per-pool policy-name info gauge would collide with
    // itself in multi-application tests because every pool's scheduler
    // would register the same name+labels. The discriminator-label
    // threading needed to make scheduler metrics multi-pool-safe is
    // deferred to PR2 alongside fair_share's real metric block.
    co_return;
}

ss::future<> scheduler::stop() {
    _draining = true;
    co_await _policy->stop();
}

ss::future<scheduler::permit>
scheduler::admit(group_id g, ss::abort_source& as) {
    if (_draining) {
        throw ss::abort_requested_exception{};
    }
    co_return co_await _policy->admit(g, as);
}

std::optional<scheduler::permit> scheduler::try_admit(group_id g) noexcept {
    if (_draining) {
        return std::nullopt;
    }
    return _policy->try_admit(g);
}

size_t scheduler::in_flight(group_id g) const noexcept {
    return _policy->in_flight(g);
}
size_t scheduler::waiters(group_id g) const noexcept {
    return _policy->waiters(g);
}
size_t scheduler::available_slots() const noexcept {
    return _policy->available_slots();
}
size_t scheduler::total_capacity() const noexcept {
    return _policy->total_capacity();
}
bool scheduler::has_waiters() const noexcept {
    for (uint8_t i = 0; i < num_group_ids; ++i) {
        if (_policy->waiters(static_cast<group_id>(i)) > 0) {
            return true;
        }
    }
    return false;
}

scheduler::permit scheduler::make_permit(group_id g) noexcept {
    return permit{this, g};
}

void scheduler::release_permit(group_id g) noexcept { _policy->release(g); }

void scheduler::release_remote(group_id g) noexcept { _policy->release(g); }

// permit move semantics
scheduler::permit::permit(permit&& other) noexcept
  : _shell(std::exchange(other._shell, nullptr))
  , _remote_sid(std::exchange(other._remote_sid, std::nullopt))
  , _group(other._group) {}

scheduler::permit& scheduler::permit::operator=(permit&& other) noexcept {
    if (this != &other) {
        return_all();
        _shell = std::exchange(other._shell, nullptr);
        _remote_sid = std::exchange(other._remote_sid, std::nullopt);
        _group = other._group;
    }
    return *this;
}

scheduler::permit::~permit() noexcept { return_all(); }

void scheduler::permit::return_all() noexcept {
    if (_shell != nullptr) {
        _shell->release_permit(_group);
        _shell = nullptr;
    }
    // Remote permits: the lease's deleter is responsible for the
    // cross-shard release. The destructor just clears state.
    _remote_sid.reset();
}

} // namespace cloud_io
