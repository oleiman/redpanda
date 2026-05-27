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

#include "cloud_io/reservation_policy.h"
#include "cloud_io/scheduler_policy.h"
#include "cloud_io/scheduler_traits.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace cloud_io {

namespace {

/// No-op admission policy.
///
/// When active, concurrency is bounded by the client pool's capacity alone.
template<typename Traits>
class passthrough final : public scheduler_policy<Traits> {
public:
    using base = scheduler_policy<Traits>;
    using base::base;
    using typename base::amount_t;

    ss::future<> admit(group_id, ss::abort_source&) override {
        return ss::now();
    }

    bool try_admit(group_id) noexcept override { return true; }

    void release(group_id) noexcept override {}

    size_t in_flight(group_id) const noexcept override { return 0; }
    size_t waiters(group_id) const noexcept override { return 0; }
    amount_t available_slots() const noexcept override {
        return this->_capacity;
    }
    amount_t total_capacity() const noexcept override {
        return this->_capacity;
    }
};

} // namespace

template<typename Traits>
std::unique_ptr<scheduler_policy<Traits>>
scheduler<Traits>::make_policy(amount_t capacity, scheduler_config cfg) {
    switch (cfg.policy) {
    case policy_type::passthrough:
        return std::make_unique<passthrough<Traits>>(capacity);
    case policy_type::reservation:
        return std::make_unique<reservation_policy<Traits>>(
          capacity,
          std::move(cfg.reservation).value_or(reservation_policy_config{}));
    }
    std::unreachable();
}

template<typename Traits>
scheduler<Traits>::scheduler(amount_t capacity, scheduler_config cfg)
  : _policy(make_policy(capacity, std::move(cfg))) {}

template<typename Traits>
scheduler<Traits>::~scheduler() noexcept = default;

template<typename Traits>
ss::future<> scheduler<Traits>::stop() {
    _draining = true;
    co_await _policy->stop();
}

template<typename Traits>
ss::future<> scheduler<Traits>::admit(group_id g, ss::abort_source& as) {
    if (_draining) {
        throw ss::abort_requested_exception{};
    }
    co_await _policy->admit(g, as);
}

template<typename Traits>
bool scheduler<Traits>::try_admit(group_id g) {
    if (_draining) {
        return false;
    }
    return _policy->try_admit(g);
}

template<typename Traits>
void scheduler<Traits>::release(group_id g) {
    _policy->release(g);
}

template<typename Traits>
size_t scheduler<Traits>::in_flight(group_id g) const {
    return _policy->in_flight(g);
}

template<typename Traits>
size_t scheduler<Traits>::waiters(group_id g) const {
    return _policy->waiters(g);
}

template<typename Traits>
auto scheduler<Traits>::available_slots() const -> amount_t {
    return _policy->available_slots();
}

template<typename Traits>
auto scheduler<Traits>::total_capacity() const -> amount_t {
    return _policy->total_capacity();
}

template<typename Traits>
bool scheduler<Traits>::has_waiters() const {
    for (uint8_t i = 0; i < num_group_ids; ++i) {
        if (_policy->waiters(static_cast<group_id>(i)) > 0) {
            return true;
        }
    }
    return false;
}

template class scheduler<slot_resource_traits>;

} // namespace cloud_io
