/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/l1_footer_cache.h"

#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>

namespace cloud_topics::l1 {

l1_footer_cache::l1_footer_cache(
  config::binding<std::chrono::milliseconds> eviction_timeout,
  config::binding<size_t> target_max_size)
  : _eviction_timeout(std::move(eviction_timeout))
  , _target_max_size(std::move(target_max_size)) {
    _eviction_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return maybe_evict().finally([this] { arm_eviction_timer(); });
        });
    });
    arm_eviction_timer();
}

l1_footer_cache::~l1_footer_cache() {
    vassert(
      _entries.empty(), "l1_footer_cache must be stopped before destruction");
}

std::optional<footer> l1_footer_cache::get(const object_id& oid) {
    if (_gate.is_closed()) {
        return std::nullopt;
    }
    auto it = std::find_if(
      _entries.begin(), _entries.end(), [&oid](const entry& e) {
          return e.oid == oid;
      });
    if (it == _entries.end()) {
        return std::nullopt;
    }
    auto& e = *it;
    e.last_used = ss::lowres_clock::now();
    // Move to back: most-recently-used end of the LRU list.
    _entries.erase(_entries.iterator_to(e));
    _entries.push_back(e);
    return e.cached_footer.copy();
}

void l1_footer_cache::put(object_id oid, footer footer_value) {
    if (_gate.is_closed()) {
        return;
    }
    auto it = std::find_if(
      _entries.begin(), _entries.end(), [&oid](const entry& e) {
          return e.oid == oid;
      });
    if (it != _entries.end()) {
        auto* existing = &*it;
        _entries.erase(_entries.iterator_to(*existing));
        delete existing; // NOLINT
    }
    auto* e = new entry{
      // NOLINT
      .oid = std::move(oid),
      .cached_footer = std::move(footer_value)};
    _entries.push_back(*e);
    maybe_evict_size();
}

l1_footer_cache::stats l1_footer_cache::get_stats() const {
    return stats{.cached_footers = _entries.size()};
}

ss::future<> l1_footer_cache::stop() {
    if (_eviction_timer.armed()) {
        _eviction_timer.cancel();
    }
    co_await _gate.close();
    _entries.clear_and_dispose([](entry* e) {
        delete e; // NOLINT
    });
}

void l1_footer_cache::arm_eviction_timer() {
    if (_gate.is_closed()) {
        return;
    }
    auto timeout = _eviction_timeout();
    if (timeout > std::chrono::milliseconds::zero()) {
        _eviction_timer.arm(timeout);
    }
}

ss::future<> l1_footer_cache::maybe_evict() {
    auto cutoff = ss::lowres_clock::now() - _eviction_timeout();
    auto it = _entries.begin();
    while (it != _entries.end()) {
        if (it->last_used >= cutoff) {
            ++it;
            continue;
        }
        auto* e = &*it;
        it = _entries.erase(it);
        delete e; // NOLINT
        co_await ss::coroutine::maybe_yield();
    }
}

void l1_footer_cache::maybe_evict_size() {
    while (over_size_limit() && !_entries.empty()) {
        auto* e = &_entries.front();
        _entries.pop_front();
        delete e; // NOLINT
    }
}

bool l1_footer_cache::over_size_limit() const {
    return _entries.size() > _target_max_size();
}

} // namespace cloud_topics::l1
