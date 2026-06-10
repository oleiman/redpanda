/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/inflight_download_map.h"

#include "base/vassert.h"

namespace cloud_topics::l1 {

inflight_download_map::leader_guard::leader_guard(
  inflight_download_map* map, std::filesystem::path key) noexcept
  : _map(map)
  , _key(std::move(key)) {}

inflight_download_map::leader_guard::leader_guard(leader_guard&& o) noexcept
  : _map(std::exchange(o._map, nullptr))
  , _key(std::move(o._key))
  , _outcome(std::move(o._outcome)) {}

inflight_download_map::leader_guard&
inflight_download_map::leader_guard::operator=(leader_guard&& o) noexcept {
    if (this != &o) {
        // Releasing here would be unusual (the previous leader didn't
        // publish), but it's the right behavior: each guard owns
        // exactly one map entry's lifetime.
        if (_map != nullptr) {
            _map->release(_key, _outcome);
        }
        _map = std::exchange(o._map, nullptr);
        _key = std::move(o._key);
        _outcome = std::move(o._outcome);
    }
    return *this;
}

inflight_download_map::leader_guard::~leader_guard() noexcept {
    if (_map != nullptr) {
        _map->release(_key, _outcome);
    }
}

void inflight_download_map::leader_guard::resolve(outcome o) noexcept {
    _outcome = std::move(o);
}

inflight_download_map::join_result inflight_download_map::join_or_lead(
  std::filesystem::path key, ss::abort_source& as) {
    // No co_await is permitted between this lookup and the emplace
    // below; that property is what makes the join-or-lead atomic on a
    // single shard.
    if (auto it = _entries.find(key); it != _entries.end()) {
        return it->second.get_shared_future(as);
    }

    if (_entries.size() >= _max_entries) {
        return at_capacity;
    }

    auto [it, inserted] = _entries.emplace(
      key, ss::shared_promise<outcome>{});
    vassert(
      inserted,
      "inflight_download_map: concurrent insert for {}",
      key.native());
    return leader_guard{this, std::move(key)};
}

void inflight_download_map::release(
  const std::filesystem::path& key, outcome o) noexcept {
    auto it = _entries.find(key);
    vassert(
      it != _entries.end(),
      "inflight_download_map: entry for {} erased outside release",
      key.native());
    it->second.set_value(o);
    _entries.erase(it);
}

} // namespace cloud_topics::l1
