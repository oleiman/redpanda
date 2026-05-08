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

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "config/property.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <optional>

namespace cloud_topics::l1 {

/// Per-shard LRU cache of parsed l1::footer structs keyed by l1::object_id.
///
/// Footers are returned by value (via footer::copy) and the cache retains
/// ownership. Eliminates duplicate footer DMA and parse work when multiple
/// readers on the same shard touch the same L1 object.
class l1_footer_cache {
public:
    struct stats {
        size_t cached_footers;
    };

    l1_footer_cache(
      config::binding<std::chrono::milliseconds> eviction_timeout,
      config::binding<size_t> target_max_size);

    l1_footer_cache(const l1_footer_cache&) = delete;
    l1_footer_cache& operator=(const l1_footer_cache&) = delete;
    l1_footer_cache(l1_footer_cache&&) = delete;
    l1_footer_cache& operator=(l1_footer_cache&&) = delete;
    ~l1_footer_cache();

    /// Look up a cached footer by object id. Returns std::nullopt on miss.
    /// On hit, the entry is moved to the most-recently-used position and a
    /// copy of the footer is returned.
    std::optional<footer> get(const object_id& oid);

    /// Insert a parsed footer into the cache. If an entry already exists
    /// for `oid`, it is replaced.
    void put(object_id oid, footer footer_value);

    stats get_stats() const;

    ss::future<> stop();

private:
    struct entry {
        object_id oid;
        footer cached_footer;
        ss::lowres_clock::time_point last_used = ss::lowres_clock::now();
        safe_intrusive_list_hook _hook;
    };

    void arm_eviction_timer();
    ss::future<> maybe_evict();
    void maybe_evict_size();
    bool over_size_limit() const;

    config::binding<std::chrono::milliseconds> _eviction_timeout;
    config::binding<size_t> _target_max_size;

    ss::gate _gate;
    ss::timer<ss::lowres_clock> _eviction_timer;

    counted_intrusive_list<entry, &entry::_hook> _entries;
};

} // namespace cloud_topics::l1
