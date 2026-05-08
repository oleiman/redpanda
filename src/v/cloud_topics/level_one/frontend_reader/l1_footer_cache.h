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
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <memory>
#include <optional>

namespace cloud_topics {
class level_one_reader_probe;
}

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

    /// The optional `probe` is used to publish the cache's current size as
    /// a metric. When provided, the cache registers a size-getter on the
    /// probe at construction and resets it back to the default on
    /// destruction; this keeps the metric callback safe across the
    /// shutdown window where the cache is torn down before the probe.
    l1_footer_cache(
      config::binding<std::chrono::milliseconds> eviction_timeout,
      config::binding<size_t> target_max_size,
      level_one_reader_probe* probe = nullptr);

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

    /// True when `target_max_size > 0`. When false, callers should skip
    /// `get`/`put` so the cache-hit/miss metrics aren't polluted by
    /// inevitable misses through a disabled cache.
    bool is_enabled() const;

    stats get_stats() const;

    ss::future<> stop();

private:
    /// Entries are owned by `_index` (keyed by oid) and linked into `_lru` for
    /// recency ordering. The two structures track distinct eviction criteria:
    ///   * `_lru` order (LRU at front, MRU at back) drives size-bounded
    ///     eviction in `maybe_evict_size`.
    ///   * `last_used` drives time-bounded idle eviction in `maybe_evict`.
    /// The `intrusive_list_hook` is in auto-unlink mode, so destroying an
    /// entry via `_index.erase` removes it from `_lru` without an explicit
    /// unlink at the callsite.
    struct entry {
        entry(object_id oid, footer cached_footer)
          : oid(std::move(oid))
          , cached_footer(std::move(cached_footer)) {}

        object_id oid;
        footer cached_footer;
        ss::lowres_clock::time_point last_used = ss::lowres_clock::now();
        intrusive_list_hook _hook;
    };

    void arm_eviction_timer();
    void maybe_evict();
    void maybe_evict_size();
    bool over_size_limit() const;

    config::binding<std::chrono::milliseconds> _eviction_timeout;
    config::binding<size_t> _target_max_size;
    level_one_reader_probe* _probe;

    ss::gate _gate;
    ss::timer<ss::lowres_clock> _eviction_timer;

    chunked_hash_map<object_id, std::unique_ptr<entry>> _index;
    intrusive_list<entry, &entry::_hook> _lru;
};

} // namespace cloud_topics::l1
