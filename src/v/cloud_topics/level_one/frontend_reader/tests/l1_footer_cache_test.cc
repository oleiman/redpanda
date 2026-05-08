/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/frontend_reader/l1_footer_cache.h"
#include "config/property.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

class l1_footer_cache_test : public seastar_test {
protected:
    static constexpr std::chrono::milliseconds default_eviction_timeout = 60s;
    static constexpr size_t default_max_size = 4;

    void SetUp() override {
        _eviction_timeout_binding = config::mock_binding(
          default_eviction_timeout);
        _max_size_binding = config::mock_binding(default_max_size);
        _cache = std::make_unique<l1_footer_cache>(
          _eviction_timeout_binding, _max_size_binding);
    }

    ss::future<> TearDownAsync() override {
        if (_cache) {
            co_await _cache->stop();
            _cache.reset();
        }
    }

    config::binding<std::chrono::milliseconds> _eviction_timeout_binding{
      config::mock_binding(default_eviction_timeout)};
    config::binding<size_t> _max_size_binding{
      config::mock_binding(default_max_size)};
    std::unique_ptr<l1_footer_cache> _cache;
};

TEST_F(l1_footer_cache_test, miss_on_empty) {
    auto miss = _cache->get(create_object_id());
    EXPECT_FALSE(miss.has_value());
    EXPECT_EQ(_cache->get_stats().cached_footers, 0);
}

TEST_F(l1_footer_cache_test, hit_after_put) {
    auto oid = create_object_id();
    _cache->put(oid, footer{});

    auto hit = _cache->get(oid);
    EXPECT_TRUE(hit.has_value());
    EXPECT_EQ(_cache->get_stats().cached_footers, 1);
}

TEST_F(l1_footer_cache_test, miss_on_different_oid) {
    auto oid1 = create_object_id();
    auto oid2 = create_object_id();
    ASSERT_NE(oid1, oid2);

    _cache->put(oid1, footer{});

    auto miss = _cache->get(oid2);
    EXPECT_FALSE(miss.has_value());
    auto hit = _cache->get(oid1);
    EXPECT_TRUE(hit.has_value());
}

TEST_F(l1_footer_cache_test, overflow_evicts_oldest) {
    std::vector<object_id> oids;
    oids.reserve(default_max_size + 2);
    for (size_t i = 0; i < default_max_size + 2; ++i) {
        oids.push_back(create_object_id());
        _cache->put(oids.back(), footer{});
    }

    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_footers, default_max_size);

    // First two oids should have been evicted as oldest.
    EXPECT_FALSE(_cache->get(oids[0]).has_value());
    EXPECT_FALSE(_cache->get(oids[1]).has_value());
    // The remaining oids should still be present.
    for (size_t i = 2; i < oids.size(); ++i) {
        EXPECT_TRUE(_cache->get(oids[i]).has_value())
          << "oid index " << i << " unexpectedly evicted";
    }
}

TEST_F(l1_footer_cache_test, replace_updates_existing) {
    auto oid = create_object_id();
    _cache->put(oid, footer{});
    EXPECT_EQ(_cache->get_stats().cached_footers, 1);

    _cache->put(oid, footer{});
    EXPECT_EQ(_cache->get_stats().cached_footers, 1);

    EXPECT_TRUE(_cache->get(oid).has_value());
}

TEST_F(l1_footer_cache_test, get_updates_last_used_for_lru) {
    std::vector<object_id> oids;
    oids.reserve(default_max_size);
    for (size_t i = 0; i < default_max_size; ++i) {
        oids.push_back(create_object_id());
        _cache->put(oids.back(), footer{});
    }
    ASSERT_EQ(_cache->get_stats().cached_footers, default_max_size);

    // Touch oid[0] so it becomes most-recently-used.
    auto touched = _cache->get(oids[0]);
    ASSERT_TRUE(touched.has_value());

    // Insert a new entry; this should evict the now-oldest, which is oid[1].
    auto new_oid = create_object_id();
    _cache->put(new_oid, footer{});

    EXPECT_EQ(_cache->get_stats().cached_footers, default_max_size);
    EXPECT_TRUE(_cache->get(oids[0]).has_value())
      << "touched oid[0] should have been retained";
    EXPECT_FALSE(_cache->get(oids[1]).has_value())
      << "oid[1] should have been evicted as oldest";
    for (size_t i = 2; i < oids.size(); ++i) {
        EXPECT_TRUE(_cache->get(oids[i]).has_value());
    }
    EXPECT_TRUE(_cache->get(new_oid).has_value());
}

} // namespace cloud_topics::l1
