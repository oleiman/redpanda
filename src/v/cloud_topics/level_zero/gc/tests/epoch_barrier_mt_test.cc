/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

/*
 * Multithreaded tests for the epoch barrier.
 *
 * These tests run the epoch barrier on multiple real Seastar shards to verify
 * that cross-shard aggregation in handle_barrier works correctly.
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "cloud_topics/level_zero/gc/tests/epoch_barrier_test_utils.h"
#include "cluster/cluster_epoch_service.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <system_error>

using namespace cloud_topics;
using namespace cloud_topics::l0::gc;
using namespace cloud_topics::l0::gc::testing;

namespace {

// -- Fixture -----------------------------------------------------------------

/// Per-shard mock partition source pointers, indexed by shard ID.
/// Populated during barrier construction.
std::vector<mock_partition_source*> g_per_shard_ps;

struct epoch_barrier_mt_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    ss::sharded<epoch_barrier> barrier;
    mock_data_plane data_plane;

    ss::future<> SetUpAsync() override {
        vassert(ss::smp::count >= 2, "Need at least 2 shards");

        g_per_shard_ps.clear();
        g_per_shard_ps.resize(ss::smp::count, nullptr);

        co_await epoch_svc.start(
          [](ss::abort_source*)
            -> ss::future<std::expected<int64_t, std::error_code>> {
              co_return int64_t{1};
          });
        co_await epoch_svc.invoke_on_all(
          &cluster::cluster_epoch_service<ss::lowres_clock>::start);

        co_await barrier.start(
          std::ref(epoch_svc),
          std::ref(data_plane),
          ss::sharded_parameter([] {
              auto p = std::make_unique<mock_partition_source>();
              g_per_shard_ps[ss::this_shard_id()] = p.get();
              return p;
          }),
          ss::sharded_parameter([] {
              return std::make_unique<mock_node_source>(model::node_id{0});
          }),
          nullptr,
          ss::sharded_parameter(
            [] { return std::unique_ptr<epoch_source>{}; }));
    }

    ss::future<> TearDownAsync() override {
        co_await barrier.stop();
        co_await epoch_svc.stop();
        g_per_shard_ps.clear();
    }

    /// Add a partition on a specific shard's mock partition source.
    ss::future<> add_partition_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset committed,
      model::term_id term,
      std::optional<model::offset> lro = std::nullopt) {
        co_await ss::smp::submit_to(shard, [&, committed, term, lro, pid] {
            g_per_shard_ps[ss::this_shard_id()]
              ->partitions[make_ntp(topic, pid)]
              = mock_partition_source::partition_state{
                .last_epoch_log_offset = committed,
                .term = term,
                .is_leader = true,
                .last_reconciled_log_offset = lro,
              };
        });
    }

    /// Set LRO for a partition on a specific shard.
    ss::future<> set_lro_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset lro) {
        co_await ss::smp::submit_to(shard, [&, pid, lro] {
            auto& state = g_per_shard_ps[ss::this_shard_id()]
                            ->partitions[make_ntp(topic, pid)];
            state.last_reconciled_log_offset = lro;
        });
    }

    ss::future<bool> advance(cluster_epoch e) {
        // First call: invalidate + start drain → pending.
        co_await barrier.local().handle_barrier(e, std::nullopt);
        // Second call: drain complete (mock is sync), collect seals, check.
        auto result = co_await barrier.local().handle_barrier(e, std::nullopt);
        co_return result == epoch_barrier::barrier_status::ready;
    }

    ss::future<bool> check(cluster_epoch e) {
        auto result = co_await barrier.local().handle_barrier(e, std::nullopt);
        co_return result == epoch_barrier::barrier_status::ready;
    }
};

} // namespace

// -- Cross-shard tests -------------------------------------------------------

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_all_must_reconcile) {
    // Shard 0: partition not caught up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(100), model::term_id(1));
    // Shard 1: partition caught up
    co_await add_partition_on_shard(
      1, "t0", 1, model::offset(200), model::term_id(1), model::offset(200));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result) << "Shard 0 should block completion";

    // Advance shard 0's LRO
    co_await set_lro_on_shard(0, "t0", 0, model::offset(100));

    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_invalidate_resets_all) {
    // Put partitions on different shards, all caught up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(100), model::term_id(1), model::offset(100));
    co_await add_partition_on_shard(
      1, "t0", 1, model::offset(200), model::term_id(1), model::offset(200));

    // Complete a round
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Advance committed on shard 0 so a new round will require catching up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(300), model::term_id(1));

    // New round — invalidate should reset all shards
    result = co_await advance(cluster_epoch(6));
    EXPECT_FALSE(result) << "Shard 0 has new seal at 300, no LRO yet";

    // Catch up shard 0
    co_await set_lro_on_shard(0, "t0", 0, model::offset(300));

    result = co_await check(cluster_epoch(6));
    EXPECT_TRUE(result);
}

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_publish_safe_epoch) {
    co_await barrier.local().handle_barrier(
      cluster_epoch(0), cluster_epoch(10));

    // Verify safe_epoch is visible on all shards
    auto all_match = co_await barrier.map_reduce0(
      [](epoch_barrier& b) -> bool {
          auto safe = b.safe_epoch();
          return safe.has_value() && *safe == cluster_epoch(10);
      },
      true,
      std::logical_and<>{});

    EXPECT_TRUE(all_match) << "safe_epoch should be 10 on all shards";
}
