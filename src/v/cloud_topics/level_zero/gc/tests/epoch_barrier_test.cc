/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "cloud_topics/level_zero/gc/tests/epoch_barrier_test_utils.h"
#include "cluster/cluster_epoch_service.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <system_error>

using namespace cloud_topics;
using namespace cloud_topics::l0::gc;
using namespace cloud_topics::l0::gc::testing;

namespace {

// -- Fixture -----------------------------------------------------------------

struct epoch_barrier_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    ss::sharded<epoch_barrier> barrier;
    mock_data_plane data_plane;
    mock_partition_source* ps{nullptr};

    ss::future<> SetUpAsync() override {
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
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_partition_source>();
              if (ss::this_shard_id() == 0) {
                  ps = p.get();
              }
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
    }

    void add_partition(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset committed,
      model::term_id term,
      std::optional<model::offset> lro = std::nullopt) {
        ps->partitions[make_ntp(topic, pid)]
          = mock_partition_source::partition_state{
            .last_epoch_log_offset = committed,
            .term = term,
            .is_leader = true,
            .last_reconciled_log_offset = lro,
          };
    }

    void set_lro(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset lro) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.last_reconciled_log_offset = lro;
    }

    void set_term(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::term_id term) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.term = term;
    }

    void set_last_placeholder(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset offset) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.last_epoch_log_offset = offset;
    }

    void set_leader(
      const ss::sstring& topic, model::partition_id::type pid, bool is_leader) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.is_leader = is_leader;
    }

    void
    remove_partition(const ss::sstring& topic, model::partition_id::type pid) {
        ps->partitions.erase(make_ntp(topic, pid));
    }

    /// Start a new barrier round. The first call kicks off the drain and
    /// returns pending; the second call (drain done since mock is sync)
    /// collects seal points and checks reconciliation.
    ss::future<bool> advance(cluster_epoch e) {
        // First call: invalidate + start drain → pending.
        co_await barrier.local().handle_barrier(e, std::nullopt);
        // Second call: drain complete, collect seals, check.
        auto result = co_await barrier.local().handle_barrier(e, std::nullopt);
        co_return result == epoch_barrier::barrier_status::ready;
    }

    /// Single poll of an existing round. Does NOT handle the case where
    /// a stale reset triggers a re-drain (use advance() for that).
    ss::future<bool> check(cluster_epoch e) {
        auto result = co_await barrier.local().handle_barrier(e, std::nullopt);
        co_return result == epoch_barrier::barrier_status::ready;
    }
};

} // namespace

// -- Data loss prevention tests ----------------------------------------------

TEST_F_CORO(epoch_barrier_test, poll_drain_false_when_lro_not_caught_up) {
    // Partition committed=100, term=1, lro=nullopt
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);
}

TEST_F_CORO(epoch_barrier_test, seal_points_captured_at_drain_time) {
    // Partition committed=100 at drain time
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Simulate more writes arriving after drain — committed advances to 200
    set_last_placeholder("t0", 0, model::offset(200));

    // LRO catches up to the original seal point (100), not 200
    set_lro("t0", 0, model::offset(100));

    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result) << "Should complete because LRO >= seal (100), "
                           "not current committed (200)";
}

TEST_F_CORO(epoch_barrier_test, term_mismatch_refreshes_seal_forward) {
    // Partition committed=100, term=1
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Term changes to 2, committed advances to 150
    set_term("t0", 0, model::term_id(2));
    set_last_placeholder("t0", 0, model::offset(150));

    // Seal refreshes — the old lro=100 won't be sufficient
    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "Seal should have refreshed to 150";

    // Now LRO catches up to the new seal point
    set_lro("t0", 0, model::offset(150));

    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(epoch_barrier_test, new_leaders_get_seal_points) {
    // Only t0/0 is a leader initially
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);

    // New leader appears on t0/1
    add_partition("t0", 1, model::offset(50), model::term_id(1));

    // t0/0 catches up, but t0/1 is discovered as a new leader
    set_lro("t0", 0, model::offset(100));

    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "New leader t0/1 should block completion";

    // t0/1 catches up
    set_lro("t0", 1, model::offset(50));

    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result);
}

// -- Resilience to leadership/term changes -----------------------------------

TEST_F_CORO(epoch_barrier_test, lost_leadership_resets_and_redrains) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));
    add_partition("t0", 1, model::offset(200), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // t0/0 loses leadership — seal table goes stale
    set_leader("t0", 0, false);
    set_lro("t0", 1, model::offset(200));

    // First poll detects stale, resets round.
    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "Stale seal table should return false";
    EXPECT_EQ(data_plane.drain_count, 1);

    // Second poll starts a new drain (async).
    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result);
    EXPECT_EQ(data_plane.drain_count, 2) << "Should have re-drained";

    // Third poll: drain complete, fresh seal table without t0/0.
    // t0/1 is caught up at 200 — should succeed.
    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result) << "Fresh seal table without t0/0 should succeed";
}

TEST_F_CORO(epoch_barrier_test, removed_partition_resets_and_redrains) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));
    add_partition("t0", 1, model::offset(200), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // t0/0 is removed — seal table goes stale
    remove_partition("t0", 0);
    set_lro("t0", 1, model::offset(200));

    // Stale detected, round reset.
    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "Stale seal table should return false";

    // Starts re-drain (async).
    result = co_await check(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 2) << "Should have re-drained";
    EXPECT_FALSE(result);

    // Drain complete, fresh seal table without t0/0.
    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result)
      << "Fresh seal table without removed partition should succeed";
}

TEST_F_CORO(epoch_barrier_test, term_change_never_regresses_seal) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Term bumps to 2, committed advances to 150
    set_term("t0", 0, model::term_id(2));
    set_last_placeholder("t0", 0, model::offset(150));

    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "Seal should have refreshed forward to 150";

    // LRO at old committed (100) is not sufficient
    set_lro("t0", 0, model::offset(100));

    result = co_await check(cluster_epoch(5));
    EXPECT_FALSE(result) << "LRO=100 < seal=150, should not complete";

    // LRO catches up to the refreshed seal
    set_lro("t0", 0, model::offset(150));

    result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result);
}

// -- Making progress tests ---------------------------------------------------

TEST_F_CORO(epoch_barrier_test, all_lros_caught_up_returns_true) {
    // All three partitions already caught up at drain time
    add_partition(
      "t0", 0, model::offset(100), model::term_id(1), model::offset(100));
    add_partition(
      "t0", 1, model::offset(200), model::term_id(1), model::offset(200));
    add_partition(
      "t1", 0, model::offset(50), model::term_id(1), model::offset(50));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result) << "All LROs >= committed, should pass immediately";
}

TEST_F_CORO(epoch_barrier_test, invalidate_resets_round_redrain) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    // Complete a full round for epoch 5
    co_await advance(cluster_epoch(5));
    set_lro("t0", 0, model::offset(100));
    auto result = co_await check(cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // Committed advances
    set_last_placeholder("t0", 0, model::offset(200));

    // New round for epoch 6 — should re-drain
    result = co_await advance(cluster_epoch(6));
    EXPECT_EQ(data_plane.drain_count, 2);
    EXPECT_FALSE(result);

    // New seal point should use current committed (200)
    set_lro("t0", 0, model::offset(200));
    result = co_await check(cluster_epoch(6));
    EXPECT_TRUE(result);
}

TEST_F_CORO(epoch_barrier_test, repeated_poll_drain_no_redrain) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    co_await advance(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);

    // Second poll_drain for same epoch should NOT re-drain
    co_await check(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);

    // Third poll_drain for same epoch — still no re-drain
    co_await check(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);
}

TEST_F_CORO(epoch_barrier_test, no_partitions_returns_true) {
    // No partitions at all — vacuously true
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(epoch_barrier_test, publish_safe_epoch_stored) {
    co_await barrier.local().handle_barrier(
      cluster_epoch(0), cluster_epoch(42));

    auto safe = barrier.local().safe_epoch();
    ASSERT_TRUE_CORO(safe.has_value());
    EXPECT_EQ(*safe, cluster_epoch(42));
}

// -- Loop integration tests --------------------------------------------------

// Fixture that wires up the leader loop with a mock epoch_source and a
// single-node mock, exercising the full path: loop → fan_out →
// advance_local → handle_barrier → seal points → publish.
struct epoch_barrier_loop_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    ss::sharded<epoch_barrier> barrier;
    mock_data_plane data_plane;
    mock_partition_source* ps{nullptr};
    mock_epoch_source* es{nullptr};

    ss::future<> SetUpAsync() override {
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
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_partition_source>();
              if (ss::this_shard_id() == 0) {
                  ps = p.get();
              }
              return p;
          }),
          ss::sharded_parameter([] {
              return std::make_unique<mock_node_source>(model::node_id{0});
          }),
          nullptr,
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_epoch_source>();
              if (ss::this_shard_id() == 0) {
                  es = p.get();
              }
              return std::unique_ptr<epoch_source>(std::move(p));
          }));
    }

    ss::future<> TearDownAsync() override {
        co_await barrier.stop();
        co_await epoch_svc.stop();
    }
};

TEST_F_CORO(epoch_barrier_loop_test, loop_converges_single_node) {
    // Partition already reconciled.
    ps->partitions[make_ntp("t0", 0)] = mock_partition_source::partition_state{
      .last_epoch_log_offset = model::offset(100),
      .term = model::term_id(1),
      .is_leader = true,
      .last_reconciled_log_offset = model::offset(100),
    };

    es->candidate = 5;
    co_await barrier.local().set_leader(true);

    // The loop should converge: compute candidate → fan_out → drain →
    // seal → reconciled → publish. With poll_interval=2s, allow up to 10s.
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this]() -> ss::future<bool> {
        co_return barrier.local().safe_epoch().has_value();
    });
    EXPECT_EQ(*barrier.local().safe_epoch(), cluster_epoch(5));
    EXPECT_GE(data_plane.drain_count.load(), 1);
}

TEST_F_CORO(epoch_barrier_loop_test, loop_waits_for_reconciliation) {
    // Partition not yet reconciled.
    ps->partitions[make_ntp("t0", 0)] = mock_partition_source::partition_state{
      .last_epoch_log_offset = model::offset(100),
      .term = model::term_id(1),
      .is_leader = true,
    };

    es->candidate = 5;
    co_await barrier.local().set_leader(true);

    // Give the loop time to start polling.
    co_await ss::sleep(3s);

    // Should NOT have converged yet — LRO is nullopt.
    EXPECT_FALSE(barrier.local().safe_epoch().has_value());

    // Now reconcile.
    ps->partitions[make_ntp("t0", 0)].last_reconciled_log_offset
      = model::offset(100);

    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this]() -> ss::future<bool> {
        co_return barrier.local().safe_epoch().has_value();
    });
    EXPECT_EQ(*barrier.local().safe_epoch(), cluster_epoch(5));
}
