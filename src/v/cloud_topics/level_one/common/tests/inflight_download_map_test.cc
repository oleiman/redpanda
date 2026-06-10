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

#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>

using namespace cloud_topics::l1;
using namespace std::chrono_literals;

namespace {

inflight_download_map::leader_guard
take_leader(inflight_download_map& map, std::filesystem::path key) {
    ss::abort_source unused;
    auto result = map.join_or_lead(std::move(key), unused);
    return std::get<inflight_download_map::leader_guard>(std::move(result));
}

} // namespace

TEST(InflightDownloadMapTest, ColdKeyYieldsLeader) {
    inflight_download_map map;
    ss::abort_source as;
    EXPECT_EQ(map.in_flight(), 0u);

    auto result = map.join_or_lead("/a", as);
    EXPECT_TRUE(
      std::holds_alternative<inflight_download_map::leader_guard>(result));
    EXPECT_EQ(map.in_flight(), 1u);
}

TEST(InflightDownloadMapTest, SecondJoinOnSameKeyYieldsMergeFuture) {
    inflight_download_map map;
    auto leader = take_leader(map, "/a");
    EXPECT_EQ(map.in_flight(), 1u);

    ss::abort_source as;
    auto second = map.join_or_lead("/a", as);
    EXPECT_TRUE(
      std::holds_alternative<inflight_download_map::merge_future>(second));
    // Map size unchanged: the second caller didn't insert.
    EXPECT_EQ(map.in_flight(), 1u);
}

TEST_CORO(InflightDownloadMapTest, LeaderResolveSuccessPublishesNullopt) {
    inflight_download_map map;
    auto leader = take_leader(map, "/a");

    ss::abort_source as;
    auto second = map.join_or_lead("/a", as);
    auto& fut = std::get<inflight_download_map::merge_future>(second);

    leader.resolve(std::nullopt);
    {
        auto disposable = std::move(leader);
        // disposable destroyed here -> publishes nullopt
    }

    auto outcome = co_await std::move(fut);
    ASSERT_FALSE_CORO(outcome.has_value());
}

TEST_CORO(InflightDownloadMapTest, LeaderResolveErrorPublishesErrc) {
    inflight_download_map map;
    auto leader = take_leader(map, "/a");

    ss::abort_source as;
    auto second = map.join_or_lead("/a", as);
    auto& fut = std::get<inflight_download_map::merge_future>(second);

    leader.resolve(io::errc::cloud_op_error);
    {
        auto disposable = std::move(leader);
    }

    auto outcome = co_await std::move(fut);
    ASSERT_TRUE_CORO(outcome.has_value());
    ASSERT_EQ_CORO(*outcome, io::errc::cloud_op_error);
}

TEST_CORO(
  InflightDownloadMapTest, LeaderDroppedWithoutResolvePublishesDefaultErrc) {
    inflight_download_map map;

    ss::abort_source as;
    auto second_fut = [&]() {
        auto leader = take_leader(map, "/a");
        auto second = map.join_or_lead("/a", as);
        // Drop leader without calling resolve(): dtor publishes the
        // default failure errc.
        return std::move(std::get<inflight_download_map::merge_future>(second));
    }();

    auto outcome = co_await std::move(second_fut);
    ASSERT_TRUE_CORO(outcome.has_value());
    ASSERT_EQ_CORO(*outcome, io::errc::file_io_error);
}

TEST(InflightDownloadMapTest, LeaderDestructionErasesEntry) {
    inflight_download_map map;
    {
        auto leader = take_leader(map, "/a");
        EXPECT_EQ(map.in_flight(), 1u);
    }
    EXPECT_EQ(map.in_flight(), 0u);
}

TEST_CORO(InflightDownloadMapTest, MoveConstructTransfersEngagement) {
    inflight_download_map map;
    auto source = take_leader(map, "/a");

    ss::abort_source as;
    auto second = map.join_or_lead("/a", as);
    auto& fut = std::get<inflight_download_map::merge_future>(second);

    auto target = std::move(source);
    target.resolve(std::nullopt);
    // source is disengaged: destroying it is a no-op. target's dtor
    // publishes when this scope ends.
    {
        auto disposable = std::move(target);
    }

    auto outcome = co_await std::move(fut);
    ASSERT_FALSE_CORO(outcome.has_value());
}

TEST_CORO(InflightDownloadMapTest, MoveAssignReleasesPriorEntry) {
    inflight_download_map map;
    auto a = take_leader(map, "/a");
    auto b = take_leader(map, "/b");
    ASSERT_EQ_CORO(map.in_flight(), 2u);

    // Awaiter on /a will receive the default-failure outcome that
    // a's release publishes when it's overwritten by move-assignment.
    ss::abort_source as;
    auto join_a = map.join_or_lead("/a", as);
    auto& fut_a = std::get<inflight_download_map::merge_future>(join_a);

    a = std::move(b);
    // a held /a, b held /b. After move-assign:
    //   - /a entry released (default errc::file_io_error to awaiter)
    //   - a now owns /b
    //   - b is disengaged
    ASSERT_EQ_CORO(map.in_flight(), 1u);

    auto outcome_a = co_await std::move(fut_a);
    ASSERT_TRUE_CORO(outcome_a.has_value());
    ASSERT_EQ_CORO(*outcome_a, io::errc::file_io_error);

    // The new /b entry hasn't been resolved yet; drop it.
    {
        auto disposable = std::move(a);
    }
    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

TEST(InflightDownloadMapTest, AtCapacityReturnsSentinel) {
    inflight_download_map map(/*max_entries=*/2);
    auto a = take_leader(map, "/a");
    auto b = take_leader(map, "/b");
    EXPECT_EQ(map.in_flight(), 2u);

    ss::abort_source as;
    auto third = map.join_or_lead("/c", as);
    EXPECT_TRUE(
      std::holds_alternative<inflight_download_map::at_capacity_t>(third));
    // Map size unchanged: the at-capacity caller didn't insert.
    EXPECT_EQ(map.in_flight(), 2u);
}

TEST(InflightDownloadMapTest, AtCapacityYieldsLeaderAfterRelease) {
    inflight_download_map map(/*max_entries=*/1);
    ss::abort_source as;
    {
        auto a = take_leader(map, "/a");
        auto blocked = map.join_or_lead("/b", as);
        EXPECT_TRUE(
          std::holds_alternative<inflight_download_map::at_capacity_t>(
            blocked));
    }
    // After a was released, /b can now lead.
    EXPECT_EQ(map.in_flight(), 0u);
    auto retry = map.join_or_lead("/b", as);
    EXPECT_TRUE(
      std::holds_alternative<inflight_download_map::leader_guard>(retry));
}

TEST_CORO(InflightDownloadMapTest, MergerAbortPropagatesToFuture) {
    inflight_download_map map;
    auto leader = take_leader(map, "/a");

    ss::abort_source as;
    auto second = map.join_or_lead("/a", as);
    auto& fut = std::get<inflight_download_map::merge_future>(second);

    // Abort the merger's abort_source before the leader resolves; the
    // shared_future is wired to abort and should yield an exception.
    as.request_abort();

    auto wrapped = co_await ss::coroutine::as_future(std::move(fut));
    ASSERT_TRUE_CORO(wrapped.failed());
    wrapped.ignore_ready_future();

    // Leader still has to publish; let it be dropped cleanly.
    {
        auto disposable = std::move(leader);
    }
}
