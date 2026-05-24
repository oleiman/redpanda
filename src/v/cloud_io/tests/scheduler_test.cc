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
#include "config/min_share_group_target.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

#include <vector>

using namespace cloud_io;

TEST_CORO(scheduler, NullAdmitAlwaysSucceeds) {
    scheduler s{policy_type::null, 2};
    EXPECT_EQ(s.total_capacity(), 2u);

    ss::abort_source as;
    co_await s.admit(group_id::producer_upload, as);
    co_await s.admit(group_id::consumer_fetch, as);
    co_await s.admit(group_id::default_group, as);

    co_await s.stop();
}

TEST_CORO(scheduler, NullTryAdmitAlwaysSucceeds) {
    scheduler s{policy_type::null, 1};

    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_TRUE(s.try_admit(group_id::consumer_fetch));

    co_await s.stop();
}

// The tests below construct a real min_share_policy through the scheduler
// shell to exercise the integration: factory wiring, has_waiters
// aggregation, the stop()-then-admit drain, and that the configured
// per-group reservation flows through to admission behavior. Policy
// internals are exercised by min_share_policy_test.cc.

TEST_CORO(scheduler, MinShareHasWaitersReflectsQueueState) {
    scoped_config cfg;
    cfg.get("cloud_io_scheduler_min_share")
      .set_value(std::vector<config::min_share_group_target>{});

    scheduler s{policy_type::min_share, 1};
    EXPECT_FALSE(s.has_waiters());

    ss::abort_source as;
    co_await s.admit(group_id::default_group, as);
    EXPECT_FALSE(s.has_waiters());

    auto queued = s.admit(group_id::default_group, as);
    co_await ss::yield();
    EXPECT_TRUE(s.has_waiters());

    s.release(group_id::default_group);
    co_await std::move(queued);
    EXPECT_FALSE(s.has_waiters());

    s.release(group_id::default_group);
    co_await s.stop();
}

TEST_CORO(scheduler, MinShareStopDrainsAndRejectsAdmits) {
    scoped_config cfg;
    cfg.get("cloud_io_scheduler_min_share")
      .set_value(std::vector<config::min_share_group_target>{});

    scheduler s{policy_type::min_share, 1};

    ss::abort_source as;
    co_await s.admit(group_id::default_group, as);

    auto queued = s.admit(group_id::default_group, as);
    co_await ss::yield();

    co_await s.stop();

    auto r = co_await ss::coroutine::as_future(std::move(queued));
    EXPECT_TRUE(r.failed());
    r.ignore_ready_future();

    EXPECT_FALSE(s.try_admit(group_id::default_group));
    auto r2 = co_await ss::coroutine::as_future(
      s.admit(group_id::default_group, as));
    EXPECT_TRUE(r2.failed());
    r2.ignore_ready_future();

    s.release(group_id::default_group);
}

TEST_CORO(scheduler, MinShareReservationsRespectConfiguredTargets) {
    // capacity=4, producer_upload reserved=2 → shared=2. The shared pool
    // is exhausted by default_group, but producer_upload can still admit
    // twice from its dedicated lane.
    scoped_config cfg;
    cfg.get("cloud_io_scheduler_min_share")
      .set_value(
        std::vector<config::min_share_group_target>{
          {.group_name = "producer_upload", .target_reserved = 2},
          {.group_name = "consumer_fetch", .target_reserved = 0},
          {.group_name = "default_group", .target_reserved = 0},
        });

    scheduler s{policy_type::min_share, 4};

    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_FALSE(s.try_admit(group_id::default_group));

    EXPECT_TRUE(s.try_admit(group_id::producer_upload));
    EXPECT_TRUE(s.try_admit(group_id::producer_upload));
    EXPECT_FALSE(s.try_admit(group_id::producer_upload));

    s.release(group_id::default_group);
    s.release(group_id::default_group);
    s.release(group_id::producer_upload);
    s.release(group_id::producer_upload);
    co_await s.stop();
}
