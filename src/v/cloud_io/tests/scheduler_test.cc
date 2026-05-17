/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/null_policy.h"
#include "cloud_io/scheduler.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

using namespace cloud_io;

// 1. Factory returns null_policy for policy_type::null
TEST_CORO(scheduler, FactoryReturnsNullPolicyForNullType) {
    scheduler s{policy_type::null, 4};
    co_await s.start();
    EXPECT_NE(s.policy_as<null_policy>(), nullptr);
    EXPECT_EQ(s.total_capacity(), 4u);
    co_await s.stop();
}

// 2. Under capacity: two admits succeed immediately, available_slots tracks
TEST_CORO(scheduler, NullAdmitImmediateUnderCapacity) {
    scheduler s{policy_type::null, 2};
    co_await s.start();

    ss::abort_source as;
    auto p1 = co_await s.admit(group_id::producer_upload, as);
    auto p2 = co_await s.admit(group_id::consumer_fetch, as);
    EXPECT_EQ(s.available_slots(), 0u);

    p1.return_all();
    EXPECT_EQ(s.available_slots(), 1u);

    co_await s.stop();
}

// 3. At capacity: second admit blocks until first permit is released
TEST_CORO(scheduler, NullAdmitBlocksAtCapacity) {
    scheduler s{policy_type::null, 1};
    co_await s.start();

    ss::abort_source as;
    auto p1 = co_await s.admit(group_id::default_group, as);

    // Second admit should queue — launch it as a background future.
    auto fut2 = s.admit(group_id::consumer_fetch, as);
    // Yield so the background fiber can reach the semaphore wait.
    co_await ss::yield();

    EXPECT_FALSE(fut2.available());
    EXPECT_EQ(s.waiters(group_id::consumer_fetch), 1u);

    // Release the first permit; second admit should now resolve.
    p1.return_all();
    auto p2 = co_await std::move(fut2);
    EXPECT_EQ(s.available_slots(), 0u);

    co_await s.stop();
}

// 4. try_admit is non-blocking: first succeeds, second returns nullopt
TEST_CORO(scheduler, NullTryAdmitNonBlocking) {
    scheduler s{policy_type::null, 1};
    co_await s.start();

    auto opt1 = s.try_admit(group_id::default_group);
    EXPECT_TRUE(opt1.has_value());

    auto opt2 = s.try_admit(group_id::consumer_fetch);
    EXPECT_FALSE(opt2.has_value());
    // No fiber should be queued — try_admit never blocks.
    EXPECT_EQ(s.waiters(group_id::consumer_fetch), 0u);

    co_await s.stop();
}

// 5. Abort source cancels a queued admit and unlinks the waiter
TEST_CORO(scheduler, NullAdmitAbortUnlinksWaiter) {
    scheduler s{policy_type::null, 1};
    co_await s.start();

    ss::abort_source never;
    auto p1 = co_await s.admit(group_id::default_group, never);

    ss::abort_source as;
    auto fut2 = s.admit(group_id::consumer_fetch, as);
    co_await ss::yield();
    EXPECT_FALSE(fut2.available());

    as.request_abort();
    auto result = co_await ss::coroutine::as_future(std::move(fut2));
    EXPECT_TRUE(result.failed());
    result.ignore_ready_future();

    EXPECT_EQ(s.waiters(group_id::consumer_fetch), 0u);

    co_await s.stop();
}

// 6. Move-constructing a permit transfers ownership; scope-exit releases once
TEST_CORO(scheduler, PermitMoveTransfersOwnership) {
    scheduler s{policy_type::null, 1};
    co_await s.start();

    ss::abort_source as;
    auto p1 = co_await s.admit(group_id::default_group, as);
    EXPECT_EQ(s.available_slots(), 0u);

    {
        // Move p1 into p2; p1 should now be a no-op on destruction.
        scheduler::permit p2{std::move(p1)};
        EXPECT_EQ(s.available_slots(), 0u);
        // p2 destructs here, releasing exactly one slot.
    }
    EXPECT_EQ(s.available_slots(), 1u);

    co_await s.stop();
}

// 7. Double return_all is safe: only one slot is released
TEST_CORO(scheduler, DoubleReleaseSafe) {
    scheduler s{policy_type::null, 2};
    co_await s.start();

    ss::abort_source as;
    auto p = co_await s.admit(group_id::default_group, as);
    EXPECT_EQ(s.available_slots(), 1u);

    p.return_all();
    EXPECT_EQ(s.available_slots(), 2u);

    // Second call must be idempotent — available_slots stays at 2.
    p.return_all();
    EXPECT_EQ(s.available_slots(), 2u);

    co_await s.stop();
}
