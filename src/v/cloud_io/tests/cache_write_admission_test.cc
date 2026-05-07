/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/cache_write_admission.h"
#include "config/mock_property.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <vector>

using namespace std::chrono_literals;

namespace {

config::binding<uint64_t> fixed_binding(uint64_t v) {
    static thread_local std::vector<config::mock_property<uint64_t>> store;
    store.emplace_back(v);
    return store.back().bind();
}

} // namespace

TEST_CORO(CacheWriteAdmission, CtorReportsConfiguredMax) {
    cloud_io::cache_write_admission admission(
      fixed_binding(8 * 1024 * 1024), fixed_binding(64 * 1024));
    EXPECT_EQ(admission.max_bytes(), 8 * 1024 * 1024);
    EXPECT_EQ(admission.available_bytes(), 8 * 1024 * 1024);
    co_return;
}

TEST_CORO(CacheWriteAdmission, BasicAdmitReleaseCycle) {
    cloud_io::cache_write_admission admission(
      fixed_binding(8 * 1024 * 1024), fixed_binding(64 * 1024));
    ss::abort_source as;

    {
        auto units = co_await admission.wait(1 * 1024 * 1024, as);
        EXPECT_EQ(admission.available_bytes(), 7 * 1024 * 1024);
        EXPECT_EQ(units.count(), 1 * 1024 * 1024);
    }
    EXPECT_EQ(admission.available_bytes(), 8 * 1024 * 1024);
}

TEST_CORO(CacheWriteAdmission, RoundsUpToMinReservation) {
    cloud_io::cache_write_admission admission(
      fixed_binding(8 * 1024 * 1024), fixed_binding(1 * 1024 * 1024));
    ss::abort_source as;

    auto units = co_await admission.wait(64 * 1024, as);
    // 64 KiB request rounded up to the 1 MiB minimum reservation.
    EXPECT_EQ(units.count(), 1 * 1024 * 1024);
}

TEST_CORO(CacheWriteAdmission, CapsRequestsAtTotalCapacity) {
    cloud_io::cache_write_admission admission(
      fixed_binding(2 * 1024 * 1024), fixed_binding(64 * 1024));
    ss::abort_source as;

    auto units = co_await admission.wait(8 * 1024 * 1024, as);
    // 8 MiB request capped at the 2 MiB total capacity.
    EXPECT_EQ(units.count(), 2 * 1024 * 1024);
}

TEST_CORO(CacheWriteAdmission, BlocksUntilUnitsReleased) {
    cloud_io::cache_write_admission admission(
      fixed_binding(2 * 1024 * 1024), fixed_binding(64 * 1024));
    ss::abort_source as;

    auto first = co_await admission.wait(2 * 1024 * 1024, as);
    EXPECT_EQ(admission.available_bytes(), 0);
    EXPECT_EQ(admission.waiters(), 0);

    auto second_fut = admission.wait(1 * 1024 * 1024, as);
    co_await ss::sleep(50ms);
    EXPECT_FALSE(second_fut.available());
    EXPECT_EQ(admission.waiters(), 1);

    first.return_all();
    auto second = co_await std::move(second_fut);
    EXPECT_EQ(second.count(), 1 * 1024 * 1024);
}

TEST_CORO(CacheWriteAdmission, AbortSourceCancelsWait) {
    cloud_io::cache_write_admission admission(
      fixed_binding(2 * 1024 * 1024), fixed_binding(64 * 1024));
    ss::abort_source as;

    auto first = co_await admission.wait(2 * 1024 * 1024, as);
    auto second_fut = admission.wait(1 * 1024 * 1024, as);

    co_await ss::sleep(5ms);
    EXPECT_FALSE(second_fut.available());

    as.request_abort();

    auto fut = co_await ss::coroutine::as_future(std::move(second_fut));
    EXPECT_TRUE(fut.failed());
    // Consume the stored exception so it isn't reported as ignored.
    auto ex = fut.get_exception();
    EXPECT_TRUE(static_cast<bool>(ex));
}
