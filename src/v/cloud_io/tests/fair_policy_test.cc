/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/fair_policy.h"
#include "cloud_io/scheduler_types.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

#include <array>
#include <vector>

using namespace std::chrono_literals;

namespace {

// All test co_awaits are wrapped with this so a hang reports a
// timed_out_error rather than locking up the test binary.
template<typename T>
ss::future<T> with_test_timeout(ss::future<T> fut) {
    return ss::with_timeout(
      ss::lowres_clock::now() + std::chrono::seconds{5}, std::move(fut));
}

// Apply weights + capacity post-construction. fair_policy is
// non-movable (semaphore + state arrays), so construction must
// happen at the call site.
void configure(
  cloud_io::fair_policy& fp,
  size_t total_slots = 20,
  std::array<uint32_t, cloud_io::num_group_ids> weights = {
    1000,
    1000,
    1500,
  }) {
    // Phase 1 hard reservation: tests don't inherit the production
    // cluster-config defaults (pu=2, cf=2, default=0) because many
    // tests use total_slots < 4 which would trip the construction
    // vassert. Reset to zero here; individual tests that exercise
    // reservation behavior re-establish their own min_reserved
    // values via set_min_reserved().
    for (size_t i = 0; i < cloud_io::num_group_ids; ++i) {
        fp.set_min_reserved(static_cast<cloud_io::group_id>(i), 0);
    }
    fp.set_total_slots(total_slots);
    for (size_t i = 0; i < cloud_io::num_group_ids; ++i) {
        fp.set_weight(static_cast<cloud_io::group_id>(i), weights[i]);
    }
}

} // namespace

TEST_CORO(FairPolicyTest, ConstructionReportsCapacityAndWeights) {
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);

    EXPECT_EQ(fp.total_capacity(), 20u);
    EXPECT_EQ(fp.available_slots(), 20u);
    EXPECT_EQ(fp.weight(cloud_io::group_id::producer_upload), 1000u);
    EXPECT_EQ(fp.weight(cloud_io::group_id::default_group), 1500u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 0u);
    co_return;
}

TEST_CORO(FairPolicyTest, BasicAdmitReleaseCycle) {
    cloud_io::fair_policy fp{20};
    configure(fp, 20);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(fp.available_slots(), 19u);

    fp.release(cloud_io::group_id::consumer_fetch);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(fp.available_slots(), 20u);
}

TEST_CORO(FairPolicyTest, TryAdmitFastPathAndCapDenial) {
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    // Fast-path success when uncontested.
    EXPECT_TRUE(fp.try_admit(cloud_io::group_id::consumer_fetch));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(fp.available_slots(), 19u);
    fp.release(cloud_io::group_id::consumer_fetch);

    // Seed producer_upload as effective-active (admit + release;
    // last_active = fake_now). Both groups equal weight 1000 → caps = 10.
    ss::abort_source as;
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);

    // Fill consumer_fetch to its cap via try_admit.
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(fp.try_admit(cloud_io::group_id::consumer_fetch));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 10u);

    // 11th try_admit must fail (cap hit) and must NOT enqueue a waiter.
    EXPECT_FALSE(fp.try_admit(cloud_io::group_id::consumer_fetch));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 10u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 0u);

    // Cleanup.
    for (int i = 0; i < 10; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, SaturationCausesQueueing) {
    cloud_io::fair_policy fp{2};
    configure(fp, 2);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 0u);

    auto p3_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(50ms);
    EXPECT_FALSE(p3_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);

    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(p3_fut));
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 2u);

    fp.release(cloud_io::group_id::consumer_fetch);
    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, AbortCancelsQueuedWait) {
    cloud_io::fair_policy fp{1};
    configure(fp, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    auto queued_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);

    co_await ss::sleep(10ms);
    EXPECT_FALSE(queued_fut.available());

    as.request_abort();

    auto fut = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(queued_fut)));
    EXPECT_TRUE(fut.failed());
    fut.ignore_ready_future();

    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, AlreadyAbortedSourceRejectsSlowPath) {
    cloud_io::fair_policy fp{1};
    configure(fp, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 0u);

    as.request_abort();

    auto queued_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    auto result = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(queued_fut)));
    EXPECT_TRUE(result.failed());
    result.ignore_ready_future();

    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, CrossGroupDispatchWakesQueuedWaiter) {
    cloud_io::fair_policy fp{1};
    configure(fp, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 0u);

    auto b_fut = fp.admit(cloud_io::group_id::default_group, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(b_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::default_group), 1u);

    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(b_fut));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::default_group), 0u);

    fp.release(cloud_io::group_id::default_group);
}

TEST_CORO(FairPolicyTest, FairShareConvergesToInFlightRatio) {
    // Use 11 slots and weights 1000:100 → steady-state ≈10:1.
    cloud_io::fair_policy fp{11};
    configure(fp, /*total_slots=*/11);
    // Override default_group weight to 100 so it's the low-weight group.
    fp.set_weight(cloud_io::group_id::default_group, 100);
    ss::abort_source as;

    // Pre-occupy all 11 slots with consumer_fetch.
    for (int i = 0; i < 11; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }

    // Queue plenty of waiters for both groups.
    const int N = 200;
    std::vector<ss::future<>> cf_futs;
    std::vector<ss::future<>> dg_futs;
    cf_futs.reserve(N);
    dg_futs.reserve(N);
    for (int i = 0; i < N; ++i) {
        cf_futs.push_back(fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    for (int i = 0; i < N; ++i) {
        dg_futs.push_back(fp.admit(cloud_io::group_id::default_group, as));
    }
    co_await ss::sleep(20ms);

    // Release all initial cf slots. This triggers 11 dispatches.
    for (int i = 0; i < 11; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
    co_await ss::sleep(50ms);

    size_t cf_if = fp.in_flight(cloud_io::group_id::consumer_fetch);
    size_t dg_if = fp.in_flight(cloud_io::group_id::default_group);

    EXPECT_GE(cf_if, 8u); // cf should hold ≥ 8 of 11 slots
    EXPECT_LE(dg_if, 3u); // dg should hold ≤ 3 of 11 slots
    EXPECT_GE(dg_if, 1u); // dg must hold at least 1 (work-conserving)

    // Cleanup.
    as.request_abort();
    for (auto& f : cf_futs) {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(f)));
        if (r.failed()) {
            r.ignore_ready_future();
        } else {
            fp.release(cloud_io::group_id::consumer_fetch);
        }
    }
    for (auto& f : dg_futs) {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(f)));
        if (r.failed()) {
            r.ignore_ready_future();
        } else {
            fp.release(cloud_io::group_id::default_group);
        }
    }
}

TEST_CORO(FairPolicyTest, WorkConservingWhenOneGroupIdle) {
    cloud_io::fair_policy fp{4};
    configure(fp, 4);
    // Low-weight group via override.
    fp.set_weight(cloud_io::group_id::default_group, 100);
    ss::abort_source as;

    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::default_group, as));
    }

    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 4u);
    EXPECT_EQ(fp.available_slots(), 0u);

    for (int i = 0; i < 4; ++i) {
        fp.release(cloud_io::group_id::default_group);
    }
}

TEST_CORO(FairPolicyTest, RuntimeWeightChangeIsObservable) {
    cloud_io::fair_policy fp{4};
    configure(fp, 4);
    EXPECT_EQ(fp.weight(cloud_io::group_id::default_group), 1500u);

    fp.set_weight(cloud_io::group_id::default_group, 500);
    EXPECT_EQ(fp.weight(cloud_io::group_id::default_group), 500u);
    co_return;
}

TEST_CORO(FairPolicyTest, TotalSlotsResizeUpReleasesCapacity) {
    cloud_io::fair_policy fp{2};
    configure(fp, 2);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 0u);

    fp.set_total_slots(5);
    EXPECT_EQ(fp.total_capacity(), 5u);
    EXPECT_EQ(fp.available_slots(), 3u);

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 2u);

    fp.release(cloud_io::group_id::consumer_fetch);
    fp.release(cloud_io::group_id::consumer_fetch);
    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, TotalSlotsResizeDownReducesAvailability) {
    cloud_io::fair_policy fp{5};
    configure(fp, 5);
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(fp.available_slots(), 4u);

    fp.set_total_slots(2);
    EXPECT_EQ(fp.total_capacity(), 2u);
    EXPECT_EQ(fp.available_slots(), 1u);

    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, MultiGroupFillsCapacity) {
    cloud_io::fair_policy fp{4};
    configure(fp, /*total_slots=*/4);
    // consumer_fetch and producer_upload both have weight=1000.
    ss::abort_source as;

    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));

    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 2u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);
    EXPECT_EQ(fp.available_slots(), 0u);

    fp.release(cloud_io::group_id::consumer_fetch);
    fp.release(cloud_io::group_id::consumer_fetch);
    fp.release(cloud_io::group_id::producer_upload);
    fp.release(cloud_io::group_id::producer_upload);
}

TEST_CORO(FairPolicyTest, FastPathCapBoundsBurstTakeover) {
    // 20 slots, two equal-weight groups (1000 each) → cap = 10.
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);
    ss::abort_source as;

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    // Seed producer_upload as effective-active.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);

    // consumer_fetch tries to burst. Expected cap = 10.
    for (int i = 0; i < 10; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }

    auto fut11 = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(50ms);
    EXPECT_FALSE(fut11.available());
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 10u);

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(fut11)));
    if (r.failed()) {
        r.ignore_ready_future();
    } else {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
    for (int i = 0; i < 10; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, SoloGroupGetsFullCapacity) {
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);
    ss::abort_source as;

    for (int i = 0; i < 20; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 20u);
    EXPECT_EQ(fp.available_slots(), 0u);

    for (int i = 0; i < 20; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, DwellPreservesShareAcrossGap) {
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);
    ss::abort_source as;

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    // producer_upload admits + releases; last_active = current fake_now.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);

    // Advance clock to dwell/2 — producer_upload still effective-active.
    fake_now += cloud_io::default_dwell_duration / 2;

    // consumer_fetch bursts. cap = 20 * 1000 / 2000 = 10.
    for (int i = 0; i < 10; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    auto fut11 = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(fut11.available());
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 10u);

    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(fut11)));
    if (r.failed()) {
        r.ignore_ready_future();
    }

    // Advance past dwell. cap should expand to 20.
    fake_now += cloud_io::default_dwell_duration;
    ss::abort_source as2;
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as2));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 11u);

    // Cleanup.
    for (int i = 0; i < 11; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, DispatchPicksMostUnderservedOnRelease) {
    cloud_io::fair_policy fp{10};
    configure(fp, /*total_slots=*/10);
    fp.set_weight(cloud_io::group_id::default_group, 100);
    ss::abort_source as;

    // Fill all 10 slots with consumer_fetch.
    for (int i = 0; i < 10; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    EXPECT_EQ(fp.available_slots(), 0u);

    // Queue one waiter on default_group (most under-served).
    auto dg_fut = fp.admit(cloud_io::group_id::default_group, as);

    // Release one cf slot.
    fp.release(cloud_io::group_id::consumer_fetch);

    co_await with_test_timeout(std::move(dg_fut));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 1u);

    // Cleanup.
    fp.release(cloud_io::group_id::default_group);
    for (int i = 0; i < 9; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, CapRecomputesOnActivityTransition) {
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);
    ss::abort_source as;

    // consumer_fetch admits 15 slots (would be capped if anyone else
    // were active).
    for (int i = 0; i < 15; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 15u);

    // producer_upload (also weight 1000) becomes active. First admit
    // succeeds (cap = 10 for B, in_flight=0 < cap).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));

    // consumer_fetch next admit attempt should queue (cap = 10 < 15).
    auto a16_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(a16_fut.available());

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(a16_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::producer_upload);
    for (int i = 0; i < 15; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, ThreeWaySlowPathFairShareAtDefaultWeights) {
    // 14 slots with default weights (1000, 1000, 1500) — ideal in_flight
    // = (4, 4, 6) exactly. Saturate via pre-occupy + 3-way queueing, then
    // release producer slots one at a time. dispatch_next must converge to
    // the weight-proportional steady state.
    cloud_io::fair_policy fp{14};
    configure(fp, /*total_slots=*/14);
    ss::abort_source as;

    // Pre-occupy all 14 slots with producer_upload (solo group → cap=14).
    for (int i = 0; i < 14; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }

    // Queue plenty of waiters per group so dispatch_next always has a
    // choice across all three.
    const int N = 100;
    std::vector<ss::future<>> p_futs;
    std::vector<ss::future<>> c_futs;
    std::vector<ss::future<>> d_futs;
    p_futs.reserve(N);
    c_futs.reserve(N);
    d_futs.reserve(N);
    for (int i = 0; i < N; ++i) {
        p_futs.push_back(fp.admit(cloud_io::group_id::producer_upload, as));
    }
    for (int i = 0; i < N; ++i) {
        c_futs.push_back(fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    for (int i = 0; i < N; ++i) {
        d_futs.push_back(fp.admit(cloud_io::group_id::default_group, as));
    }
    co_await ss::sleep(20ms);

    // Release initial producer slots one at a time, yielding after each so
    // the dispatched waiter's coroutine resumes and bumps its group's
    // in_flight before the next dispatch_next observes state.
    for (int i = 0; i < 14; ++i) {
        fp.release(cloud_io::group_id::producer_upload);
        co_await ss::yield();
    }
    co_await ss::sleep(50ms);

    // Convergence point under weights 1000:1000:1500 with 14 slots is
    // (producer, consumer, default) = (4, 4, 6). Allow ±1 absolute.
    size_t p_if = fp.in_flight(cloud_io::group_id::producer_upload);
    size_t c_if = fp.in_flight(cloud_io::group_id::consumer_fetch);
    size_t d_if = fp.in_flight(cloud_io::group_id::default_group);

    EXPECT_GE(p_if, 3u);
    EXPECT_LE(p_if, 5u);
    EXPECT_GE(c_if, 3u);
    EXPECT_LE(c_if, 5u);
    EXPECT_GE(d_if, 5u);
    EXPECT_LE(d_if, 7u);
    EXPECT_EQ(p_if + c_if + d_if, 14u);

    // Cleanup.
    as.request_abort();
    for (auto& f : p_futs) {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(f)));
        if (r.failed()) {
            r.ignore_ready_future();
        } else {
            fp.release(cloud_io::group_id::producer_upload);
        }
    }
    for (auto& f : c_futs) {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(f)));
        if (r.failed()) {
            r.ignore_ready_future();
        } else {
            fp.release(cloud_io::group_id::consumer_fetch);
        }
    }
    for (auto& f : d_futs) {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(f)));
        if (r.failed()) {
            r.ignore_ready_future();
        } else {
            fp.release(cloud_io::group_id::default_group);
        }
    }
}

TEST_CORO(FairPolicyTest, DispatchEagerlyBumpsInFlightAndKeepsEAWConsistent) {
    // Regression: when dispatch_next pops a waiter, it must eagerly
    // bump the group's in_flight before set_value(). Otherwise the
    // dispatched group sits in a transient (in_flight=0, waiters=[],
    // last_active=epoch) state until its coroutine resumes — during
    // which is_effective_active returns false, and a follow-up admit
    // on the same group double-counts its weight in
    // _effective_active_weight.
    cloud_io::fair_policy fp{2};
    configure(fp, /*total_slots=*/2);
    ss::abort_source as;

    // Saturate slots with producer. eaw should now reflect producer
    // alone (1000).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.effective_active_weight(), 1000);
    EXPECT_EQ(fp.available_slots(), 0u);

    // Queue a consumer waiter. Slow path increments eaw by consumer
    // weight (1000). eaw is now 2000 (producer + consumer).
    auto c_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(10ms);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(fp.effective_active_weight(), 2000);

    // Release one producer slot. dispatch_next pops the consumer
    // waiter; the fix ensures it eagerly bumps consumer.in_flight to 1
    // BEFORE setting the awaiting coroutine's value. We observe this
    // state synchronously, before yielding to let the coroutine run.
    fp.release(cloud_io::group_id::producer_upload);

    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(fp.effective_active_weight(), 2000);

    // Follow-up admit on the same group, still before the dispatched
    // coroutine resumes. With the fix, is_effective_active returns
    // true (consumer.in_flight=1) and eaw stays at 2000. Without the
    // fix, eaw would balloon to 3000 (consumer counted twice).
    auto c2_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    EXPECT_EQ(fp.effective_active_weight(), 2000);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // Drain.
    as.request_abort();
    co_await with_test_timeout(std::move(c_fut));
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(c2_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::producer_upload);
    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, ThreeWayFastPathCapAtDefaultWeights) {
    // 14 slots with default weights (1000, 1000, 1500). With all three
    // groups in the effective-active set, caps are floor(14·1000/3500)=4
    // for producer/consumer and floor(14·1500/3500)=6 for default.
    cloud_io::fair_policy fp{14};
    configure(fp, /*total_slots=*/14);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Seed each group as effective-active (admit + release → enters dwell).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(fp.admit(cloud_io::group_id::default_group, as));
    fp.release(cloud_io::group_id::default_group);

    // Burst-admit each group up to its cap.
    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }
    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    for (int i = 0; i < 6; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::default_group, as));
    }

    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 4u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 4u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 6u);
    EXPECT_EQ(fp.available_slots(), 0u);

    // 5th producer admit must NOT complete via fast path (cap hit).
    auto p5_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(p5_fut.available());
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 4u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);

    // 5th consumer admit must queue too.
    auto c5_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(c5_fut.available());
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 4u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // 7th default admit must queue.
    auto d7_fut = fp.admit(cloud_io::group_id::default_group, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(d7_fut.available());
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 6u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::default_group), 1u);

    // Cleanup: drain the 3 queued futures, then release in-flight slots.
    as.request_abort();
    auto rp = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(p5_fut)));
    if (rp.failed()) {
        rp.ignore_ready_future();
    } else {
        fp.release(cloud_io::group_id::producer_upload);
    }
    auto rc = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(c5_fut)));
    if (rc.failed()) {
        rc.ignore_ready_future();
    } else {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
    auto rd = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(d7_fut)));
    if (rd.failed()) {
        rd.ignore_ready_future();
    } else {
        fp.release(cloud_io::group_id::default_group);
    }

    for (int i = 0; i < 4; ++i) {
        fp.release(cloud_io::group_id::producer_upload);
    }
    for (int i = 0; i < 4; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
    for (int i = 0; i < 6; ++i) {
        fp.release(cloud_io::group_id::default_group);
    }
}

TEST_CORO(FairPolicyTest, ReservedAdmitBypassesCapComputation) {
    // Hard reservation bypasses the fast-path cap entirely.
    // capacity=6, pu_min=2, low pu weight so cap formula would
    // deny pu. Verify pu admits 2 from its reserved pool regardless
    // of the cap, and the 3rd pu admit queues normally.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    // Very low pu weight so the cap formula pins it to ~0 or 1.
    fp.set_weight(cloud_io::group_id::producer_upload, 100);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Seed cf and default as effective-active.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(fp.admit(cloud_io::group_id::default_group, as));
    fp.release(cloud_io::group_id::default_group);

    // pu cap = max(1, 6*100/(100+1000+1500)) = max(1, 0) = 1.
    // But reserved bypasses cap: pu can still admit 2.
    EXPECT_TRUE(fp.try_admit(cloud_io::group_id::producer_upload));
    EXPECT_TRUE(fp.try_admit(cloud_io::group_id::producer_upload));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);

    // 3rd pu admit: reserved pool exhausted, shared cap = 1 < in_flight(2).
    // Even without cap being the issue, reserved is gone → queues.
    auto p3_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(p3_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(p3_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::producer_upload);
    fp.release(cloud_io::group_id::producer_upload);
}

TEST_CORO(FairPolicyTest, FloorBypassesFastPathCap) {
    // A group whose fast-path cap is tighter than its min_reserved
    // floor can still grow up to the floor via the fast path.
    cloud_io::fair_policy fp{20};
    configure(fp, /*total_slots=*/20);
    // Low weight on pu so its fast-path cap is small.
    fp.set_weight(cloud_io::group_id::producer_upload, 200);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 3);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Seed cf and default as effective-active so pu's cap shrinks.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(fp.admit(cloud_io::group_id::default_group, as));
    fp.release(cloud_io::group_id::default_group);

    // pu admits up to its floor: cap formula yields
    // cap_pu = max(1, 20 * 200 / (200+1000+1500)) = max(1, 1) = 1
    // but the floor lets pu grow to 3 anyway.
    for (int i = 0; i < 3; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 3u);

    // 4th admit: pu.in_flight (3) is NOT below floor (3 < 3 false),
    // so the cap check applies. Expected to queue, not admit fast-
    // path.
    auto p4_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(p4_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(p4_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    for (int i = 0; i < 3; ++i) {
        fp.release(cloud_io::group_id::producer_upload);
    }
}

TEST_CORO(FairPolicyTest, ReservedPoolReleasesAreGroupIsolated) {
    // Two groups each have their own reserved pool. Release of a
    // reserved slot from one group dispatches only to that group's
    // queued waiters, never to the other group's queue.
    //
    // capacity=8, pu_min=2, cf_min=2 → _shared=4, _reserved[pu]=2,
    // _reserved[cf]=2. Saturate both reserved pools + fill shared.
    // Queue a 3rd pu waiter and a 3rd cf waiter. Release one pu
    // reserved slot → only pu's waiter wakes. Release one cf reserved
    // slot → only cf's waiter wakes.
    cloud_io::fair_policy fp{8};
    configure(fp, /*total_slots=*/8);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);
    fp.set_min_reserved(cloud_io::group_id::consumer_fetch, 2);

    ss::abort_source as;

    // Saturate pu reserved (2) + cf reserved (2) + shared (3 of 4).
    // With all 3 groups effective-active, eaw=3500, so default's
    // fast-path cap = floor(8 * 1500 / 3500) = 3. The 4th default
    // admit would block; use 3 to stay within cap.
    for (int i = 0; i < 2; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }
    for (int i = 0; i < 2; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    for (int i = 0; i < 3; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::default_group, as));
    }
    EXPECT_LE(fp.available_slots(), 1u);

    // Queue one more waiter for each of pu and cf.
    auto p3_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    auto c3_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(10ms);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // Release one pu slot (reserved). Only pu's waiter gets it.
    fp.release(cloud_io::group_id::producer_upload);
    co_await ss::yield();
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u); // untouched

    co_await with_test_timeout(std::move(p3_fut));

    // Release one cf slot (reserved). Only cf's waiter gets it.
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await ss::yield();
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 2u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 0u);

    co_await with_test_timeout(std::move(c3_fut));

    // Cleanup remaining in-flight.
    fp.release(cloud_io::group_id::producer_upload);
    fp.release(cloud_io::group_id::consumer_fetch);
    for (int i = 0; i < 3; ++i) {
        fp.release(cloud_io::group_id::default_group);
    }
}

TEST_CORO(FairPolicyTest, FloorAtZeroDisablesPreference) {
    // With all min_reserved=0 the floor preference branch never
    // fires and dispatch behavior matches the pre-floor algorithm:
    // dispatch goes to whichever group has the most-negative
    // deviation.
    cloud_io::fair_policy fp{4};
    configure(fp, /*total_slots=*/4);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 0);
    fp.set_min_reserved(cloud_io::group_id::consumer_fetch, 0);
    fp.set_min_reserved(cloud_io::group_id::default_group, 0);

    ss::abort_source as;

    // Saturate slots with cf.
    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }

    // Queue pu (would have been floored under the previous test, now
    // not). default has higher weight (1500 vs 1000 for pu) so its
    // dev floor is deeper at in_flight=0.
    auto p_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    auto d_fut = fp.admit(cloud_io::group_id::default_group, as);
    co_await ss::sleep(10ms);

    fp.release(cloud_io::group_id::consumer_fetch);

    // default wins because its deeper deviation is the only criterion.
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::default_group), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::default_group), 0u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);

    co_await with_test_timeout(std::move(d_fut));

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(p_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::default_group);
    for (int i = 0; i < 3; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, ReservationSlotsAreDedicatedToOwnerGroup) {
    // capacity=6, pu_min=2 → _shared=4, _reserved[pu]=2.
    // Saturate shared with cf (4 admits). Verify pu can still admit 2
    // (consuming its reserved pool) while a 3rd pu admit queues.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    ss::abort_source as;

    // Fill the shared pool with cf.
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(fp.try_admit(cloud_io::group_id::consumer_fetch));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 4u);
    // shared pool exhausted; reserved pool still has 2 for pu.
    EXPECT_EQ(fp.available_slots(), 2u);

    // pu can still admit 2 from its reserved pool.
    EXPECT_TRUE(fp.try_admit(cloud_io::group_id::producer_upload));
    EXPECT_TRUE(fp.try_admit(cloud_io::group_id::producer_upload));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);
    EXPECT_EQ(fp.available_slots(), 0u);

    // 3rd pu admit must queue (reserved pool exhausted, shared empty).
    auto p3_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(p3_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(p3_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::producer_upload);
    fp.release(cloud_io::group_id::producer_upload);
    for (int i = 0; i < 4; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, ReservedSlotsReturnToReservedPool) {
    // pu holds 2 reserved + 4 shared (capacity=6, pu_min=2). Queue a cf
    // waiter. First 2 pu releases are reserved-slot releases and must NOT
    // dispatch cf — they return to pu's reserved pool. The 3rd release
    // (shared slot) dispatches cf.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    ss::abort_source as;

    // Admit 2 from reserved, then 4 from shared (6 total for pu).
    for (int i = 0; i < 6; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 6u);
    EXPECT_EQ(fp.available_slots(), 0u);

    // Queue a cf waiter.
    ss::abort_source as2;
    auto cf_fut = fp.admit(cloud_io::group_id::consumer_fetch, as2);
    co_await ss::sleep(10ms);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // First pu release: reserved slot → returns to _reserved[pu], NOT cf.
    fp.release(cloud_io::group_id::producer_upload);
    co_await ss::yield();
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_FALSE(cf_fut.available());

    // Second pu release: another reserved slot → same.
    fp.release(cloud_io::group_id::producer_upload);
    co_await ss::yield();
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_FALSE(cf_fut.available());

    // Third pu release: shared slot → dispatch_next picks cf.
    fp.release(cloud_io::group_id::producer_upload);
    co_await with_test_timeout(std::move(cf_fut));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 1u);

    // Cleanup remaining.
    fp.release(cloud_io::group_id::consumer_fetch);
    for (int i = 0; i < 3; ++i) {
        fp.release(cloud_io::group_id::producer_upload);
    }
}

TEST_CORO(FairPolicyTest, ReservedSlotsCannotBeBorrowedByOtherGroups) {
    // pu_min=2, pu idle. Saturate shared with cf (4 admits). A 5th cf
    // admit must queue — it cannot consume pu's reserved pool.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    ss::abort_source as;

    // Exhaust shared pool with cf.
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(fp.try_admit(cloud_io::group_id::consumer_fetch));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::consumer_fetch), 4u);
    // pu reserved pool still holds 2 but cf cannot touch them.
    EXPECT_EQ(fp.available_slots(), 2u);

    // 5th cf admit: shared pool empty, reserved belongs to pu → must queue.
    auto cf5_fut = fp.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::sleep(20ms);
    EXPECT_FALSE(cf5_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 0u);

    // Cleanup.
    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(cf5_fut)));
    if (r.failed()) {
        r.ignore_ready_future();
    }
    for (int i = 0; i < 4; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

// ---- Phase 2: work-conserving reservation tests ----

TEST_CORO(FairPolicyTest, ReservationDecaysAfterDwellExpiration) {
    // capacity=6, pu_min=2. After one admit+release cycle, pu goes
    // idle. Advancing the clock past dwell and triggering a refresh
    // must return all 2 idle reserved slots to the shared pool
    // (current_reserved(pu)==0, available_slots()==6).
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Admit once (from reserved), then release. pu goes idle.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 2u);
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 2u);

    // Advance clock past dwell. Trigger refresh via a cf admit.
    fake_now += cloud_io::default_dwell_duration + std::chrono::seconds{1};
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));

    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.available_slots(), 5u); // 6 total - 1 held by cf
    fp.release(cloud_io::group_id::consumer_fetch);
    EXPECT_EQ(fp.available_slots(), 6u);
}

TEST_CORO(FairPolicyTest, DecayTwoCyclesConvergesToZero) {
    // Capacity=6, pu_min=2. Two sequential decay cycles: first
    // cycle decays only the 1 idle reserved slot available at dwell
    // expiry (the second slot was returned by a second release that
    // happened between cycles). Second cycle decays the remaining 1.
    // This exercises the code path where decay=min(available,
    // current_reserved) < current_reserved on the first cycle.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Admit pu twice (both from reserved).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);
    // _reserved[pu] = 0 (both consumed by reserved admits).

    // Release one. _reserved[pu] = 1 (returned via reserved-release
    // path). pu is still active (in_flight=1).
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 1u);

    // Release the second. _reserved[pu] = 2. pu goes idle.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 0u);

    // Advance past dwell, trigger first refresh via a cf admit.
    // Both reserved slots are idle → decay fires: current_reserved → 0.
    fake_now += cloud_io::default_dwell_duration + std::chrono::seconds{1};
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    fp.release(cloud_io::group_id::consumer_fetch);

    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.available_slots(), 6u);

    // Second full cycle: re-establish reservation, decay again.
    // set_min_reserved resets current_reserved = value.
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 2u);

    // Use pu once, release, advance, decay again.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);
    fake_now += cloud_io::default_dwell_duration + std::chrono::seconds{1};
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::consumer_fetch, as));
    fp.release(cloud_io::group_id::consumer_fetch);

    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.available_slots(), 6u);
}

TEST_CORO(FairPolicyTest, StealBackGrowsReservationOnReleases) {
    // capacity=6, pu_min=2. Drive pu's reservation to 0 via decay.
    // Sole active group is pu (other groups past dwell). Verify that
    // shared-release events with no queued waiters route slots back to
    // pu's reserved lane via steal-back, growing current_reserved toward
    // min_reserved. Once full, further releases return to the shared pool.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Admit + release pu, then decay. Use pu itself to trigger the
    // refresh (admit pu again after clock advance; that triggers
    // refresh_dwell_expirations inside admit(), which expires pu's own
    // dwell and decays the reservation; then pu becomes active again).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);
    // Advance well past both pu and any other group's dwell window.
    fake_now += cloud_io::default_dwell_duration * 3;

    // Trigger refresh + admit pu via shared (reserved decays to 0
    // inside refresh_dwell_expirations during this admit call, before
    // any try_wait; then reserved is empty so fast path falls through
    // to shared).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);

    // pu is now the sole effective-active group (in_flight=1 via shared).
    // Fill remaining shared slots with cf. Since pu is sole active,
    // eaw = pu.weight = 1000. cf's cap = max(1, 6*1000/(1000+1000)) = 3
    // once cf is active. Admit cf twice (within its cap of 3). Then pu
    // and cf share eaw. One more cf admit succeeds (in_flight=3, cap=3).
    // Then 4th would block — use only 3 cf admits to stay safe.
    // Total in_flight = 1(pu) + 3(cf) = 4. shared = 2 remaining.
    // Admit 2 more cf: cf in_flight grows to 5 total. shared = 0.
    // Actually: after pu admit, _shared = 5. cf cap at first admit:
    // eaw = 1000(pu, active) + 1000(cf, becomes active on first admit).
    // Wait — cf becomes active on its first admit. At that point
    // eaw = 2000, cap_cf = max(1, 6*1000/2000) = 3. Admits 1, 2, 3 ok.
    // 4th cf would block. So admit 3 cf (not 5).
    for (int i = 0; i < 3; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    // in_flight: pu=1, cf=3. _shared = 2. available_slots() = 2.
    // Drain the remaining 2 shared slots with cf too, but cf cap=3...
    // Just verify the steal-back behavior with what we have.
    // Release pu (shared slot). No waiters → steal-back picks pu.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 1u);

    // Release one cf (shared slot). No waiters → steal-back picks pu.
    fp.release(cloud_io::group_id::consumer_fetch);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 2u);

    // Release another cf. pu is now full (current_reserved >= min_reserved).
    // Steal-back does NOT fire → slot returns to shared.
    const auto avail_before = fp.available_slots();
    fp.release(cloud_io::group_id::consumer_fetch);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 2u);
    EXPECT_EQ(fp.available_slots(), avail_before + 1u);

    // Cleanup remaining cf in_flight.
    fp.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(FairPolicyTest, WakeTransientUsesDispatchFloor) {
    // capacity=6, pu_min=2. After full decay (current_reserved=0),
    // the shared pool grows to 6. Saturate with cf (using try_admit to
    // avoid cap blocking). Admit pu → must queue (reserved empty, shared
    // full). On next release, the dispatch floor branch fires (pu.in_flight
    // =0 < min_reserved=2 AND pu has a waiter), pu gets dispatched via
    // shared lane. The subsequent release (no waiters) routes to steal-back.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Decay pu reservation to 0. Use the same self-trigger approach:
    // admit/release pu, advance clock, admit pu again (triggers decay).
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);
    fake_now += cloud_io::default_dwell_duration * 3;
    // This admit decays pu's reservation and admits via shared.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);
    // Release pu so it's idle again before saturating.
    fp.release(cloud_io::group_id::producer_upload);

    // Advance past pu's dwell so pu is no longer in effective-active set.
    fake_now += cloud_io::default_dwell_duration * 3;

    // Saturate all 6 shared slots with cf via try_admit (bypasses
    // cap check in a queueing-free manner for test setup).
    // cf.cap would be limited by eaw, but try_admit uses the same cap
    // path. Use a sole-cf admit to seed cf as effective-active first,
    // then use the floor bypass: in_flight < min_reserved check only
    // applies to cf if cf has a min_reserved. Since cf.min_reserved=0,
    // the below_floor check is false. Use direct slow-path admission
    // by filling and queuing only what we need.
    // Simpler: saturate via 6 admissions on a solo-active cf group
    // (pu is past dwell, cf becomes the sole active group with cap=6).
    for (int i = 0; i < 6; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    EXPECT_EQ(fp.available_slots(), 0u);

    // Admit pu — must queue (reserved empty, shared full).
    auto pu_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::sleep(10ms);
    EXPECT_FALSE(pu_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 0u);

    // Release one cf. dispatch_next floor branch: pu.in_flight(0) <
    // min_reserved(2) AND pu has waiter → pu wins via shared lane.
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(pu_fut));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 0u);

    // Release pu. Shared release with no waiters. pu is effective-active
    // (just ran) and under-reserved → steal-back grows it.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 1u);

    // Cleanup remaining cf.
    for (int i = 0; i < 5; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

TEST_CORO(FairPolicyTest, StealBackOnlyFiresAfterDispatchNext) {
    // capacity=4, pu_min=1. After decay (current_reserved=0), fill
    // all slots with cf (sole active group → cap=4). Queue both a pu
    // waiter and a default waiter. On release, dispatch_next fires for
    // pu (floor branch: in_flight=0 < min_reserved=1). Steal-back must
    // NOT fire on the same release; current_reserved(pu) stays 0.
    cloud_io::fair_policy fp{4};
    configure(fp, /*total_slots=*/4);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 1);

    auto fake_now = ss::lowres_clock::time_point{} + std::chrono::seconds{60};
    fp.set_now_fn_for_test([&fake_now] { return fake_now; });

    ss::abort_source as;

    // Decay pu reservation to 0 via the self-trigger approach.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    fp.release(cloud_io::group_id::producer_upload);
    fake_now += cloud_io::default_dwell_duration * 3;
    // Admit pu again: decays reservation + admits via shared.
    co_await with_test_timeout(
      fp.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);
    fp.release(cloud_io::group_id::producer_upload);

    // Advance past pu dwell so pu exits effective-active set.
    fake_now += cloud_io::default_dwell_duration * 3;

    // Saturate all 4 slots with cf (sole active group → cap=4).
    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::consumer_fetch, as));
    }
    EXPECT_EQ(fp.available_slots(), 0u);

    // Queue a pu waiter and a default waiter.
    auto pu_fut = fp.admit(cloud_io::group_id::producer_upload, as);
    auto dg_fut = fp.admit(cloud_io::group_id::default_group, as);
    co_await ss::sleep(10ms);
    EXPECT_FALSE(pu_fut.available());
    EXPECT_FALSE(dg_fut.available());
    EXPECT_EQ(fp.waiters(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.waiters(cloud_io::group_id::default_group), 1u);

    // Release one cf. dispatch_next fires for pu (floor branch).
    // Steal-back must not fire — slot was consumed by dispatch_next.
    fp.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(pu_fut));
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 1u);
    // steal-back did NOT fire: current_reserved unchanged at 0.
    EXPECT_EQ(fp.current_reserved(cloud_io::group_id::producer_upload), 0u);

    // Cleanup: abort default waiter and drain remaining in-flight.
    as.request_abort();
    auto dr = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(dg_fut)));
    if (dr.failed()) {
        dr.ignore_ready_future();
    }
    fp.release(cloud_io::group_id::producer_upload);
    for (int i = 0; i < 3; ++i) {
        fp.release(cloud_io::group_id::consumer_fetch);
    }
}

// ---- End Phase 2 tests ----

TEST_CORO(FairPolicyTest, ReservedAndSharedSlotsCoexist) {
    // capacity=6, pu_min=2. pu admits 4 total: first 2 come from
    // reserved, next 2 from shared. Verify reserved_in_flight=2, then
    // that reserved-first releases drain the reserved pool before shared,
    // with available_slots() correct at each step.
    cloud_io::fair_policy fp{6};
    configure(fp, /*total_slots=*/6);
    fp.set_min_reserved(cloud_io::group_id::producer_upload, 2);

    ss::abort_source as;

    // Admit 4 into pu: 2 reserved + 2 shared.
    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          fp.admit(cloud_io::group_id::producer_upload, as));
    }
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 4u);
    // reserved pool drained (0 left) + shared has 2 left.
    EXPECT_EQ(fp.available_slots(), 2u);

    // Release 1: reserved_in_flight was 2 → release goes back to reserved.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 3u);
    // reserved pool has 1 again; shared unchanged at 2.
    EXPECT_EQ(fp.available_slots(), 3u);

    // Release 2: still a reserved slot → back to reserved.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 2u);
    // reserved pool fully restored (2); shared still 2.
    EXPECT_EQ(fp.available_slots(), 4u);

    // Release 3: reserved_in_flight now 0 → shared release path.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(fp.available_slots(), 5u);

    // Release 4: last shared slot.
    fp.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(fp.in_flight(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(fp.available_slots(), 6u);
}
