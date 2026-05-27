/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/reservation_policy.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_io/logger.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/metrics.hh>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>
#include <vector>

namespace cloud_io {

namespace {

constexpr std::array<group_id, num_group_ids> all_groups{
  group_id::producer_upload,
  group_id::consumer_fetch,
  group_id::default_group,
};

/// Build the per-group state. Each group's reservation lane gets a
/// container named after its group_id.
template<typename Traits, size_t... Is>
per_group<reservation_group_state<Traits>>
make_group_states(std::index_sequence<Is...>) {
    return {{reservation_group_state<Traits>{
      static_cast<group_id>(Is),
      fmt::format(
        "cloud_io/reservation_policy/reserved/{}",
        to_string_view(static_cast<group_id>(Is)))}...}};
}

} // namespace

template<typename Traits>
void reservation_policy<Traits>::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const auto group_name = prometheus_sanitize::metrics_name(
      "cloud_io_scheduler");

    _metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [this] { return available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved).")),
        sm::make_gauge(
          "total_capacity",
          [this] { return total_capacity(); },
          sm::description("Configured total slot capacity.")),
        sm::make_gauge(
          "total_waiters",
          [this] { return total_waiters(); },
          sm::description("Total fibers queued across all groups.")),
      });

    constexpr auto group_label_key = "group_id";

    for (auto g : all_groups) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, g] { return in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels),
            sm::make_gauge(
              "waiters",
              [this, g] { return waiters(g); },
              sm::description("Fibers queued on this group."),
              labels),
            sm::make_counter(
              "admit_total",
              [this, g] { return admit_total(g); },
              sm::description("Total admit() calls completed for this group."),
              labels),
            sm::make_counter(
              "admit_immediate_total",
              [this, g] { return admit_immediate_total(g); },
              sm::description(
                "admit() calls that took the fast path (no queue)."),
              labels),
            sm::make_gauge(
              "current_reserved",
              [this, g] { return current_reserved(g); },
              sm::description(
                "Runtime reservation size. Starts at target_reserved; "
                "reclaimed by the policy when idle past dwell; rebuilt "
                "via refill."),
              labels),
          });
    }

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    const auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _public_metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [this] { return available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved)."))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "total_capacity",
          [this] { return total_capacity(); },
          sm::description("Configured total slot capacity."))
          .aggregate(aggregate_labels),
      });

    for (auto g : all_groups) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _public_metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, g] { return in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels)
              .aggregate(aggregate_labels),
            sm::make_gauge(
              "waiters",
              [this, g] { return waiters(g); },
              sm::description("Fibers queued on this group."),
              labels)
              .aggregate(aggregate_labels),
          });
    }
}

template<typename Traits>
reservation_policy<Traits>::reservation_policy(
  amount_t capacity, reservation_policy_config cfg)
  : scheduler_policy<Traits>(capacity)
  , _current_total_capacity(capacity)
  , _shared(Traits::make_container(0, "cloud_io/reservation_policy/shared"))
  , _groups(make_group_states<Traits>(std::make_index_sequence<num_group_ids>{}))
  , _now_fn([] { return ss::lowres_clock::now(); }) {
    const size_t target_sum = std::ranges::fold_left(
      cfg.target_reserved, size_t{0}, std::plus{});
    vassert(
      target_sum <= capacity,
      "reservation_policy: target_reserved sum ({}) exceeds capacity ({})",
      target_sum,
      capacity);

    Traits::grant(_shared, capacity);
    for (const auto g : all_group_ids) {
        set_target_reserved(g, cfg.target_reserved[g]);
    }

    setup_metrics();

    vlog(
      log.info,
      "reservation_policy initialized: capacity={} dwell={}s "
      "target_reserved={}",
      _current_total_capacity,
      default_dwell_duration.count(),
      cfg.target_reserved.data);
}

template<typename Traits>
reservation_policy<Traits>::~reservation_policy() noexcept {
    for (const auto& gs : _groups) {
        vassert(
          gs.waiters.empty(),
          "cloud_io::reservation_policy destroyed with active waiters");
    }
}

template<typename Traits>
ss::future<> reservation_policy<Traits>::stop() {
    _metrics.clear();
    _public_metrics.clear();
    // Synchronous: abort all queued waiters.
    for (auto& gs : _groups) {
        while (!gs.waiters.empty()) {
            gs.cancel_waiter(gs.waiters.front(), _now_fn());
        }
    }
    return ss::now();
}

template<typename Traits>
size_t reservation_policy<Traits>::in_flight(group_id g) const noexcept {
    return _groups[g].in_flight;
}

template<typename Traits>
size_t reservation_policy<Traits>::waiters(group_id g) const noexcept {
    return _groups[g].waiter_count();
}

template<typename Traits>
auto reservation_policy<Traits>::available_slots() const noexcept -> amount_t {
    amount_t total = Traits::available(_shared);
    for (const auto& gs : _groups) {
        total += Traits::available(gs.reserved_container);
    }
    return total;
}

template<typename Traits>
auto reservation_policy<Traits>::total_capacity() const noexcept -> amount_t {
    return _current_total_capacity;
}

template<typename Traits>
ss::future<>
reservation_policy<Traits>::admit(group_id g, ss::abort_source& as) {
    // Fast path.
    if (try_admit(g)) {
        co_return;
    }

    auto& gs = _groups[g];

    // Slow path: queue this caller on the group's waiter list. The
    // caller must keep the waiter node alive until the future
    // resolves (success or abort).
    reservation_waiter w;
    gs.enqueue_waiter(w, _waiter_seq_counter++);
    auto fut = w.p.get_future();

    auto sub = as.subscribe(
      [&w, &gs, this](const std::optional<std::exception_ptr>& ex) noexcept {
          gs.cancel_waiter(w, _now_fn(), ex.value_or(nullptr));
      });

    if (!sub) {
        // abort_source was already aborted, so just cancel the waiter.
        // cancel operation will fail its promise with abort_requested
        gs.cancel_waiter(w, _now_fn());
    }

    co_await std::move(fut);
    gs.on_dispatched_admit();
    co_return;
}

template<typename Traits>
bool reservation_policy<Traits>::try_admit(group_id g) noexcept {
    auto& gs = _groups[g];

    // Reclaim idle reservations first. Otherwise a waiting group could
    // queue behind capacity stranded on an inactive group.
    reclaim_idle_reservations(_now_fn());

    if (gs.try_take_reserved_slot()) {
        gs.on_immediate_admit(/*from_reserved=*/true);
        return true;
    }

    if (Traits::try_acquire(_shared, Traits::unit)) {
        gs.on_immediate_admit(/*from_reserved=*/false);
        return true;
    }

    return false;
}

template<typename Traits>
void reservation_policy<Traits>::release(group_id g) noexcept {
    auto& gs = _groups[g];
    gs.on_release(_now_fn());

    // Try to return a slot directly to this group, either to another waiter or
    // to the reservation lane.
    if (gs.maybe_return_reserved_slot()) {
        return;
    }

    // Common-pool release: try to dispatch a queued waiter, then
    // refill a reservation lane, then fall back to the common pool.
    if (dispatch_next()) {
        return;
    }
    if (const auto target = pick_refill_candidate(); target.has_value()) {
        _groups[*target].grant_reserved_slot();
    } else {
        Traits::grant(_shared, Traits::unit);
    }
}

template<typename Traits>
bool reservation_policy<Traits>::dispatch_next() noexcept {
    // One pass picks two candidates: the oldest seq among under-target
    // groups (preferred), and the oldest seq globally (fallback).
    std::optional<group_id> under_target_pick;
    std::optional<group_id> any_pick;
    uint64_t under_target_oldest = std::numeric_limits<uint64_t>::max();
    uint64_t any_oldest = std::numeric_limits<uint64_t>::max();

    for (const auto& gs : _groups) {
        if (gs.waiters.empty()) {
            continue;
        }
        const uint64_t front_seq = gs.waiters.front().seq;
        if (front_seq < any_oldest) {
            any_oldest = front_seq;
            any_pick = gs.id;
        }
        if (gs.has_reservation_headroom() && front_seq < under_target_oldest) {
            under_target_oldest = front_seq;
            under_target_pick = gs.id;
        }
    }

    const auto pick = under_target_pick.has_value() ? under_target_pick
                                                    : any_pick;
    if (!pick.has_value()) {
        return false;
    }

    auto& gs = _groups[*pick];
    gs.release_front_waiter();

    if (_dispatch_counter++ % 1000 == 0) {
        vlog(
          log.debug,
          "reservation_policy: dispatch #{} picked={} | {}",
          _dispatch_counter,
          to_string_view(gs.id),
          fmt::join(_groups, " "));
    }

    return true;
}

template<typename Traits>
void reservation_policy<Traits>::set_total_slots(amount_t desired) {
    if (desired == _current_total_capacity) {
        return;
    }
    if (desired > _current_total_capacity) {
        Traits::grant(_shared, desired - _current_total_capacity);
    } else {
        Traits::take(_shared, _current_total_capacity - desired);
    }
    vlog(
      log.info,
      "cloud_io reservation_policy total slots: {} -> {}",
      _current_total_capacity,
      desired);
    _current_total_capacity = desired;
}

template<typename Traits>
void reservation_policy<Traits>::set_target_reserved(group_id g, amount_t value) {
    auto& gs = _groups[g];
    // Reconcile the reservation lane to reflect the new target. Compute
    // the delta against current_reserved() (the derived current size) so
    // that any reclamation or refill since the last call are accounted
    // for; the lane may have ebbed and flowed between calls.
    const auto cur = gs.current_reserved();
    if (value > cur) {
        const auto delta = value - cur;
        vassert(
          Traits::available(_shared) >= delta,
          "set_target_reserved({}, {}): would underflow _shared "
          "(current={}, delta={})",
          to_string_view(g),
          value,
          Traits::available(_shared),
          delta);
        Traits::take(_shared, delta);
        Traits::grant(gs.reserved_container, delta);
    } else if (value < cur) {
        const auto delta = cur - value;
        vassert(
          Traits::available(gs.reserved_container) >= delta,
          "set_target_reserved({}, {}): would underflow reserved_container "
          "(current={}, in_flight={}, delta={})",
          to_string_view(g),
          value,
          Traits::available(gs.reserved_container),
          gs.reserved_in_flight,
          delta);
        Traits::take(gs.reserved_container, delta);
        Traits::grant(_shared, delta);
    }
    gs.target_reserved = value;
}

template<typename Traits>
auto reservation_policy<Traits>::target_reserved(group_id g) const noexcept
  -> amount_t {
    return _groups[g].target_reserved;
}

template<typename Traits>
auto reservation_policy<Traits>::current_reserved(group_id g) const noexcept
  -> amount_t {
    return _groups[g].current_reserved();
}

template<typename Traits>
uint64_t
reservation_policy<Traits>::admit_total(group_id g) const noexcept {
    return _groups[g].admit_total;
}

template<typename Traits>
uint64_t reservation_policy<Traits>::admit_immediate_total(
  group_id g) const noexcept {
    return _groups[g].admit_immediate_total;
}

template<typename Traits>
size_t reservation_policy<Traits>::total_waiters() const noexcept {
    return std::ranges::fold_left(
      _groups, size_t{0}, [](size_t acc, const auto& gs) {
          return acc + gs.waiter_count();
      });
}

template<typename Traits>
void reservation_policy<Traits>::set_now_fn_for_test(now_fn_t fn) {
    _now_fn = std::move(fn);
}

template<typename Traits>
void reservation_policy<Traits>::reclaim_idle_reservations(
  ss::lowres_clock::time_point now) {
    for (auto& gs : _groups) {
        if (gs.is_dwell_expired(now)) {
            Traits::grant(_shared, gs.drain_idle_reserved());
        }
    }
}

template<typename Traits>
std::optional<group_id>
reservation_policy<Traits>::pick_refill_candidate() noexcept {
    const auto now = _now_fn();
    std::optional<group_id> winner;
    // Smaller ratio = more under-target.
    size_t lowest_ratio = std::numeric_limits<size_t>::max();
    for (const auto& gs : _groups) {
        if (!gs.is_refill_eligible(now)) {
            continue;
        }
        if (
          const auto ratio = gs.refill_priority_ratio(); ratio < lowest_ratio) {
            lowest_ratio = ratio;
            winner = gs.id;
        }
    }
    return winner;
}

template class reservation_policy<slot_resource_traits>;
template class reservation_policy<bytes_resource_traits>;

} // namespace cloud_io
