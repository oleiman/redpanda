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

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_io/logger.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>

#include <algorithm>
#include <limits>
#include <utility>

namespace cloud_io {

namespace {

constexpr std::array<group_id, num_group_ids> all_groups{
  group_id::producer_upload,
  group_id::consumer_fetch,
  group_id::default_group,
};

// Group is currently active (has demand or in-flight ops).
bool is_active(const fair_group_state& gs) {
    return gs.in_flight > 0 || !gs.waiters.empty();
}

// Group is in the effective active set: either currently active,
// or recently active and still within the dwell window.
bool is_effective_active(
  const fair_group_state& gs, ss::lowres_clock::time_point now) {
    if (is_active(gs)) {
        return true;
    }
    if (gs.last_active == ss::lowres_clock::time_point{}) {
        return false;
    }
    return now - gs.last_active < default_dwell_duration;
}

} // namespace

fair_policy::fair_policy(size_t capacity)
  : scheduler_policy(capacity)
  , _current_total_capacity(capacity)
  , _shared(0, "cloud_io/fair_policy/shared")
  , _reserved{
      ssx::semaphore(0, "cloud_io/fair_policy/reserved/producer_upload"),
      ssx::semaphore(0, "cloud_io/fair_policy/reserved/consumer_fetch"),
      ssx::semaphore(0, "cloud_io/fair_policy/reserved/default_group"),
    }
  , _now_fn([] { return ss::lowres_clock::now(); }) {
    const auto& cfg = config::shard_local_cfg();
    _groups[static_cast<size_t>(group_id::producer_upload)].weight
      = cfg.cloud_io_scheduler_fair_producer_upload_weight();
    _groups[static_cast<size_t>(group_id::consumer_fetch)].weight
      = cfg.cloud_io_scheduler_fair_consumer_fetch_weight();
    _groups[static_cast<size_t>(group_id::default_group)].weight
      = cfg.cloud_io_scheduler_fair_default_group_weight();

    // min_reserved: start at 0 (all shared). Production callers invoke
    // set_min_reserved() immediately after construction to establish the
    // configured hard reservations. Starting at 0 here avoids a
    // constructor-time vassert when tests construct with small capacities
    // that would otherwise be smaller than the config defaults.
    //
    // TODO(phase-2): reserved slots sit idle when their owner is dormant.
    // A follow-up will reclaim idle reserved capacity via dwell-tied
    // expiry or priority-debt borrowing so the shared pool recovers the
    // wasted concurrency budget.
    _shared.signal(capacity);

    vlog(
      log.info,
      "fair_policy initialized: capacity={} "
      "weights={{producer_upload={}, consumer_fetch={}, default_group={}}} "
      "dwell={}s",
      _current_total_capacity,
      _groups[static_cast<size_t>(group_id::producer_upload)].weight,
      _groups[static_cast<size_t>(group_id::consumer_fetch)].weight,
      _groups[static_cast<size_t>(group_id::default_group)].weight,
      default_dwell_duration.count());

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
          [this] { return _current_total_capacity; },
          sm::description("Configured total slot capacity.")),
        sm::make_gauge(
          "total_waiters",
          [this] {
              size_t sum = 0;
              for (const auto& gs : _groups) {
                  sum += std::distance(gs.waiters.begin(), gs.waiters.end());
              }
              return sum;
          },
          sm::description("Total fibers queued across all groups.")),
        sm::make_gauge(
          "effective_active_weight",
          [this] { return _effective_active_weight; },
          sm::description(
            "Sum of weights of groups in the effective active set "
            "(currently active OR within dwell window). Cap "
            "denominator for fast-path admission.")),
      });

    constexpr auto group_label_key = "group_id";

    for (auto g : all_groups) {
        const auto idx = static_cast<size_t>(g);
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ss::sstring{to_string_view(g)})};

        _metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, idx] { return _groups[idx].in_flight; },
              sm::description("Concurrent ops currently holding a slot."),
              labels),
            sm::make_gauge(
              "waiters",
              [this, idx] {
                  return std::distance(
                    _groups[idx].waiters.begin(), _groups[idx].waiters.end());
              },
              sm::description("Fibers queued on this group."),
              labels),
            sm::make_counter(
              "admit_total",
              [this, idx] { return _groups[idx].admit_total; },
              sm::description("Total admit() calls completed for this group."),
              labels),
            sm::make_counter(
              "admit_immediate_total",
              [this, idx] { return _groups[idx].admit_immediate_total; },
              sm::description("admit() calls that did not queue."),
              labels),
            sm::make_gauge(
              "seconds_since_active",
              [this, idx] {
                  const auto& gs = _groups[idx];
                  if (gs.last_active == ss::lowres_clock::time_point{}) {
                      return int64_t{0};
                  }
                  if (is_active(gs)) {
                      return int64_t{0};
                  }
                  const auto delta = _now_fn() - gs.last_active;
                  const auto secs
                    = std::chrono::duration_cast<std::chrono::seconds>(delta)
                        .count();
                  const int64_t cap_secs = 4 * default_dwell_duration.count();
                  return secs > cap_secs ? cap_secs : int64_t(secs);
              },
              sm::description(
                "Seconds since this group last transitioned to "
                "inactive. 0 if currently active or never active. "
                "Capped at 4 × dwell to bound metric range."),
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
          [this] { return _current_total_capacity; },
          sm::description("Configured total slot capacity."))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "effective_active_weight",
          [this] { return _effective_active_weight; },
          sm::description(
            "Sum of weights of groups in the effective active set."))
          .aggregate(aggregate_labels),
      });

    for (auto g : all_groups) {
        const auto idx = static_cast<size_t>(g);
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ss::sstring{to_string_view(g)})};

        _public_metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, idx] { return _groups[idx].in_flight; },
              sm::description("Concurrent ops currently holding a slot."),
              labels)
              .aggregate(aggregate_labels),
            sm::make_gauge(
              "waiters",
              [this, idx] {
                  return std::distance(
                    _groups[idx].waiters.begin(), _groups[idx].waiters.end());
              },
              sm::description("Fibers queued on this group."),
              labels)
              .aggregate(aggregate_labels),
          });
    }
}

fair_policy::~fair_policy() noexcept {
    for (const auto& gs : _groups) {
        vassert(
          gs.waiters.empty(),
          "cloud_io::fair_policy destroyed with active waiters");
    }
}

ss::future<> fair_policy::stop() {
    for (auto& gs : _groups) {
        while (!gs.waiters.empty()) {
            auto& w = gs.waiters.front();
            gs.waiters.pop_front();
            if (!is_active(gs)) {
                gs.last_active = _now_fn();
            }
            w.p.set_exception(
              std::make_exception_ptr(ss::abort_requested_exception{}));
        }
    }
    return ss::make_ready_future<>();
}

size_t fair_policy::in_flight(group_id g) const noexcept {
    return _groups[static_cast<size_t>(g)].in_flight;
}

size_t fair_policy::waiters(group_id g) const noexcept {
    const auto& q = _groups[static_cast<size_t>(g)].waiters;
    return std::distance(q.begin(), q.end());
}

uint32_t fair_policy::weight(group_id g) const noexcept {
    return _groups[static_cast<size_t>(g)].weight;
}

size_t fair_policy::available_slots() const noexcept {
    ssize_t total = static_cast<ssize_t>(_shared.current());
    for (const auto& s : _reserved) {
        total += static_cast<ssize_t>(s.current());
    }
    return total < 0 ? 0 : static_cast<size_t>(total);
}

int64_t fair_policy::effective_active_weight() const noexcept {
    return _effective_active_weight;
}

size_t fair_policy::total_capacity() const noexcept {
    return _current_total_capacity;
}

ss::future<> fair_policy::admit(group_id g, ss::abort_source& as) {
    const auto idx = static_cast<size_t>(g);
    auto& gs = _groups[idx];
    const auto now = _now_fn();

    // Lazily expire dwell windows; updates _effective_active_weight.
    refresh_dwell_expirations(now);

    // Compute cap denominator: effective active weight including
    // this group (if not already counted).
    const bool self_eff_active = is_effective_active(gs, now);
    const int64_t denom = self_eff_active
                            ? _effective_active_weight
                            : _effective_active_weight + int64_t(gs.weight);
    // At least 1 so low-weight groups can always make progress
    // when there are free slots (work-conserving property).
    const auto cap = denom > 0 ? std::max(
                                   size_t{1},
                                   size_t(
                                     (int64_t(_current_total_capacity)
                                      * int64_t(gs.weight))
                                     / denom))
                               : _current_total_capacity;

    // Phase 1 hard reservation: try the group's dedicated reserved
    // pool first. Reserved slots are not subject to the cap; they are
    // a guaranteed concurrency budget for this group.
    if (gs.min_reserved > 0 && _reserved[idx].try_wait(1)) {
        ++gs.in_flight;
        ++gs.reserved_in_flight;
        ++gs.admit_total;
        ++gs.admit_immediate_total;
        if (!self_eff_active) {
            _effective_active_weight += int64_t(gs.weight);
        }
        co_return;
    }

    // Shared fast path: cap check (with below-floor bypass) + shared
    // semaphore. below_floor is effectively dead with hard reservation
    // in place (a group below min_reserved has reserved slots
    // available; that branch above would have succeeded), but is kept
    // intact to ease the Phase 2 transition.
    const bool below_floor = gs.in_flight < gs.min_reserved;
    if ((below_floor || gs.in_flight < cap) && _shared.try_wait(1)) {
        ++gs.in_flight;
        ++gs.admit_total;
        ++gs.admit_immediate_total;
        if (!self_eff_active) {
            _effective_active_weight += int64_t(gs.weight);
        }
        co_return;
    }

    // Slow path: queue this caller on the group's waiter list.
    // Becoming a waiter makes the group active; track that.
    fair_waiter w;
    w.seq = _waiter_seq_counter++;
    gs.waiters.push_back(w);
    if (!self_eff_active) {
        _effective_active_weight += int64_t(gs.weight);
    }

    auto fut = w.p.get_future();

    // Wire abort: on abort, remove from queue and complete future
    // with exception. subscribe() returns an empty optional when the
    // abort source is already aborted at subscription time.
    auto sub = as.subscribe(
      [&w, &gs, this](const std::optional<std::exception_ptr>& ex) noexcept {
          if (!w.link.is_linked()) {
              return;
          } // already dispatched
          w.link.unlink();
          if (!is_active(gs)) {
              gs.last_active = _now_fn();
          }
          w.p.set_exception(ex.value_or(
            std::make_exception_ptr(ss::abort_requested_exception{})));
      });

    if (!sub) {
        // Already aborted at subscribe() time; callback was not
        // installed.
        if (w.link.is_linked()) {
            w.link.unlink();
        }
        if (!is_active(gs)) {
            gs.last_active = _now_fn();
        }
        throw ss::abort_requested_exception{};
    }

    co_await std::move(fut);

    // Dispatched. admit_total bumped here so it reflects completed
    // admissions, not attempts. in_flight was bumped eagerly in
    // dispatch_next() so the group's state is consistent between
    // dispatch and resumption (avoids a double-count of weight in
    // _effective_active_weight when a follow-up admit observes
    // is_effective_active=false during the resumption gap).
    ++gs.admit_total;
    co_return;
}

bool fair_policy::try_admit(group_id g) noexcept {
    const auto idx = static_cast<size_t>(g);
    auto& gs = _groups[idx];
    const auto now = _now_fn();

    refresh_dwell_expirations(now);

    const bool self_eff_active = is_effective_active(gs, now);
    const int64_t denom = self_eff_active
                            ? _effective_active_weight
                            : _effective_active_weight + int64_t(gs.weight);
    const auto cap = denom > 0 ? std::max(
                                   size_t{1},
                                   size_t(
                                     (int64_t(_current_total_capacity)
                                      * int64_t(gs.weight))
                                     / denom))
                               : _current_total_capacity;

    if (gs.min_reserved > 0 && _reserved[idx].try_wait(1)) {
        ++gs.in_flight;
        ++gs.reserved_in_flight;
        ++gs.admit_total;
        ++gs.admit_immediate_total;
        if (!self_eff_active) {
            _effective_active_weight += int64_t(gs.weight);
        }
        return true;
    }

    const bool below_floor_ta = gs.in_flight < gs.min_reserved;
    if ((below_floor_ta || gs.in_flight < cap) && _shared.try_wait(1)) {
        ++gs.in_flight;
        ++gs.admit_total;
        ++gs.admit_immediate_total;
        if (!self_eff_active) {
            _effective_active_weight += int64_t(gs.weight);
        }
        return true;
    }
    return false;
}

void fair_policy::release(group_id g) noexcept {
    const auto idx = static_cast<size_t>(g);
    auto& gs = _groups[idx];
    --gs.in_flight;
    if (!is_active(gs)) {
        gs.last_active = _now_fn();
    }

    if (gs.reserved_in_flight > 0) {
        // Release a reserved slot. The slot can only go back to this
        // group's reserved pool or to a same-group queued waiter; it
        // cannot cross to another group (would violate the reservation
        // invariant).
        --gs.reserved_in_flight;
        if (!gs.waiters.empty()) {
            auto& w = gs.waiters.front();
            gs.waiters.pop_front();
            ++gs.in_flight;
            ++gs.reserved_in_flight;
            w.p.set_value();
        } else {
            _reserved[idx].signal(1);
        }
        return;
    }

    // Shared release: existing cross-group dispatch_next applies.
    if (!dispatch_next()) {
        _shared.signal(1);
    }
}

bool fair_policy::dispatch_next() noexcept {
    // Phase 0: floor preference. Any group whose in_flight is below
    // its min_reserved floor AND has a queued waiter wins ahead of
    // the deviation race. Among multiple floored-and-under groups,
    // pick the one whose front waiter is oldest (FIFO across groups).
    size_t floor_idx = num_group_ids;
    uint64_t oldest_seq = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < num_group_ids; ++i) {
        const auto& gs = _groups[i];
        if (gs.in_flight < gs.min_reserved && !gs.waiters.empty()) {
            const uint64_t front_seq = gs.waiters.front().seq;
            if (front_seq < oldest_seq) {
                oldest_seq = front_seq;
                floor_idx = i;
            }
        }
    }

    // Phase 1+2: deviation race (only consulted if floor didn't pick).
    int64_t total_waiting_weight = 0;
    int waiting_group_count = 0;
    for (const auto& gs : _groups) {
        if (!gs.waiters.empty()) {
            total_waiting_weight += int64_t(gs.weight);
            ++waiting_group_count;
        }
    }
    if (total_waiting_weight == 0) {
        return false;
    }

    std::array<int64_t, num_group_ids> devs{};
    size_t best_idx = num_group_ids;
    int64_t best_dev = 0;
    const int64_t total_slots = int64_t(_current_total_capacity);
    for (size_t i = 0; i < num_group_ids; ++i) {
        const auto& gs = _groups[i];
        if (gs.waiters.empty()) {
            continue;
        }
        devs[i] = int64_t(gs.in_flight) * total_waiting_weight
                  - total_slots * int64_t(gs.weight);
        if (best_idx == num_group_ids || devs[i] < best_dev) {
            best_dev = devs[i];
            best_idx = i;
        }
    }

    const size_t picked_idx = floor_idx != num_group_ids ? floor_idx : best_idx;
    const bool floor_used = floor_idx != num_group_ids;

    auto& gs = _groups[picked_idx];
    auto& w = gs.waiters.front();
    gs.waiters.pop_front();
    // Eagerly transfer slot ownership to the dispatched waiter's
    // group before signaling. Keeps gs.in_flight consistent with
    // _effective_active_weight across the gap between dispatch and
    // the awaiting coroutine's resumption.
    ++gs.in_flight;
    w.p.set_value();

    if (
      waiting_group_count >= 2
      && (_multi_group_dispatch_counter++ % 100 == 0)) {
        const auto& pu
          = _groups[static_cast<size_t>(group_id::producer_upload)];
        const auto& cf = _groups[static_cast<size_t>(group_id::consumer_fetch)];
        const auto& dg = _groups[static_cast<size_t>(group_id::default_group)];
        vlog(
          log.info,
          "fair_policy: dispatch #{} picked={} reason={} dev={} eaw={} | "
          "pu(if={}[r={}], w={}, min={}, q={}, dev={}) "
          "cf(if={}[r={}], w={}, min={}, q={}, dev={}) "
          "default(if={}[r={}], w={}, min={}, q={}, dev={})",
          _multi_group_dispatch_counter,
          to_string_view(static_cast<group_id>(picked_idx)),
          floor_used ? "floor" : "dev",
          floor_used ? int64_t{0} : best_dev,
          _effective_active_weight,
          pu.in_flight,
          pu.reserved_in_flight,
          pu.weight,
          pu.min_reserved,
          std::distance(pu.waiters.begin(), pu.waiters.end()),
          devs[static_cast<size_t>(group_id::producer_upload)],
          cf.in_flight,
          cf.reserved_in_flight,
          cf.weight,
          cf.min_reserved,
          std::distance(cf.waiters.begin(), cf.waiters.end()),
          devs[static_cast<size_t>(group_id::consumer_fetch)],
          dg.in_flight,
          dg.reserved_in_flight,
          dg.weight,
          dg.min_reserved,
          std::distance(dg.waiters.begin(), dg.waiters.end()),
          devs[static_cast<size_t>(group_id::default_group)]);
    }

    return true;
}

void fair_policy::set_total_slots(size_t desired) {
    if (desired == _current_total_capacity) {
        return;
    }
    if (desired > _current_total_capacity) {
        _shared.signal(desired - _current_total_capacity);
    } else {
        _shared.consume(_current_total_capacity - desired);
    }
    vlog(
      log.info,
      "cloud_io fair_policy total slots: {} -> {}",
      _current_total_capacity,
      desired);
    _current_total_capacity = desired;
}

void fair_policy::set_weight(group_id g, uint32_t weight) {
    const auto idx = static_cast<size_t>(g);
    auto& gs = _groups[idx];
    if (gs.weight == weight) {
        return;
    }
    const auto now = _now_fn();
    refresh_dwell_expirations(now);
    if (is_effective_active(gs, now)) {
        _effective_active_weight += int64_t(weight) - int64_t(gs.weight);
    }
    vlog(
      log.info,
      "cloud_io fair_policy group {} weight: {} -> {}",
      to_string_view(g),
      gs.weight,
      weight);
    gs.weight = weight;
}

uint32_t fair_policy::min_reserved(group_id g) const noexcept {
    return _groups[static_cast<size_t>(g)].min_reserved;
}

void fair_policy::set_min_reserved(group_id g, uint32_t value) {
    const auto idx = static_cast<size_t>(g);
    auto& gs = _groups[idx];
    const uint32_t old_value = gs.min_reserved;
    if (old_value == value) {
        return;
    }
    if (value > old_value) {
        const uint32_t delta = value - old_value;
        _shared.consume(delta);
        _reserved[idx].signal(delta);
    } else {
        const uint32_t delta = old_value - value;
        _reserved[idx].consume(delta);
        _shared.signal(delta);
    }
    gs.min_reserved = value;
}

void fair_policy::set_now_fn_for_test(now_fn_t fn) { _now_fn = std::move(fn); }

void fair_policy::refresh_dwell_expirations(ss::lowres_clock::time_point now) {
    for (auto& gs : _groups) {
        if (is_active(gs)) {
            continue;
        }
        if (gs.last_active == ss::lowres_clock::time_point{}) {
            continue; // never been active
        }
        if (now - gs.last_active >= default_dwell_duration) {
            _effective_active_weight -= int64_t(gs.weight);
            gs.last_active = ss::lowres_clock::time_point{};
        }
    }
}

} // namespace cloud_io
