/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/logging/log_manager.h"

#include "base/vassert.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "strings/utf8.h"
#include "transform/logging/io.h"
#include "transform/logging/logger.h"

#include <seastar/core/smp.hh>

namespace transform::logging {
namespace {
// totally arbitrary. will be easier to assess once there's actual I/O in the
// flush fibers
constexpr int FLUSH_MAX_CONCURRENCY = 10;
} // namespace

template<typename ClockType>
manager<ClockType>::manager(
  model::node_id self,
  std::unique_ptr<client> c,
  size_t bc,
  config::binding<size_t> ll,
  config::binding<std::chrono::milliseconds> fi)
  : _self(self)
  , _client(std::move(c))
  , _line_limit_bytes(std::move(ll))
  , _buffer_limit_bytes(bc)
  , _buffer_low_water_mark(_buffer_limit_bytes / lwm_denom)
  , _flush_interval_ms(std::move(fi))
  , _buffer_sem(_buffer_limit_bytes, "Log manager buffer semaphore") {
    _flush_timer.set_callback(
      [this]() { ssx::spawn_with_gate(_gate, [this]() { return flush(); }); });
}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    _as = ss::abort_source{};
    _flush_timer.arm(_flush_interval_ms());
    return ss::now();
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    _flush_timer.cancel();
    _as.request_abort();
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
    // TODO(oren): last gasp
    // ticket: https://github.com/redpanda-data/core-internal/issues/1036
    // co_return co_await flush();
}

template<typename ClockType>
ss::future<>
manager<ClockType>::do_flush(model::transform_name_view name, queue_t events) {
    ss::chunked_fifo<iobuf> ev_json;

    while (!events.empty()) {
        auto e = std::move(events.front());
        events.pop_front();
        iobuf b;
        e.event.to_json(name, b);
        ev_json.emplace_back(std::move(b));

        // reactor will stall if we try to serialize a whole lot of
        // messages all at once
        co_await ss::maybe_yield();
    }

    // semaphore units for the primary buffer are free at this point.

    // TODO(oren): it might be a good idea to cap the amount of serialized
    // log data in flight at one time.

    co_await _client->write(name, std::move(ev_json));
    co_return;
}

template<typename ClockType>
ss::future<> manager<ClockType>::flush() {
    size_t tot = 0;
    // TODO(oren): how much parallelism? any at all?
    co_await ss::max_concurrent_for_each(
      _event_queue,
      FLUSH_MAX_CONCURRENCY,
      [this, &tot](auto& pr) -> ss::future<> {
          auto& [n, q] = pr;
          if (q.empty()) {
              return ss::now();
          }
          tot += q.size();

          // immediately swap in an empty queue. semaphore units associated
          // with buffered events are not returned until the corresponding
          // event is serialized.
          auto ev = std::exchange(q, queue_t{});
          return do_flush(model::transform_name_view{n}, std::move(ev));
      });

    vlog(tlg_log.trace, "Processed {} log events", tot);
    if (!_as.abort_requested()) {
        _flush_timer.arm(_flush_interval_ms());
    }
    co_return;
}

template<typename ClockType>
bool manager<ClockType>::check_lwm() const {
    return _buffer_sem.available_units() <= _buffer_low_water_mark;
}

template<typename ClockType>
void manager<ClockType>::enqueue_log(
  ss::log_level level,
  model::transform_name_view transform_name,
  std::string_view message) {
    // capture line limit exactly once to avoid any weirdness
    auto msg_len =
      [lim = _line_limit_bytes()](std::string_view message) -> size_t {
        return std::min(lim, message.size());
    };

    auto validate_msg =
      [&msg_len](std::string_view message) -> std::optional<ss::sstring> {
        auto sub_view = message.substr(0, msg_len(message));
        if (!is_valid_utf8(sub_view)) {
            return std::nullopt;
        } else if (contains_control_character(sub_view)) {
            // escape control chars and truncate (again, if necessary)
            auto res = replace_control_chars_in_string(sub_view);
            return res.substr(0, msg_len(res));
        }
        return ss::sstring{sub_view.data(), sub_view.size()};
    };

    auto get_queue = [this](std::string_view name) {
        auto res = _event_queue.find(name);
        if (res == _event_queue.end()) {
            auto [it, ins] = _event_queue.emplace(
              ss::sstring{name.data(), name.size()}, queue_t{});
            if (ins) {
                return it;
            }
            // otherwise something went badly wrong. we'll return the end
            // iterator and fail the operation
        }
        return res;
    };

    auto it = get_queue(transform_name);
    if (it == _event_queue.end()) {
        vlog(tlg_log.warn, "Failed to enqueue transform log: Buffer error");
        return;
    }

    // Unfortunately, we don't know how long an escaped string will be
    // until we've allocated memory for it. So we optimistically grab
    // units for the unmodified log message here, hoping that, in the
    // common case, no control chars are present. If the message fails
    // validation, we will simply return the units to _buffer_sem on
    // function return.
    // NOTE(oren): we can still truncate up front w/o allocating
    message = message.substr(0, msg_len(message));
    auto units = ss::try_get_units(_buffer_sem, message.size());
    if (!units) {
        vlog(tlg_log.debug, "Failed to enqueue transform log: Buffer full");
        return;
    }

    auto b = validate_msg(message);
    if (!b.has_value()) {
        vlog(
          tlg_log.debug,
          "Failed to enqueue transform log: Message validation failed");
        return;
    }

    it->second.emplace_back(
      event{_self, event::clock_type::now(), level, std::move(*b)},
      std::move(*units));

    // if the timer is not armed, we're either shutting down or there's a flush
    // in progress. in either case, don't bother expediting a flush.
    if (check_lwm() && _flush_timer.armed()) {
        _flush_timer.rearm(ClockType::now());
    }
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform::logging
