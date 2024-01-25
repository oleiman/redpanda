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
manager<ClockType>::manager(model::node_id self, std::unique_ptr<sink> ssink)
  : _self(self)
  , _sink(std::move(ssink))
  , _line_limit_bytes(
      config::shard_local_cfg().data_transforms_logging_line_max_bytes.bind())
  , _buffer_limit_bytes(
      config::shard_local_cfg().data_transforms_logging_buffer_capacity_bytes())
  , _buffer_low_water_mark(_buffer_limit_bytes / lwm_denom)
  , _flush_interval_ms(config::shard_local_cfg()
                         .data_transforms_logging_flush_interval_ms.bind())
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
ss::future<> manager<ClockType>::flush() {
    size_t tot = 0;
    // TODO(oren): how much parallelism? any at all?
    co_await ss::max_concurrent_for_each(
      _event_queue,
      FLUSH_MAX_CONCURRENCY,
      ss::coroutine::lambda([this, &tot](auto& pr) -> ss::future<> {
          auto& [n, q] = pr;
          if (q.empty()) {
              co_return;
          }

          // immediately swap in an empty queue. semaphore units associated
          // with buffered events are not returned until the corresponding
          // event is serialized.
          auto ev = std::exchange(q, queue_t{});
          ss::chunked_fifo<iobuf> ev_json;

          while (!ev.empty()) {
              auto e = std::move(ev.front());
              ev.pop_front();
              iobuf b;
              e.event.to_json(model::transform_name_view{n}, b);
              ev_json.emplace_back(std::move(b));

              // reactor will stall if we try to serialize a whole lot of
              // messages all at once
              co_await ss::maybe_yield();
          }

          // semaphore units for the primary buffer are free at this point.

          // TODO(oren): it might be a good idea to cap the amount of serialized
          // log data in flight at one time.

          tot += ev_json.size();
          co_await _sink->write(
            model::transform_name_view{n}, std::move(ev_json));
          co_return;
      }));

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
    auto msg_len = [this](std::string_view message) -> size_t {
        return std::min(_line_limit_bytes(), message.size());
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

    auto b = validate_msg(message);
    if (!b.has_value()) {
        vlog(
          tlg_log.debug,
          "Failed to enqueue transform log: Message validation failed");
        return;
    }

    // Annoyingly, we've already allocated the message copy here, since we
    // need to do control char escaping to determine the final length.
    // Alternatively, we could have a happy path that avoids allocating
    // the message until we've acquired sufficient units in cases where
    // there are no control chars present (happy path).
    auto units = ss::try_get_units(_buffer_sem, b->size());
    if (!units) {
        vlog(tlg_log.debug, "Failed to enqueue transform log: Buffer full");
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
