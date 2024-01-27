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

#include <seastar/core/condition-variable.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

namespace transform::logging {
namespace {
// totally arbitrary. will be easier to assess once there's actual I/O in the
// flush fibers
constexpr int FLUSH_MAX_CONCURRENCY = 10;
using namespace std::chrono_literals;
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
  , _flush_jitter(_flush_interval_ms(), 50ms)
  , _buffer_sem(_buffer_limit_bytes, "Log manager buffer semaphore") {
    _flush_interval_ms.watch([this]() {
        _flush_jitter = simple_time_jitter<ClockType>{
          _flush_interval_ms(), 50ms};
    });
}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    ssx::spawn_with_gate(
      _gate, [this]() -> ss::future<> { return flusher_fiber(); });
    return ss::now();
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    _as.request_abort();
    _flush_signal.broken();
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
}

template<typename ClockType>
ss::future<ss::chunked_fifo<iobuf>> manager<ClockType>::do_serialize_log_events(
  model::transform_name_view name, log_event_queue_t events) {
    ss::chunked_fifo<iobuf> ev_json;
    ev_json.reserve(events.size());

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

    co_return std::move(ev_json);
}

template<typename ClockType>
ss::future<std::pair<typename manager<ClockType>::json_batch_table_t, size_t>>
manager<ClockType>::concurrent_serialize_log_events() {
    size_t n_events = 0;
    json_batch_table_t result{};
    co_await ss::max_concurrent_for_each(
      _log_event_queues,
      FLUSH_MAX_CONCURRENCY,
      [this, &result, &n_events](auto& pr) mutable -> ss::future<> {
          auto& [n, q] = pr;
          if (q.empty()) {
              return ss::now();
          }
          // immediately swap in an empty queue. semaphore units associated
          // with buffered events are not returned until the corresponding
          // event is serialized.
          auto events = std::exchange(q, log_event_queue_t{});

          n_events += events.size();
          model::transform_name_view nv{n};
          auto pid = _client->compute_output_partition(nv);

          return do_serialize_log_events(nv, std::move(events))
            .then([name = n, pid, &result](auto ev_json) -> ss::future<> {
                auto [it, _] = result.try_emplace(pid, io::json_batches{});
                it->second.emplace_back(
                  model::transform_name{std::move(name)}, std::move(ev_json));
                return ss::now();
            });
      });
    co_return std::make_pair(std::move(result), n_events);
}

template<typename ClockType>
ss::future<>
manager<ClockType>::do_flush(model::partition_id pid, io::json_batches events) {
    // TODO(oren): it might be a good idea to cap the amount of serialized
    // log data in flight at one time.
    // maybe batching actually happens here

    co_await _client->write(pid, std::move(events));
    co_return;
}

template<typename ClockType>
ss::future<> manager<ClockType>::flush() {
    auto [pre_batches, tot] = co_await concurrent_serialize_log_events();

    co_await ss::max_concurrent_for_each(
      pre_batches,
      FLUSH_MAX_CONCURRENCY,
      [this](auto& pr) mutable -> ss::future<> {
          auto& [pid, evs] = pr;
          return do_flush(pid, std::move(evs));
      });

    vlog(tlg_log.trace, "Processed {} log events", tot);
    co_return;
}

template<typename ClockType>
ss::future<> manager<ClockType>::flusher_fiber() {
    while (!_as.abort_requested()) {
        try {
            // the duration overload passes now() + dur to the timepoint
            // overload, but the now() calculation is tied to an underlying
            // clock type, so it doesn't work with ss::manual_clock. the
            // timepoint overload template has a clocktype parameter, so we use
            // that one to get the behavior we want for testing
            co_await _flush_signal.wait(_flush_jitter());
        } catch (const ss::broken_condition_variable&) {
            break;
        } catch (const ss::condition_variable_timed_out&) {
        }
        co_await flush();
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
        auto res = _log_event_queues.find(name);
        if (res == _log_event_queues.end()) {
            auto [it, ins] = _log_event_queues.emplace(
              ss::sstring{name.data(), name.size()}, log_event_queue_t{});
            if (ins) {
                return it;
            }
            // otherwise something went badly wrong. we'll return the end
            // iterator and fail the operation
        }
        return res;
    };

    auto it = get_queue(transform_name);
    if (it == _log_event_queues.end()) {
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

    if (check_lwm()) {
        _flush_signal.signal();
    }
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform::logging
