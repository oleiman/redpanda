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
#include "random/simple_time_jitter.h"
#include "strings/utf8.h"
#include "transform/logging/io.h"
#include "transform/logging/logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

namespace transform::logging {
namespace {
constexpr int FLUSH_MAX_CONCURRENCY = 10;
using namespace std::chrono_literals;
} // namespace

namespace detail {

template<typename ClockType>
class flusher {
public:
    flusher() = delete;
    explicit flusher(
      ss::abort_source* as,
      std::unique_ptr<transform::logging::client> c,
      config::binding<std::chrono::milliseconds> fi,
      ClockType::duration jitter)
      : _as(as)
      , _client(std::move(c))
      , _flush_interval_ms(std::move(fi))
      , _flush_jitter(_flush_interval_ms(), jitter) {
        _flush_interval_ms.watch([this, jitter]() {
            _flush_jitter = simple_time_jitter<ClockType>{
              _flush_interval_ms(), jitter};
        });
    }

    template<typename BuffersT>
    ss::future<> start(ss::gate& gate, BuffersT* bufs) {
        ssx::spawn_with_gate(
          gate, [this, bufs]() -> ss::future<> { return flush_loop(bufs); });
        return ss::now();
    }

    void wakeup() { _flush_signal.signal(); }
    ss::future<> stop() {
        _flush_signal.broken();
        return ss::now();
    }

    ss::future<> flush_loop(auto* bufs) {
        while (!_as->abort_requested()) {
            try {
                // the duration overload passes now() + dur to the timepoint
                // overload, but the now() calculation is tied to an underlying
                // clock type, so it doesn't work with ss::manual_clock. the
                // timepoint overload template has a clocktype parameter, so we
                // use that one to get the behavior we want for testing
                if constexpr (std::is_same_v<ClockType, ss::manual_clock>) {
                    co_await _flush_signal.wait(
                      ClockType::now() + _flush_jitter.base_duration());
                } else {
                    co_await _flush_signal.wait(_flush_jitter());
                }
            } catch (const ss::broken_condition_variable&) {
                break;
            } catch (const ss::condition_variable_timed_out&) {
            }
            co_await flush(bufs);
        }
        co_return;
    }

    ss::future<> flush(auto* bufs) {
        size_t n_events = 0;
        absl::flat_hash_map<model::partition_id, io::json_batches> batches{};

        if (bufs == nullptr) {
            vlog(tlg_log.error, "Missing buffers");
            co_return;
        }

        co_await ss::max_concurrent_for_each(
          *bufs, FLUSH_MAX_CONCURRENCY, [this, &batches, &n_events](auto& pr) {
              auto& [n, q] = pr;
              if (q.empty()) {
                  return ss::now();
              }
              // immediately swap in an empty queue. semaphore units associated
              // with buffered events are not returned until the corresponding
              // event is serialized.
              auto events = std::exchange(q, {});

              n_events += events.size();
              model::transform_name_view nv{n};
              auto pid = _client->compute_output_partition(nv);

              return do_serialize_log_events(nv, std::move(events))
                .then([name = n, pid, &batches](auto ev_json) -> ss::future<> {
                    auto [it, _] = batches.try_emplace(pid, io::json_batches{});
                    it->second.emplace_back(
                      model::transform_name{std::move(name)},
                      std::move(ev_json));
                    return ss::now();
                });
          });

        co_await ss::max_concurrent_for_each(
          batches,
          FLUSH_MAX_CONCURRENCY,
          [this](auto& pr) mutable -> ss::future<> {
              auto& [pid, evs] = pr;
              return do_flush(pid, std::move(evs));
          });

        vlog(tlg_log.trace, "Processed {} log events", n_events);
        co_return;
    }

    ss::future<ss::chunked_fifo<iobuf>>
    do_serialize_log_events(model::transform_name_view name, auto events) {
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

    ss::future<> do_flush(model::partition_id pid, io::json_batches events) {
        // TODO(oren): it might be a good idea to cap the amount of serialized
        // log data in flight at one time.
        // maybe batching actually happens here

        co_await _client->write(pid, std::move(events));
        co_return;
    }

private:
    ss::abort_source* _as = nullptr;
    ss::condition_variable _flush_signal{};
    std::unique_ptr<transform::logging::client> _client{};
    config::binding<std::chrono::milliseconds> _flush_interval_ms;
    simple_time_jitter<ClockType> _flush_jitter;
};

template class flusher<ss::lowres_clock>;
template class flusher<ss::manual_clock>;

} // namespace detail

template<typename ClockType>
manager<ClockType>::manager(
  model::node_id self,
  std::unique_ptr<client> c,
  size_t bc,
  config::binding<size_t> ll,
  config::binding<std::chrono::milliseconds> fi,
  std::optional<typename ClockType::duration> jitter)
  : _self(self)
  , _line_limit_bytes(std::move(ll))
  , _buffer_limit_bytes(bc)
  , _buffer_low_water_mark(_buffer_limit_bytes / lwm_denom)
  , _buffer_sem(_buffer_limit_bytes, "Log manager buffer semaphore")
  , _flusher(std::make_unique<detail::flusher<ClockType>>(
      &_as, std::move(c), std::move(fi), jitter.value_or(50ms))) {}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    return _flusher->template start<>(_gate, &_log_buffers);
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    _as.request_abort();
    co_await _flusher->stop();
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
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
        auto res = _log_buffers.find(name);
        if (res == _log_buffers.end()) {
            auto [it, ins] = _log_buffers.emplace(
              ss::sstring{name.data(), name.size()}, buffer_t{});
            if (ins) {
                return it;
            }
            // otherwise something went badly wrong. we'll return the end
            // iterator and fail the operation
        }
        return res;
    };

    auto it = get_queue(transform_name);
    if (it == _log_buffers.end()) {
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
        _flusher->wakeup();
    }
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform::logging
