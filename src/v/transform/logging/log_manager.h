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

#pragma once

#include "config/property.h"
#include "model/transform.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "transform/logging/event.h"
#include "transform/logging/io.h"
#include "wasm/logger.h"

#include <seastar/core/lowres_clock.hh>

#include <absl/container/flat_hash_map.h>

#include <utility>

namespace transform::logging {

template<typename ClockType = ss::lowres_clock>
class manager {
    static_assert(
      std::is_same_v<ClockType, ss::lowres_clock>
        || std::is_same_v<ClockType, ss::manual_clock>,
      "Only lowres or manual clocks are supported");

public:
    manager() = delete;
    ~manager();
    manager(const manager&) = delete;
    manager& operator=(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;

    explicit manager(
      model::node_id,
      std::unique_ptr<client>,
      size_t buffer_cap,
      config::binding<size_t> line_limit,
      config::binding<std::chrono::milliseconds> flush_interval);

    ss::future<> start();
    ss::future<> stop();

    void enqueue_log(
      ss::log_level lvl, model::transform_name_view, std::string_view message);

private:
    // TODO(oren): make configurable?
    static constexpr double lwm_denom = 10;

    ss::future<> flush();
    ss::future<> flusher_fiber();
    bool check_lwm() const;

    model::node_id _self;
    std::unique_ptr<transform::logging::client> _client;
    config::binding<size_t> _line_limit_bytes;
    size_t _buffer_limit_bytes;
    ssize_t _buffer_low_water_mark;
    config::binding<std::chrono::milliseconds> _flush_interval_ms;
    simple_time_jitter<ClockType> _flush_jitter;
    ssx::semaphore _buffer_sem;

    ss::gate _gate{};
    ss::abort_source _as{};
    ss::condition_variable _flush_signal{};

    struct log_event {
        log_event() = delete;
        explicit log_event(event event, ssx::semaphore_units units)
          : event(std::move(event))
          , units(std::move(units)) {}
        event event;
        ssx::semaphore_units units;
    };
    // per @rockwood
    // TODO(oren): Evaluate (and probably substitute) `chunked_vector` once it
    // lands
    using log_event_queue_t = ss::chunked_fifo<log_event>;
    struct string_hash {
        using is_transparent = void;
        [[nodiscard]] size_t operator()(std::string_view txt) const {
            return std::hash<std::string_view>{}(txt);
        }
        [[nodiscard]] size_t operator()(const ss::sstring& txt) const {
            return std::hash<ss::sstring>{}(txt);
        }
    };
    absl::flat_hash_map<
      ss::sstring,
      log_event_queue_t,
      string_hash,
      std::equal_to<>>
      _log_event_queues;

    ss::future<ss::chunked_fifo<iobuf>>
      do_serialize_log_events(model::transform_name_view, log_event_queue_t);

    using json_batch_table_t
      = absl::flat_hash_map<model::partition_id, io::json_batches>;

    ss::future<std::pair<json_batch_table_t, size_t>>
    concurrent_serialize_log_events();

    ss::future<> do_flush(model::partition_id, io::json_batches q);
};

} // namespace transform::logging
