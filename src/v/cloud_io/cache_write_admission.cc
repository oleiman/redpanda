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

#include "base/vlog.h"
#include "cloud_io/logger.h"

#include <algorithm>

namespace cloud_io {

cache_write_admission::cache_write_admission(
  config::binding<uint64_t> max_bytes,
  config::binding<uint64_t> min_reservation_bytes)
  : _max_bytes(std::move(max_bytes))
  , _min_reservation_bytes(std::move(min_reservation_bytes))
  , _current_capacity(_max_bytes())
  , _sem(_current_capacity, "cloud_io/cache_write_admission") {
    _max_bytes.watch([this] { on_max_bytes_changed(); });
}

ss::future<ssx::semaphore_units>
cache_write_admission::wait(uint64_t bytes, ss::abort_source& as) {
    const auto reservation = std::max<uint64_t>(
      bytes, _min_reservation_bytes());
    // Cap at total capacity to avoid permanent waits when an oversize
    // request arrives.
    const auto capped_reservation = std::min<uint64_t>(
      reservation, _current_capacity);
    co_return co_await ss::get_units(_sem, capped_reservation, as);
}

void cache_write_admission::on_max_bytes_changed() {
    const auto desired = _max_bytes();
    if (desired == _current_capacity) {
        return;
    }
    if (desired > _current_capacity) {
        const auto delta = desired - _current_capacity;
        _sem.signal(delta);
        vlog(
          log.info,
          "cache_write_admission capacity increased: {} -> {} bytes",
          _current_capacity,
          desired);
    } else {
        const auto delta = _current_capacity - desired;
        _sem.consume(delta);
        vlog(
          log.info,
          "cache_write_admission capacity decreased: {} -> {} bytes",
          _current_capacity,
          desired);
    }
    _current_capacity = desired;
}

} // namespace cloud_io
