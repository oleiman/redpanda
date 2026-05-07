/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace cloud_io {

/// \brief Per-shard byte-counted admission gate for cloud_storage cache
/// writes.
///
/// Bounds the number of simultaneous in-flight cache-write operations
/// per shard, expressed in bytes. The class is intended to be acquired
/// at the start of the cache-write phase of a cloud-object download
/// (see `remote::download_stream`) and released after the write
/// completes. By bounding concurrent NVMe write queue depth, this
/// indirectly bounds cloud_client lease durations — when local NVMe
/// is saturated, fewer simultaneous writers means each write completes
/// faster, which transitively shortens the lease.
///
/// This is not a network-rate limiter; that role is filled by
/// `io_resources::throttle_download`.
class cache_write_admission {
public:
    /// Construct. `max_bytes` and `min_reservation_bytes` are bound to
    /// `cloud_io_cache_write_admission_max_bytes` and
    /// `cloud_io_cache_write_admission_min_reservation_bytes` respectively.
    cache_write_admission(
      config::binding<uint64_t> max_bytes,
      config::binding<uint64_t> min_reservation_bytes);

    cache_write_admission(const cache_write_admission&) = delete;
    cache_write_admission& operator=(const cache_write_admission&) = delete;
    cache_write_admission(cache_write_admission&&) = delete;
    cache_write_admission& operator=(cache_write_admission&&) = delete;
    ~cache_write_admission() noexcept = default;

    /// Acquire admission for `bytes` bytes worth of cache-write work.
    /// Requests below `min_reservation_bytes()` are rounded up to the
    /// minimum reservation. Requests above the configured maximum are
    /// capped at total capacity to avoid permanent waits.
    ///
    /// \return RAII units; release on destruction.
    /// \throws ss::abort_requested_exception if `as` fires while waiting.
    ss::future<ssx::semaphore_units> wait(uint64_t bytes, ss::abort_source& as);

    /// Current available bytes.
    uint64_t available_bytes() const noexcept { return _sem.current(); }

    /// Current waiters count.
    size_t waiters() const noexcept { return _sem.waiters(); }

    /// Configured maximum.
    uint64_t max_bytes() const noexcept { return _max_bytes(); }

    /// Configured minimum reservation per waiter.
    uint64_t min_reservation_bytes() const noexcept {
        return _min_reservation_bytes();
    }

private:
    /// Resize the semaphore when the bound max changes.
    void on_max_bytes_changed();

    config::binding<uint64_t> _max_bytes;
    config::binding<uint64_t> _min_reservation_bytes;

    /// Initial capacity matches `_max_bytes` at construction; later
    /// resized via `on_max_bytes_changed`.
    uint64_t _current_capacity{0};

    ssx::semaphore _sem;
};

} // namespace cloud_io
