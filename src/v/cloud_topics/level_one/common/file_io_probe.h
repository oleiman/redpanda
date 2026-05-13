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

#include "metrics/metrics.h"

#include <cstdint>

namespace cloud_topics::l1 {

/// Per-shard metrics for file_io. Tracks in-flight L1 cache-download
/// dedup activity: how often a caller issued a fresh download
/// (leader) versus joined an already-in-flight one (waiter). Under
/// K-fanout consumer workloads the waiter count grows with K-1
/// because Kafka partition leadership pins all K fetches for a
/// partition to the same shard.
class file_io_probe {
public:
    file_io_probe();

    void register_dedup_leader() { ++_inflight_dedup_leaders; }
    void register_dedup_waiter() { ++_inflight_dedup_waiters; }

    // New for partition-segment prefetch dedup.
    void register_prefetch_leader() { ++_inflight_prefetch_leaders; }
    void register_prefetch_waiter() { ++_inflight_prefetch_waiters; }
    void register_prefetch_cache_hit() { ++_prefetch_cache_hits; }
    void register_prefetch_download_failure() { ++_prefetch_download_failures; }
    void register_prefetch_reservation_failure() {
        ++_prefetch_reservation_failures;
    }
    void register_dedup_waiter_abort() { ++_dedup_waiter_aborts; }

private:
    void setup_metrics();

    uint64_t _inflight_dedup_leaders{0};
    uint64_t _inflight_dedup_waiters{0};

    // New counters.
    uint64_t _inflight_prefetch_leaders{0};
    uint64_t _inflight_prefetch_waiters{0};
    uint64_t _prefetch_cache_hits{0};
    uint64_t _prefetch_download_failures{0};
    uint64_t _prefetch_reservation_failures{0};
    uint64_t _dedup_waiter_aborts{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
