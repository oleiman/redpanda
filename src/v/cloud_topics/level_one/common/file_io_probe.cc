/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

file_io_probe::file_io_probe() { setup_metrics(); }

void file_io_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_level_one_file_io"),
      {
        sm::make_counter(
          "inflight_dedup_leaders",
          [this] { return _inflight_dedup_leaders; },
          sm::description(
            "Number of L1 cache-miss downloads that became the leader for "
            "their (oid, position, size) extent on this shard.")),
        sm::make_counter(
          "inflight_dedup_waiters",
          [this] { return _inflight_dedup_waiters; },
          sm::description(
            "Number of L1 cache-miss callers that joined an already-in-"
            "flight download on this shard rather than issuing a "
            "duplicate.")),
        sm::make_counter(
          "inflight_prefetch_leaders",
          [this] { return _inflight_prefetch_leaders; },
          sm::description(
            "Number of L1 cold-miss callers that became the prefetch "
            "leader for a partition segment on this shard.")),
        sm::make_counter(
          "inflight_prefetch_waiters",
          [this] { return _inflight_prefetch_waiters; },
          sm::description(
            "Number of L1 callers that waited on an already-in-flight "
            "partition-segment prefetch.")),
        sm::make_counter(
          "prefetch_cache_hits",
          [this] { return _prefetch_cache_hits; },
          sm::description(
            "Number of read_object calls served from a cached "
            "partition-segment file via get_stream_range. Every "
            "increment is a request the narrow byte-range cache would "
            "have missed.")),
        sm::make_counter(
          "prefetch_download_failures",
          [this] { return _prefetch_download_failures; },
          sm::description(
            "Number of background partition-segment downloads that "
            "failed. Does not propagate to caller (byte-range path "
            "serves them); future requests retry.")),
        sm::make_counter(
          "prefetch_reservation_failures",
          [this] { return _prefetch_reservation_failures; },
          sm::description(
            "Number of partition-segment prefetches skipped because "
            "cache space reservation failed. Caller's byte-range path "
            "unaffected. Sustained nonzero rate suggests cache budget "
            "is too small for the prefetch working set.")),
        sm::make_counter(
          "dedup_waiter_aborts",
          [this] { return _dedup_waiter_aborts; },
          sm::description(
            "Number of dedup waiters (either byte-range or prefetch "
            "layer) that returned early because the caller's abort_source "
            "fired during the wait.")),
      });
}

} // namespace cloud_topics::l1
