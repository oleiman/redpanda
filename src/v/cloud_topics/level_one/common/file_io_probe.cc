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
      });
}

} // namespace cloud_topics::l1
