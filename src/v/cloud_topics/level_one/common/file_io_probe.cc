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
          "concurrent_read_merges",
          [this] { return _concurrent_read_merges; },
          sm::description(
            "L1 reads that joined an already-in-flight download for the "
            "same extent on this shard and resolved with a cache hit on "
            "retry (an avoided S3 GET). Excludes joins that aborted or "
            "saw the in-flight download fail. Sustained rate indicates "
            "concurrent demand on hot extents.")),
        sm::make_counter(
          "merged_read_aborts",
          [this] { return _merged_read_aborts; },
          sm::description(
            "Reads that joined an in-flight L1 download (byte-range or "
            "prefetch) but were aborted before the download "
            "completed.")),
        sm::make_counter(
          "concurrent_prefetch_merges",
          [this] { return _concurrent_prefetch_merges; },
          sm::description(
            "L1 reads that joined an already-in-flight partition-segment "
            "prefetch on this shard.")),
        sm::make_counter(
          "prefetch_cache_hits",
          [this] { return _prefetch_cache_hits; },
          sm::description(
            "L1 reads served from a partition-segment prefetch cache "
            "file.")),
        sm::make_counter(
          "prefetch_download_failures",
          [this] { return _prefetch_download_failures; },
          sm::description(
            "Background partition-segment prefetch downloads that "
            "failed. Does not propagate to caller (byte-range path "
            "serves them); future requests retry.")),
        sm::make_counter(
          "prefetch_reservation_failures",
          [this] { return _prefetch_reservation_failures; },
          sm::description(
            "Partition-segment prefetches skipped because cache space "
            "reservation failed. Caller's byte-range path unaffected. "
            "Sustained nonzero rate suggests cache budget is too small "
            "for the prefetch working set.")),
      });
}

} // namespace cloud_topics::l1
