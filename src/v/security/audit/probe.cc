/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "security/audit/probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "security/audit/audit_log_manager.h"

#include <seastar/core/metrics.hh>

#include <chrono>

namespace security::audit {

void audit_probe::setup_metrics(const audit_log_manager& mgr) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    auto pct_full_gauge = sm::make_gauge(
      "percent_full",
      [&mgr] { return mgr.percent_full(); },
      sm::description("Audit buffer percent full per shard."));

    if (ss::this_shard_id() == mgr.client_shard_id()) {
        _metrics.add_group(
          prometheus_sanitize::metrics_name("security_audit"),
          {
            sm::make_counter(
              "last_event",
              [this] {
                  return std::chrono::duration_cast<std::chrono::seconds>(
                           _last_event.time_since_epoch())
                    .count();
              },
              sm::description("Timestamp of last successful publish on the "
                              "audit log (seconds since epoch)"))
              .aggregate(aggregate_labels),
            sm::make_counter(
              "errors",
              [this] { return _audit_error_count; },
              sm::description("Running count of errors in creating/publishing "
                              "audit event log entries"))
              .aggregate(aggregate_labels),
            pct_full_gauge.aggregate(aggregate_labels),
            sm::make_gauge(
              "client_percent_full",
              [&mgr] { return mgr.client_percent_full(); },
              sm::description("Audit client semaphore percent full"))
              .aggregate(aggregate_labels),
          });
    } else {
        _metrics.add_group(
          prometheus_sanitize::metrics_name("security_audit"),
          {
            pct_full_gauge.aggregate(aggregate_labels),
          });
    }
}
} // namespace security::audit
