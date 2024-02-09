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

#include "transform/logging/probes.h"

#include "base/seastarx.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics_registration.hh>

namespace transform::logging {

void logger_probe::setup_metrics(model::transform_name_view transform_name) {
    namespace sm = ss::metrics;

    const auto name_label = sm::label("transform_name");
    const std::vector<sm::label_instance> labels = {
      name_label(transform_name()),
    };

    auto setup_common =
      [this, &labels](const std::vector<sm::label>& aggregate_labels) {
          std::vector<sm::metric_definition> defs;
          defs.emplace_back(
            sm::make_counter(
              "events_total",
              [this]() { return _total_log_events; },
              sm::description("Running count of transform log events"),
              labels)
              .aggregate(aggregate_labels));
          defs.emplace_back(
            sm::make_counter(
              "events_dropped_total",
              [this]() { return _total_dropped_log_events; },
              sm::description("Running count of dropped transform log events"),
              labels)
              .aggregate(aggregate_labels));
          return defs;
      };

    auto group_name = prometheus_sanitize::metrics_name(
      "data_transforms_logger");

    if (!config::shard_local_cfg().disable_metrics()) {
        const auto aggregate_labels
          = config::shard_local_cfg().aggregate_metrics()
              ? std::vector<sm::label>{sm::shard_label, name_label}
              : std::vector<sm::label>{};

        _metrics.add_group(group_name, setup_common(aggregate_labels));
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        const auto aggregate_labels
          = config::shard_local_cfg().aggregate_metrics()
              ? std::vector<sm::label>{sm::shard_label, name_label}
              : std::vector<sm::label>{sm::shard_label};
        _public_metrics.add_group(group_name, setup_common(aggregate_labels));
    }
}

void manager_probe::setup_metrics(std::function<double()> get_usage_ratio) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto setup = [this, fn = std::move(get_usage_ratio)](
                   const std::vector<sm::label>& aggregate_labels) {
        std::vector<sm::metric_definition> defs;
        defs.emplace_back(
          sm::make_gauge(
            "buffer_usage_ratio",
            [fn] { return fn(); },
            sm::description("Transform log manager buffer usage ratio"))
            .aggregate(aggregate_labels));
        defs.emplace_back(sm::make_counter(
          "write_errors_total",
          [this] { return _total_write_errors; },
          sm::description("Running count of errors while writing to the "
                          "transform logs topic")));
        return defs;
    };

    auto group_name = prometheus_sanitize::metrics_name(
      "data_transforms_log_manager");

    if (!config::shard_local_cfg().disable_metrics()) {
        const auto aggregate_labels
          = config::shard_local_cfg().aggregate_metrics()
              ? std::vector<sm::label>{sm::shard_label}
              : std::vector<sm::label>{};

        auto defs = setup(aggregate_labels);

        _metrics.add_group(group_name, defs);
    }
}
} // namespace transform::logging
