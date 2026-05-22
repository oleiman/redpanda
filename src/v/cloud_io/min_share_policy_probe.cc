/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/min_share_policy_probe.h"

#include "cloud_io/min_share_policy.h"
#include "cloud_io/scheduler_types.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/metrics.hh>

#include <array>
#include <vector>

namespace cloud_io {

namespace {

constexpr std::array<group_id, num_group_ids> all_groups{
  group_id::producer_upload,
  group_id::consumer_fetch,
  group_id::default_group,
};

} // namespace

min_share_policy_probe::min_share_policy_probe(const min_share_policy& policy) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const auto group_name = prometheus_sanitize::metrics_name(
      "cloud_io_scheduler");

    _metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [&policy] { return policy.available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved).")),
        sm::make_gauge(
          "total_capacity",
          [&policy] { return policy.total_capacity(); },
          sm::description("Configured total slot capacity.")),
        sm::make_gauge(
          "total_waiters",
          [&policy] { return policy.total_waiters(); },
          sm::description("Total fibers queued across all groups.")),
      });

    constexpr auto group_label_key = "group_id";

    for (auto g : all_groups) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [&policy, g] { return policy.in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels),
            sm::make_gauge(
              "waiters",
              [&policy, g] { return policy.waiters(g); },
              sm::description("Fibers queued on this group."),
              labels),
            sm::make_counter(
              "admit_total",
              [&policy, g] { return policy.admit_total(g); },
              sm::description("Total admit() calls completed for this group."),
              labels),
            sm::make_counter(
              "admit_immediate_total",
              [&policy, g] { return policy.admit_immediate_total(g); },
              sm::description(
                "admit() calls that took the fast path (no queue)."),
              labels),
            sm::make_gauge(
              "current_reserved",
              [&policy, g] { return policy.current_reserved(g); },
              sm::description(
                "Runtime reservation size. Starts at target_reserved; "
                "reclaimed by the policy when idle past dwell; rebuilt "
                "via refill."),
              labels),
          });
    }

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    const auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _public_metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [&policy] { return policy.available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved)."))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "total_capacity",
          [&policy] { return policy.total_capacity(); },
          sm::description("Configured total slot capacity."))
          .aggregate(aggregate_labels),
      });

    for (auto g : all_groups) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _public_metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [&policy, g] { return policy.in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels)
              .aggregate(aggregate_labels),
            sm::make_gauge(
              "waiters",
              [&policy, g] { return policy.waiters(g); },
              sm::description("Fibers queued on this group."),
              labels)
              .aggregate(aggregate_labels),
          });
    }
}

} // namespace cloud_io
