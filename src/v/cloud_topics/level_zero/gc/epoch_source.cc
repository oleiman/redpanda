/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_source.h"

#include "cloud_topics/logger.h"
#include "cluster/controller_stm.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace {
constexpr ss::lowres_clock::duration health_report_query_timeout = 10s;
} // namespace

namespace cloud_topics::l0::gc {

seastar::future<std::expected<std::optional<cluster_epoch>, std::string>>
epoch_source::max_barrier_candidate_epoch(seastar::abort_source* as) {
    // Collect the partition snapshot (centralized, from the topic table)
    // and per-partition epoch estimates (distributed, from health reports).
    auto partitions = co_await get_partition_snapshot(as);
    if (!partitions.has_value()) {
        co_return std::unexpected(partitions.error());
    }
    if (partitions.value().partitions.empty()) {
        co_return std::nullopt;
    }

    auto estimates = co_await get_partition_epoch_estimates(as);
    if (!estimates.has_value()) {
        co_return std::unexpected(estimates.error());
    }

    // The candidate is the minimum across the snapshot revision and all
    // partition estimates. Any single partition can hold it back.
    auto result = partitions.value().snap_revision;

    vlog(
      cd_log.debug,
      "Calculating barrier candidate epoch with snapshot epoch {}",
      result);

    for (const auto& partition : partitions.value().partitions) {
        const auto& tp_ns = partition.first;
        auto nit = estimates.value().find(tp_ns);
        if (nit == estimates.value().end()) {
            co_return std::unexpected(
              fmt::format(
                "Topic '{}' in snapshot has no epoch estimate", tp_ns));
        }

        for (const auto p_id : partition.second) {
            auto pit = nit->second.find(p_id);
            if (pit == nit->second.end()) {
                co_return std::unexpected(
                  fmt::format(
                    "Partition '{}/{}' in snapshot has no epoch estimate",
                    tp_ns,
                    p_id));
            }

            const auto prev_result = result;
            result = std::min(result, pit->second);

            vlog(
              cd_log.debug,
              "Reducing result {} from min(result={}, p={}) for {}/{}",
              result,
              prev_result,
              pit->second,
              tp_ns,
              p_id);
        }
    }

    co_return result;
}

namespace {

class epoch_source_impl : public epoch_source {
public:
    explicit epoch_source_impl(
      seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
      seastar::sharded<cluster::controller_stm>* controller_stm,
      seastar::sharded<cluster::topic_table>* topic_table,
      safe_epoch_fn safe_epoch)
      : health_monitor_(health_monitor)
      , controller_stm_(controller_stm)
      , topic_table_(topic_table)
      , safe_epoch_(std::move(safe_epoch)) {}

    seastar::future<std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(seastar::abort_source*) override {
        if (safe_epoch_) {
            auto epoch = safe_epoch_();
            if (epoch.has_value() && probe_) {
                probe_->set_min_partition_gc_epoch(*epoch);
            }
            co_return epoch;
        }
        co_return std::nullopt;
    }

    seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partition_snapshot(seastar::abort_source* as) override {
        const auto& topic_table = topic_table_->local();

        // this revision is for detecting concurrent modifications
        const auto iter_start_rev = topic_table.topics_map_revision();

        /*
         * The controller stm last applied offset is used, as opposed to using
         * the topic table last applied offset, because we need the version to
         * move forward. the controller stm offset is a at the top of the stm
         * hierarchy and is consistent with the topic table last applied offset.
         */
        partitions_snapshot snap;
        snap.snap_revision = cluster_epoch(
          co_await controller_stm_->invoke_on(
            cluster::controller_stm_shard, [](auto& stm) {
                return model::revision_id(stm.get_last_applied_offset());
            }));

        for (const auto& topic : topic_table.topics_map()) {
            // we only care about cloud topics
            if (!topic.second.get_metadata()
                   .get_configuration()
                   .is_cloud_topic()) {
                continue;
            }

            auto& partitions = snap.partitions[topic.first];
            for (const auto& partition : topic.second.partitions) {
                partitions.push_back(model::partition_id(partition.first));
            }

            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }

            // Detect concurrent changes to the topic table to avoid accessing
            // an invalid iterator.
            try {
                topic_table.check_topics_map_stable(iter_start_rev);
            } catch (...) {
                // TODO: its rare, so should we retry immediately or abort this
                // round and wait for the next GC loop? i think it's a balance
                // of more code/complexity and behavior. For now I think it is
                // fine.
                co_return std::unexpected(
                  "Concurrent container iteration invalidation. Will retry");
            }
        }

        co_return snap;
    }

    seastar::future<std::expected<partition_epoch_estimates, std::string>>
    get_partition_epoch_estimates(seastar::abort_source* as) override {
        /*
         * Get a recent health report. Partitions use the health reporting
         * mechanism to self-report GC candidate epochs.
         */

        auto health_report
          = co_await health_monitor_->local().get_cluster_health(
            cluster::cluster_report_filter{},
            cluster::force_refresh::no,
            model::timeout_clock::now() + health_report_query_timeout);

        if (!health_report.has_value()) {
            co_return std::unexpected(
              fmt::format(
                "Error retrieving cluster health report: {}",
                health_report.error()));
        }

        partition_epoch_estimates result;
        for (const auto& node_health : health_report.value().node_reports) {
            for (const auto& topic_status : node_health->topics) {
                const auto& tp_ns = topic_status.first;
                for (const auto& partition_status : topic_status.second) {
                    /*
                     * calculate the max gc epoch for each partition. the catch
                     * here is that this value is reported through the health
                     * reporting system, and that system reports information for
                     * all partition replicas (leader and followers). so how do
                     * we know which value to use here? first, the max gc epoch
                     * only increases in value. second, we recognize that
                     * all reported values from any replica are valid at (and
                     * forever after) the moment they are reported. third, only
                     * the leader advances the epoch.
                     *
                     * because of the second point, using max gc epoch from any
                     * replica will result in correct behavior, however it may
                     * be pessimistic. using the one from the leader is better,
                     * but leadership is a lagging signal. instead, we can take
                     * the maximum reported as the most optimistic value.
                     *
                     * if a replica reports no epoch then it is considered to be
                     * in an indeterminite state and it has no affect on the
                     * computed result (effectively it is treated as having
                     * epoch 0 in the max reduction across replicas).
                     *
                     * if all replicas report no epoch then the partition is not
                     * included in the result set returned to the caller. this
                     * covers two cases.
                     *
                     * the first case is that the partition is part of a
                     * standard topic. in this case the partition will also not
                     * be in the set returned by `get_partitions` and thus the
                     * join in `max_gc_eligible_epoch` will ignore the topic.
                     *
                     * in the second case the join would fail, and later succeed
                     * in the once at least one replica is returning max gc
                     * epoch. this shouldn't be a problem in practice: there is
                     * a narrow window at start-up time where a partition is
                     * bootstrapping the L0 CT STM state where the state is
                     * unknown. for brand new partitions this should be the
                     * partition's creation revision ID.
                     */
                    // TODO(oren): fix up comments
                    // Health reports include all replicas. We take the
                    // max across replicas: epoch estimates only increase,
                    // and any replica's report is valid forever after the
                    // moment it was observed. Replicas with no estimate
                    // are skipped (treated as epoch 0 in the max). If no
                    // replica reports an estimate, the partition is omitted
                    // from the result — the join in
                    // max_barrier_candidate_epoch will fail or the
                    // partition is a non-cloud topic (not in the snapshot).
                    const auto maybe_max_gc_epoch
                      = partition_status.second
                          .cloud_topic_max_gc_eligible_epoch;
                    if (!maybe_max_gc_epoch.has_value()) {
                        continue;
                    }
                    const auto max_gc_epoch = cluster_epoch(
                      maybe_max_gc_epoch.value());

                    const auto p_id = partition_status.first;
                    auto& partition_epochs = result[tp_ns];
                    const auto it = partition_epochs.find(p_id);
                    if (it == partition_epochs.end()) {
                        partition_epochs.try_emplace(p_id, max_gc_epoch);
                    } else {
                        it->second = std::max(it->second, max_gc_epoch);
                    }
                }
            }

            /*
             * A scheduling point is injected after looking at each node's
             * report. We own the list of node reports which is a set of shared
             * pointers, so iteration is safe, and the scheduling point is
             * intended to help avoid reactor stalls. If we need to inject
             * scheduling points at a finer granularity we'll need to take a
             * closer look at concurrency rules of the reports themselves.
             */
            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }
        }

        co_return result;
    }

private:
    seastar::sharded<cluster::health_monitor_frontend>* health_monitor_;
    seastar::sharded<cluster::controller_stm>* controller_stm_;
    seastar::sharded<cluster::topic_table>* topic_table_;
    safe_epoch_fn safe_epoch_;
};

} // namespace

std::unique_ptr<epoch_source> epoch_source::make_default(
  seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
  seastar::sharded<cluster::controller_stm>* controller_stm,
  seastar::sharded<cluster::topic_table>* topic_table,
  safe_epoch_fn safe_epoch) {
    return std::make_unique<epoch_source_impl>(
      health_monitor, controller_stm, topic_table, std::move(safe_epoch));
}

} // namespace cloud_topics::l0::gc
