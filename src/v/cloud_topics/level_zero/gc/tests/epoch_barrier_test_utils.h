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

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "cloud_topics/level_zero/gc/epoch_source.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <atomic>

namespace cloud_topics::l0::gc::testing {

using pinfo = epoch_barrier::partition_source::info;

class mock_partition_source : public epoch_barrier::partition_source {
public:
    struct partition_state {
        model::offset last_epoch_log_offset;
        model::term_id term;
        bool is_leader{true};
        std::optional<model::offset> last_reconciled_log_offset;
    };

    chunked_hash_map<model::ntp, partition_state> partitions;

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, state] : partitions) {
            result.emplace_back(
              ntp,
              info{
                .term = state.term,
                .is_leader = state.is_leader,
                .has_epoch = true,
                .last_epoch_log_offset = state.last_epoch_log_offset,
                .last_reconciled_log_offset = state.last_reconciled_log_offset,
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto it = partitions.find(ntp);
        if (it == partitions.end()) {
            return std::nullopt;
        }
        const auto& state = it->second;
        return info{
          .term = state.term,
          .is_leader = state.is_leader,
          .has_epoch = true,
          .last_epoch_log_offset = state.last_epoch_log_offset,
          .last_reconciled_log_offset = state.last_reconciled_log_offset,
        };
    }
};

inline model::ntp
make_ntp(const ss::sstring& topic, model::partition_id::type pid) {
    return model::ntp(
      model::kafka_namespace, model::topic(topic), model::partition_id(pid));
}

class mock_data_plane : public data_plane_api {
public:
    std::atomic<int> drain_count{0};

    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { return ss::now(); }

    ss::future<std::expected<staged_write, std::error_code>>
    stage_write(chunked_vector<model::record_batch>) override {
        throw std::logic_error("not implemented");
    }

    ss::future<std::expected<chunked_vector<extent_meta>, std::error_code>>
    execute_write(
      model::ntp,
      cluster_epoch,
      staged_write,
      model::timeout_clock::time_point) override {
        throw std::logic_error("not implemented");
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp,
      size_t,
      chunked_vector<extent_meta>,
      model::timeout_clock::time_point,
      model::opt_abort_source_t,
      allow_materialization_failure) override {
        throw std::logic_error("not implemented");
    }

    size_t materialize_max_bytes() const override { return 0; }

    void cache_put(
      const model::topic_id_partition&, const model::record_batch&) override {}

    std::optional<model::record_batch>
    cache_get(const model::topic_id_partition&, model::offset) override {
        return std::nullopt;
    }

    void cache_put_ordered(
      const model::topic_id_partition&,
      chunked_vector<model::record_batch>) override {}

    std::unique_ptr<inflight_write_token> track_inflight_write() override {
        return std::make_unique<inflight_write_token>();
    }

    ss::future<> drain_inflight_writes() override {
        ++drain_count;
        return ss::now();
    }

    ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_epoch(ss::abort_source*) override {
        throw std::logic_error("not implemented");
    }

    ss::future<> cache_wait(
      const model::topic_id_partition&,
      model::offset,
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>) override {
        co_return;
    }
};

/// Mock node_source for single-node tests.
class mock_node_source : public epoch_barrier::node_source {
public:
    explicit mock_node_source(model::node_id self)
      : _self(self) {}

    model::node_id self() const override { return _self; }
    std::vector<model::node_id> node_ids() const override { return {_self}; }

private:
    model::node_id _self;
};

/// Mock epoch_source that returns a fixed candidate epoch. Only
/// max_barrier_candidate_epoch is implemented; the other methods are
/// unused by the barrier loop.
class mock_epoch_source : public epoch_source {
public:
    std::atomic<int64_t> candidate{-1}; // -1 = nullopt

    ss::future<std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(ss::abort_source*) override {
        co_return std::nullopt;
    }

    ss::future<std::expected<std::optional<cluster_epoch>, std::string>>
    max_barrier_candidate_epoch(ss::abort_source*) override {
        auto c = candidate.load();
        if (c < 0) {
            co_return std::optional<cluster_epoch>{std::nullopt};
        }
        co_return std::optional{cluster_epoch(c)};
    }

    ss::future<std::expected<partitions_snapshot, std::string>>
    get_partition_snapshot(ss::abort_source*) override {
        co_return std::unexpected("not implemented");
    }

    ss::future<std::expected<partition_epoch_estimates, std::string>>
    get_partition_epoch_estimates(ss::abort_source*) override {
        co_return std::unexpected("not implemented");
    }
};

} // namespace cloud_topics::l0::gc::testing
