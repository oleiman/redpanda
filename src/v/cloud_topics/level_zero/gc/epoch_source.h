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

#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"
#include "cloud_topics/types.h"
#include "cluster/fwd.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include <expected>
#include <memory>
#include <optional>

namespace cloud_topics::l0::gc {

/// Interface for computing the maximum epoch eligible for GC.
class epoch_source {
public:
    struct partitions_snapshot {
        using partition_map = chunked_hash_map<
          model::topic_namespace,
          chunked_vector<model::partition_id>,
          model::topic_namespace_hash,
          model::topic_namespace_eq>;

        partition_map partitions;
        cluster_epoch snap_revision;
    };

    using partition_epoch_estimates = chunked_hash_map<
      model::topic_namespace,
      chunked_hash_map<model::partition_id, cluster_epoch>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    /// Injection point for a callback that should return a value that clients
    /// can treat as the safe-to-GC epoch.
    using safe_epoch_fn
      = ss::noncopyable_function<std::optional<cluster_epoch>()>;

    epoch_source() = default;
    epoch_source(const epoch_source&) = default;
    epoch_source(epoch_source&&) = delete;
    epoch_source& operator=(const epoch_source&) = default;
    epoch_source& operator=(epoch_source&&) = delete;
    virtual ~epoch_source() = default;

    void set_probe(level_zero_gc_probe* p) { probe_ = p; }

    /// L0 objects with epochs <= the return value may be deleted. An
    /// expected return value of std::nullopt is not an error, but rather
    /// indicates that no GC eligible epoch could yet be determined.
    ///
    /// Implementations typically read the safe epoch from the local
    /// epoch_barrier, which is the authoritative result of the barrier
    /// protocol.
    virtual seastar::future<
      std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(seastar::abort_source*) = 0;

    /// Candidate epoch derived from health-report data. This is the
    /// value fed into the barrier protocol; it has NOT yet been through
    /// the barrier and therefore must NOT be used directly for deletion.
    virtual seastar::future<
      std::expected<std::optional<cluster_epoch>, std::string>>
    max_barrier_candidate_epoch(seastar::abort_source*);

    /// Snapshot of cloud topic partition identifiers along with a revision
    /// that caps the candidate epoch.
    virtual seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partition_snapshot(seastar::abort_source*) = 0;

    /// Per-partition epoch estimates collected from health reports.
    virtual seastar::future<
      std::expected<partition_epoch_estimates, std::string>>
    get_partition_epoch_estimates(seastar::abort_source*) = 0;

    /// Create the default production implementation.
    ///
    /// @param safe_epoch  Optional callback returning the barrier's
    ///                    safe epoch. When null, max_gc_eligible_epoch()
    ///                    returns nullopt (suitable for the epoch_barrier's
    ///                    own usage where only max_barrier_candidate_epoch
    ///                    is needed).
    static std::unique_ptr<epoch_source> make_default(
      seastar::sharded<cluster::health_monitor_frontend>*,
      seastar::sharded<cluster::controller_stm>*,
      seastar::sharded<cluster::topic_table>*,
      safe_epoch_fn safe_epoch = {});

protected:
    level_zero_gc_probe* probe_{nullptr};
};

} // namespace cloud_topics::l0::gc
