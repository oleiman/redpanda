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

#include "base/seastarx.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>
#include <optional>
#include <utility>

namespace cluster {
template<typename Clock>
class cluster_epoch_service;
class partition_manager;
class members_table;
class health_monitor_frontend;
class controller_stm;
class topic_table;
} // namespace cluster

namespace rpc {
class connection_cache;
} // namespace rpc

namespace cloud_topics {
class data_plane_api;
} // namespace cloud_topics

namespace cloud_topics::l0::gc {

class epoch_source;

/// Establishes a cluster-wide safe-to-GC epoch for L0 garbage collection.
///
/// GC must not delete L0 objects at epoch E unless no future writes will
/// use epoch <= E and all existing data at epoch <= E has been reconciled
/// to L1.
///
/// **Leader (barrier_loop):** Computes a candidate epoch from health
/// reports, fans out advance_barrier RPCs to all nodes concurrently,
/// polls until every node reports ready, then publishes the safe epoch.
/// Loop and poll intervals are cluster-tunable.
///
/// **Handler (handle_barrier):** On first call for a new candidate:
/// invalidates the epoch cache, kicks off an async drain of in-flight
/// writes, returns pending immediately (drain does not block the RPC).
/// On subsequent calls: checks drain completion, collects seal points
/// (last placeholder log offset + term per leader partition), verifies
/// LRO >= each seal. Returns ready when all local shards are reconciled.
/// Stalls if any leader partition has an epoch but no placeholder offset
/// (waiting for the housekeeper to advance the epoch).
///
/// Safe epoch is piggybacked on every barrier RPC and ratcheted forward
/// via max() on the handler side.
class epoch_barrier : public ss::peering_sharded_service<epoch_barrier> {
public:
    /// Abstraction over partition_manager for testability. The barrier
    /// only needs to enumerate cloud topic partitions and look up
    /// individual ones.
    class partition_source {
    public:
        struct info {
            model::term_id term;
            bool is_leader;
            bool has_epoch;
            std::optional<model::offset> last_epoch_log_offset;
            std::optional<model::offset> last_reconciled_log_offset;
        };
        virtual ~partition_source() = default;

        /// All kafka-namespace cloud topic partitions on this shard.
        virtual chunked_vector<std::pair<model::ntp, info>>
        cloud_topic_partitions() const = 0;

        /// Look up a specific partition. Returns nullopt if not found or
        /// not a cloud topic.
        virtual std::optional<info> get(const model::ntp&) const = 0;
    };

    /// Factory for the default production partition source backed by
    /// cluster::partition_manager.
    static std::unique_ptr<partition_source>
    make_default_partition_source(cluster::partition_manager&);

    /// Abstraction over cluster membership for testability. The barrier
    /// needs the local node's ID and the set of all node IDs.
    class node_source {
    public:
        virtual ~node_source() = default;
        virtual model::node_id self() const = 0;
        virtual std::vector<model::node_id> node_ids() const = 0;
    };

    /// Factory for the default production node source backed by
    /// cluster::members_table.
    static std::unique_ptr<node_source>
    make_default_node_source(model::node_id, cluster::members_table&);

    /// Result of a single advance_barrier call.
    enum class barrier_status : uint8_t {
        ready,
        pending,
        error,
    };

    /// Production constructor. Builds the epoch_source internally with
    /// the safe_epoch callback wired to this barrier.
    epoch_barrier(
      ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
        epoch_service,
      data_plane_api& data_plane,
      std::unique_ptr<partition_source> partitions,
      std::unique_ptr<node_source> nodes,
      ss::sharded<::rpc::connection_cache>* connections,
      ss::sharded<cluster::health_monitor_frontend>* health_monitor,
      ss::sharded<cluster::controller_stm>* controller_stm,
      ss::sharded<cluster::topic_table>* topic_table);

    /// Test constructor. Takes a pre-built epoch_source.
    epoch_barrier(
      ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
        epoch_service,
      data_plane_api& data_plane,
      std::unique_ptr<partition_source> partitions,
      std::unique_ptr<node_source> nodes,
      ss::sharded<::rpc::connection_cache>* connections,
      std::unique_ptr<epoch_source> epoch_src);

    ~epoch_barrier();

    ss::future<> stop();

    /// Core barrier method. Called on every node (via RPC dispatch or
    /// advance_local). Always runs on shard 0 — callers must dispatch.
    ///
    /// 1. If safe_epoch is present, publishes it idempotently via max.
    /// 2. If new candidate: invalidates epoch cache, kicks off async
    ///    drain, returns pending.
    /// 3. If draining: checks completion. On success, collects seal
    ///    points (stalls if any partition lacks a placeholder offset).
    /// 4. Checks reconciliation (LRO >= seal) across all shards.
    ss::future<barrier_status> handle_barrier(
      cluster_epoch candidate, std::optional<cluster_epoch> safe_epoch);

    /// Returns the latest safe-to-GC epoch, or nullopt if none yet.
    std::optional<cluster_epoch> safe_epoch() const noexcept {
        return _safe_epoch;
    }

    /// The epoch_source owned by this barrier. Both the barrier loop
    /// (for candidate computation) and GC (for safe epoch) use it.
    epoch_source* get_epoch_source() { return _epoch_source.get(); }

    /// Start or stop the leader-side background loop.
    ss::future<> set_leader(bool is_leader);

    /// Fire-and-forget leadership notification. Bridges the synchronous
    /// leadership callback into the async set_leader method.
    void notify_leadership_change(bool is_leader) noexcept;

private:
    ss::future<> publish_safe_epoch(cluster_epoch candidate);
    ss::future<bool>
    advance_local(cluster_epoch candidate, std::optional<cluster_epoch> safe);
    ss::future<bool> advance_remote(
      model::node_id node_id,
      cluster_epoch candidate,
      std::optional<cluster_epoch> safe);
    ss::future<bool> fan_out_advance_barrier(cluster_epoch candidate);

    struct seal_point {
        model::offset committed;
        model::term_id term;
    };

    using partition_seals = chunked_hash_map<model::partition_id, seal_point>;
    using seal_map = chunked_hash_map<model::topic, partition_seals>;

    struct round_state {
        cluster_epoch candidate;
        seal_map seals;
    };

    /// Result of checking seal points on a single shard. Values are
    /// ordered so that std::max over shards yields the worst case.
    enum class seal_check_result : uint8_t {
        reconciled,
        pending,
        stale,
    };

    bool collect_local_seal_points(cluster_epoch candidate);
    seal_check_result check_local_seal_points();
    seal_point* find_seal(const model::topic& topic, model::partition_id pid);
    void upsert_seal(
      const model::topic& topic, model::partition_id pid, seal_point sp);

    class barrier_loop;

    // Every-shard state.
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
      _epoch_service;
    data_plane_api& _data_plane;
    std::unique_ptr<partition_source> _partitions;
    std::optional<round_state> _round;
    // Shard-0 only: in-progress drain future for the current round.
    // Set when a new round starts; consumed when the drain completes.
    std::optional<ss::future<>> _drain_future;
    std::optional<cluster_epoch> _safe_epoch;

    // Leader-only state.
    std::unique_ptr<node_source> _nodes;
    ss::sharded<::rpc::connection_cache>* _connections;
    std::unique_ptr<epoch_source> _epoch_source;
    std::unique_ptr<barrier_loop> _loop;
    ss::gate _gate;
};

} // namespace cloud_topics::l0::gc
