/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/gc/epoch_source.h"
#include "cloud_topics/level_zero/gc/rpc_service.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/logger.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/members_table.h"
#include "cluster/partition_manager.h"
#include "model/namespace.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"
#include "ssx/when_all.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0::gc {

// ---------------------------------------------------------------------------
// Production implementations of partition_source and node_source,
// backed by cluster::partition_manager and cluster::members_table.
// ---------------------------------------------------------------------------

namespace {

class partition_manager_source : public epoch_barrier::partition_source {
public:
    explicit partition_manager_source(cluster::partition_manager& pm)
      : _pm(pm) {}

    ss::shared_ptr<cloud_topics::ctp_stm>
    get_ctp_stm(cluster::partition& p) const {
        return p.raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    }

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, partition] : _pm.partitions()) {
            if (ntp.ns != model::kafka_namespace) {
                continue;
            }
            auto ctp_stm = get_ctp_stm(*partition);
            if (!ctp_stm) {
                continue;
            }
            result.emplace_back(
              ntp,
              info{
                .term = partition->term(),
                .is_leader = partition->is_leader(),
                .has_epoch
                = ctp_stm->state().get_max_applied_epoch().has_value(),
                .last_epoch_log_offset
                = ctp_stm->state().get_last_epoch_log_offset(),
                .last_reconciled_log_offset
                = ctp_stm->state().get_last_reconciled_log_offset(),
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto p = _pm.get(ntp);
        if (!p) {
            return std::nullopt;
        }
        auto ctp_stm = get_ctp_stm(*p);
        if (!ctp_stm) {
            return std::nullopt;
        }
        return info{
          .term = p->term(),
          .is_leader = p->is_leader(),
          .has_epoch = ctp_stm->state().get_max_applied_epoch().has_value(),
          .last_epoch_log_offset
          = ctp_stm->state().get_last_epoch_log_offset(),
          .last_reconciled_log_offset
          = ctp_stm->state().get_last_reconciled_log_offset(),
        };
    }

private:
    cluster::partition_manager& _pm;
};

class members_table_node_source : public epoch_barrier::node_source {
public:
    members_table_node_source(
      model::node_id self, cluster::members_table& members)
      : _self(self)
      , _members(members) {}

    model::node_id self() const override { return _self; }
    std::vector<model::node_id> node_ids() const override {
        return _members.node_ids();
    }

private:
    model::node_id _self;
    cluster::members_table& _members;
};

} // namespace

// ---------------------------------------------------------------------------
// Leader-side background loop. Runs on shard 0 when this node holds L1
// metastore partition 0 leadership. Each iteration computes a candidate
// epoch, fans out advance_barrier to all nodes, and polls until
// convergence or max attempts.
// ---------------------------------------------------------------------------

using proto_t = rpc::impl::epoch_barrier_rpc_client_protocol;
static constexpr auto rpc_timeout = std::chrono::seconds(10);

class epoch_barrier::barrier_loop {
public:
    barrier_loop(epoch_barrier& parent, epoch_source& es)
      : _parent(parent)
      , _epoch_source(es) {
        ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
    }

    ss::future<> stop() noexcept {
        vlog(cd_log.debug, "Epoch barrier loop stopping...");
        _as.request_abort();
        if (!_gate.is_closed()) {
            co_await _gate.close();
        }
        vlog(cd_log.debug, "Epoch barrier loop stopped");
    }

private:
    static constexpr auto poll_interval = std::chrono::seconds(2);
    static constexpr auto loop_interval = std::chrono::seconds(5);
    static constexpr size_t max_poll_attempts = 30;

    ss::future<> run_loop() {
        while (!_as.abort_requested()) {
            auto res = co_await ss::coroutine::as_future(run_once());
            if (res.failed()) {
                auto ex = res.get_exception();
                auto log_lvl = ssx::is_shutdown_exception(ex)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(cd_log, log_lvl, "Epoch barrier round failed: {}", ex);
            }

            (co_await ss::coroutine::as_future(
               ss::sleep_abortable(loop_interval, _as)))
              .ignore_ready_future();
        }
    }

    ss::future<> run_once() {
        // Step 1: Get the candidate epoch from the epoch source.
        auto candidate_result
          = co_await _epoch_source.max_barrier_candidate_epoch(&_as);
        if (!candidate_result.has_value()) {
            vlog(
              cd_log.debug,
              "Epoch barrier: could not get candidate epoch: {}",
              candidate_result.error());
            co_return;
        }
        auto candidate_opt = candidate_result.value();
        if (!candidate_opt.has_value()) {
            vlog(
              cd_log.trace, "Epoch barrier: no candidate epoch available yet");
            co_return;
        }
        auto candidate = candidate_opt.value();

        vlog(
          cd_log.info,
          "Epoch barrier: starting round for candidate {}",
          candidate);

        // Step 2: Fan out advance_barrier to all nodes until all ready
        // or max attempts exhausted.
        for (size_t attempt = 0; attempt < max_poll_attempts; ++attempt) {
            if (_as.abort_requested()) {
                co_return;
            }

            auto res = co_await ss::coroutine::as_future(
              _parent.fan_out_advance_barrier(candidate));
            if (res.failed()) {
                auto ex = res.get_exception();
                if (ssx::is_shutdown_exception(ex)) {
                    co_return;
                }
                vlog(
                  cd_log.warn,
                  "Epoch barrier: fan-out failed for {}: {}",
                  candidate,
                  ex);
            } else if (res.get()) {
                co_await _parent.publish_safe_epoch(candidate);
                co_return;
            }

            co_await ss::sleep_abortable(poll_interval, _as);
        }

        vlog(
          cd_log.warn,
          "Epoch barrier: did not converge for candidate {} after {} attempts",
          candidate,
          max_poll_attempts);
    }

    epoch_barrier& _parent;
    epoch_source& _epoch_source;
    ss::gate _gate;
    ss::abort_source _as;
};

epoch_barrier::~epoch_barrier() = default;

std::unique_ptr<epoch_barrier::node_source>
epoch_barrier::make_default_node_source(
  model::node_id self, cluster::members_table& members) {
    return std::make_unique<members_table_node_source>(self, members);
}

// ---------------------------------------------------------------------------
// Lifecycle: constructor, factories, stop.
// ---------------------------------------------------------------------------

std::unique_ptr<epoch_barrier::partition_source>
epoch_barrier::make_default_partition_source(cluster::partition_manager& pm) {
    return std::make_unique<partition_manager_source>(pm);
}

epoch_barrier::epoch_barrier(
  ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>& epoch_service,
  data_plane_api& data_plane,
  std::unique_ptr<partition_source> partitions,
  std::unique_ptr<node_source> nodes,
  ss::sharded<::rpc::connection_cache>* connections,
  ss::sharded<cluster::health_monitor_frontend>* health_monitor,
  ss::sharded<cluster::controller_stm>* controller_stm,
  ss::sharded<cluster::topic_table>* topic_table)
  : _epoch_service(epoch_service)
  , _data_plane(data_plane)
  , _partitions(std::move(partitions))
  , _nodes(std::move(nodes))
  , _connections(connections)
  , _epoch_source(
      epoch_source::make_default(
        health_monitor, controller_stm, topic_table, [this] {
            return safe_epoch();
        })) {}

epoch_barrier::epoch_barrier(
  ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>& epoch_service,
  data_plane_api& data_plane,
  std::unique_ptr<partition_source> partitions,
  std::unique_ptr<node_source> nodes,
  ss::sharded<::rpc::connection_cache>* connections,
  std::unique_ptr<epoch_source> epoch_src)
  : _epoch_service(epoch_service)
  , _data_plane(data_plane)
  , _partitions(std::move(partitions))
  , _nodes(std::move(nodes))
  , _connections(connections)
  , _epoch_source(std::move(epoch_src)) {}

ss::future<> epoch_barrier::stop() {
    co_await _gate.close();
    if (_loop) {
        co_await _loop->stop();
    }
    if (_drain_future.has_value()) {
        (co_await ss::coroutine::as_future(std::move(*_drain_future)))
          .ignore_ready_future();
        _drain_future.reset();
    }
}

// ---------------------------------------------------------------------------
// Barrier protocol: handle_barrier (per-node RPC handler), safe epoch
// publication, and the advance_local/advance_remote/fan_out methods
// that the leader loop calls to drive the protocol.
// ---------------------------------------------------------------------------

ss::future<epoch_barrier::barrier_status> epoch_barrier::handle_barrier(
  cluster_epoch candidate, std::optional<cluster_epoch> safe_epoch) {
    vassert(
      ss::this_shard_id() == 0,
      "handle_barrier must run on shard 0, got shard {}",
      ss::this_shard_id());

    // 1. Idempotent safe_epoch propagation: ratchet forward via max.
    if (safe_epoch.has_value()) {
        co_await publish_safe_epoch(*safe_epoch);
    }

    // 2. If no round in progress for this candidate, invalidate the epoch
    //    cache and kick off a drain. The drain runs asynchronously — we
    //    return pending immediately so the RPC resolves quickly.
    if (!_round || _round->candidate != candidate) {
        vlog(
          cd_log.debug,
          "Epoch barrier: starting round for candidate {}",
          candidate);

        // Abandon any in-progress drain from a previous round. The old
        // drain's writes use older epochs (the cache was invalidated),
        // so they don't affect the new round's safety. The gate ensures
        // the future is awaited on shutdown.
        if (_drain_future.has_value()) {
            auto df
              = std::move(std::exchange(_drain_future, std::nullopt)).value();
            if (_gate.is_closed()) {
                (co_await ss::coroutine::as_future(std::move(df)))
                  .ignore_ready_future();
                co_return barrier_status::pending;
            }
            ssx::spawn_with_gate(_gate, [fut = std::move(df)]() mutable {
                return std::move(fut).discard_result();
            });
        }

        // Reset round state on all shards.
        co_await container().invoke_on_all(
          [](epoch_barrier& b) { b._round.reset(); });

        // Ensure the cluster epoch is past our candidate so new writes
        // get epoch > candidate. force_epoch_update only bumps raft0 if
        // the cached epoch is behind. invalidate_epoch_cache forces stale
        // followers to re-fetch a value > candidate.
        co_await _epoch_service.local().force_epoch_update(candidate());
        co_await _epoch_service.local().invalidate_epoch_cache(candidate());

        // Kick off the drain but don't block the RPC on it. The drain
        // future is checked on subsequent polls.
        _drain_future = _data_plane.drain_inflight_writes();
        _round = round_state{.candidate = candidate};

        co_return barrier_status::pending;
    }

    // 3. If draining, check whether the drain has completed.
    if (_drain_future.has_value()) {
        if (!_drain_future->available()) {
            co_return barrier_status::pending;
        }

        // Drain finished — consume the future and check for errors.
        auto fut = std::exchange(_drain_future, std::nullopt);
        auto res = co_await ss::coroutine::as_future(std::move(*fut));
        if (res.failed()) {
            auto ex = res.get_exception();
            vlog(
              cd_log.warn,
              "Epoch barrier: drain failed for {}: {}",
              candidate,
              ex);
            // Reset so the next poll restarts the round.
            co_await container().invoke_on_all(
              [](epoch_barrier& b) { b._round.reset(); });
            co_return barrier_status::pending;
        }

        // Drain succeeded — collect seal points on ALL shards. If any
        // shard has a leader partition with an epoch but no placeholder
        // offset, the round stalls until the housekeeper catches up.
        auto seals_ok = co_await container().map_reduce0(
          [candidate](epoch_barrier& b) {
              return b.collect_local_seal_points(candidate);
          },
          true,
          std::logical_and<>{});

        if (!seals_ok) {
            vlog(
              cd_log.debug,
              "Epoch barrier: seal collection incomplete for {}, "
              "waiting for placeholder offsets",
              candidate);
            co_await container().invoke_on_all(
              [](epoch_barrier& b) { b._round.reset(); });
            co_return barrier_status::pending;
        }

        vlog(
          cd_log.debug,
          "Epoch barrier: drain complete, checking reconciliation for {}",
          candidate);
    }

    // 4. Check reconciliation progress on ALL shards.
    auto check = co_await container().map_reduce0(
      [](epoch_barrier& b) { return b.check_local_seal_points(); },
      seal_check_result::reconciled,
      [](seal_check_result a, seal_check_result b) {
          if (a == seal_check_result::stale || b == seal_check_result::stale) {
              return seal_check_result::stale;
          }
          if (
            a == seal_check_result::pending
            || b == seal_check_result::pending) {
              return seal_check_result::pending;
          }
          return seal_check_result::reconciled;
      });

    if (check == seal_check_result::stale) {
        vlog(
          cd_log.debug,
          "Epoch barrier: seal table stale for {}, will redrain",
          candidate);
        co_await container().invoke_on_all(
          [](epoch_barrier& b) { b._round.reset(); });
        co_return barrier_status::pending;
    }

    if (check == seal_check_result::pending) {
        vlog(
          cd_log.debug,
          "Epoch barrier: reconciliation not yet complete for {}",
          candidate);
        co_return barrier_status::pending;
    }

    co_return barrier_status::ready;
}

ss::future<> epoch_barrier::publish_safe_epoch(cluster_epoch candidate) {
    vlog(
      cd_log.info, "Epoch barrier: publishing safe-to-GC epoch {}", candidate);
    co_await container().invoke_on_all([candidate](epoch_barrier& b) {
        if (!b._safe_epoch || *b._safe_epoch < candidate) {
            b._safe_epoch = candidate;
        }
    });
}

ss::future<bool> epoch_barrier::advance_local(
  cluster_epoch candidate, std::optional<cluster_epoch> safe) {
    // Dispatch to shard 0 for the same reason as the RPC path:
    // handle_barrier's round state must live on a single shard.
    auto result_f = co_await ss::coroutine::as_future(
      container().invoke_on(0, [candidate, safe](epoch_barrier& b) {
          return b.handle_barrier(candidate, safe);
      }));
    if (result_f.failed()) {
        auto ex = result_f.get_exception();
        vlog(
          cd_log.warn,
          "handle_barrier({}, {}) failed: {}",
          candidate,
          safe,
          ex);
        co_return false;
    }
    co_return result_f.get() == barrier_status::ready;
}

ss::future<bool> epoch_barrier::advance_remote(
  model::node_id node_id,
  cluster_epoch candidate,
  std::optional<cluster_epoch> safe) {
    auto timeout = model::timeout_clock::now() + rpc_timeout;
    rpc::barrier_request req{.candidate = candidate, .safe_epoch = safe};
    auto res = co_await _connections->local()
                 .with_node_client<proto_t>(
                   _nodes->self(),
                   ss::this_shard_id(),
                   node_id,
                   rpc_timeout,
                   [req, timeout](proto_t client) mutable {
                       return client.advance_barrier(
                         std::move(req), ::rpc::client_opts{timeout});
                   })
                 .then(&::rpc::get_ctx_data<rpc::barrier_response>);
    if (res.has_error()) {
        vlog(
          cd_log.warn,
          "Epoch barrier: advance_barrier RPC to node {} failed: {}",
          node_id,
          res.error().message());
        co_return false;
    }
    co_return res.value().s == rpc::barrier_response::status::ready;
}

ss::future<bool>
epoch_barrier::fan_out_advance_barrier(cluster_epoch candidate) {
    auto nodes = _nodes->node_ids();
    auto self = _nodes->self();
    auto safe = _safe_epoch;

    auto results = co_await ssx::when_all_succeed<chunked_vector<uint8_t>>(
      std::views::transform(
        nodes,
        [this, self, candidate, safe](auto node_id) {
            return node_id == self ? advance_local(candidate, safe)
                                   : advance_remote(node_id, candidate, safe);
        })
      | std::ranges::to<chunked_vector<ss::future<bool>>>());

    co_return std::ranges::all_of(results, [](uint8_t r) { return r != 0; });
}

// ---------------------------------------------------------------------------
// Seal points: per-shard tracking of committed offsets at drain time.
// After draining, each leader partition's committed_offset and term are
// recorded. The reconciliation check polls until LRO reaches each seal.
// Seal points fix the reconciliation target so it doesn't chase the
// ever-advancing committed_offset from new writes.
// ---------------------------------------------------------------------------

epoch_barrier::seal_point*
epoch_barrier::find_seal(const model::topic& topic, model::partition_id pid) {
    auto topic_it = _round->seals.find(topic);
    if (topic_it == _round->seals.end()) {
        return nullptr;
    }
    auto pid_it = topic_it->second.find(pid);
    if (pid_it == topic_it->second.end()) {
        return nullptr;
    }
    return &pid_it->second;
}

void epoch_barrier::upsert_seal(
  const model::topic& topic, model::partition_id pid, seal_point sp) {
    _round->seals[topic][pid] = sp;
}

// Called on each shard after drain completes. Records the last epoch
// log offset and term for every leader partition. The seal targets
// the last epoch-bearing batch (placeholder or advance_epoch), which
// LRLO can reach via reconciliation or sync_to_next_placeholder.
//
// Returns false if any leader partition has an epoch but no epoch log
// offset — the round must stall until the housekeeper advances the
// epoch or new data arrives.
bool epoch_barrier::collect_local_seal_points(cluster_epoch candidate) {
    _round = round_state{.candidate = candidate};
    for (const auto& [ntp, pinfo] : _partitions->cloud_topic_partitions()) {
        if (!pinfo.is_leader) {
            continue;
        }
        // Partition has seen an epoch but has no placeholder offset.
        // This happens after restart (in-memory field not yet populated)
        // or on idle partitions before the housekeeper runs advance_epoch.
        // We can't seal it, so we stall.
        if (pinfo.has_epoch && !pinfo.last_epoch_log_offset) {
            return false;
        }
        if (!pinfo.last_epoch_log_offset) {
            continue;
        }
        upsert_seal(
          ntp.tp.topic,
          ntp.tp.partition,
          seal_point{
            .committed = *pinfo.last_epoch_log_offset, .term = pinfo.term});
    }
    return true;
}

// We need a consistent snapshot: every sealed partition still leader at
// the same term, with LRO past its seal. Any inconsistency means another
// node may have written epoch-C data between our seal and now (the
// barrier fans out sequentially, so remote nodes can still accept epoch-C
// writes until they're called). The term-push-forward absorbs that data
// by extending the seal to the current committed offset.
epoch_barrier::seal_check_result epoch_barrier::check_local_seal_points() {
    if (!_round) {
        return seal_check_result::reconciled;
    }

    auto result = seal_check_result::reconciled;

    for (auto& [topic, partitions] : _round->seals) {
        for (auto& [pid, seal] : partitions) {
            auto ntp = model::ntp(model::kafka_namespace, topic, pid);
            auto pinfo = _partitions->get(ntp);

            // Step 1: partition gone or lost leadership → stale. Must
            // redrain because we can't verify reconciliation here.
            if (!pinfo || !pinfo->is_leader) {
                return seal_check_result::stale;
            }

            // Step 2: term changed but still leader → push seal forward.
            // Another node may have committed epoch-C data at offsets
            // above our seal before it was invalidated. The current
            // last placeholder offset includes that data, so we extend
            // the target. This can only move the seal forward.
            if (pinfo->term != seal.term) {
                if (pinfo->last_epoch_log_offset) {
                    seal.committed = *pinfo->last_epoch_log_offset;
                }
                seal.term = pinfo->term;
                result = seal_check_result::pending;
                continue;
            }

            // Step 3: stable term → check reconciliation. LRO past the
            // seal means all data up to the seal (including all epoch-C
            // data) has been reconciled to L1.
            auto lrlo = pinfo->last_reconciled_log_offset;
            if (!lrlo || *lrlo < seal.committed) {
                result = seal_check_result::pending;
            }
        }
    }

    // Step 4: new leaders discovered since seal collection. A partition
    // that gained leadership after we sealed needs its own seal point —
    // it may have epoch-C data we haven't accounted for.
    for (const auto& [ntp, pinfo] : _partitions->cloud_topic_partitions()) {
        if (!pinfo.is_leader) {
            continue;
        }
        if (find_seal(ntp.tp.topic, ntp.tp.partition)) {
            continue;
        }
        if (pinfo.has_epoch && !pinfo.last_epoch_log_offset) {
            return seal_check_result::stale;
        }
        if (!pinfo.last_epoch_log_offset) {
            continue;
        }
        upsert_seal(
          ntp.tp.topic,
          ntp.tp.partition,
          seal_point{
            .committed = *pinfo.last_epoch_log_offset, .term = pinfo.term});
        result = seal_check_result::pending;
    }

    return result;
}

void epoch_barrier::notify_leadership_change(bool is_leader) noexcept {
    ssx::spawn_with_gate(
      _gate, [this, is_leader] { return set_leader(is_leader); });
}

// ---------------------------------------------------------------------------
// Leadership transitions. Starts or stops the barrier_loop when L1
// metastore partition 0 leadership changes on this node.
// ---------------------------------------------------------------------------

ss::future<> epoch_barrier::set_leader(bool is_leader) {
    if (!is_leader) {
        if (_loop) {
            auto loop = std::exchange(_loop, nullptr);
            co_await loop->stop();
        }
        co_return;
    }

    // Already running.
    if (_loop) {
        co_return;
    }

    _loop = std::make_unique<barrier_loop>(*this, *_epoch_source);
}

} // namespace cloud_topics::l0::gc
