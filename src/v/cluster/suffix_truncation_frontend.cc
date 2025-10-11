/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/suffix_truncation_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_stm.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/suffix_truncation_types.h"
#include "rpc/connection_cache.h"

namespace cluster::suffix_truncation {
frontend::frontend(
  model::node_id self,
  ssx::single_sharded<table>& table,
  ss::sharded<controller_stm>& controller,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _table(&table)
  , _controller(&controller)
  , _connections(&connections)
  , _leaders_table(&leaders.local())
  , _as(as) {}

ss::future<std::expected<id, std::error_code>>
frontend::truncate(suffix_truncation trunc) {
    vlog(st_log.debug, "truncating: {}", trunc);

    truncation_request req{.truncation = std::move(trunc)};

    // NOTE(oren): just assume local for now

    return container().invoke_on(
      suffix_truncation_shard, [req = std::move(req)](frontend& local) mutable {
          return local.do_local_truncate(std::move(req.truncation));
      });
}

ss::future<std::expected<id, std::error_code>>
frontend::do_local_truncate(suffix_truncation trunc) {
    auto controller_leader = _leaders_table->get_leader(model::controller_ntp);

    if (!controller_leader.has_value()) {
        vlog(
          st_log.warn,
          "unable to process truncation {}: no controller present",
          trunc);
        co_return std::unexpected(errc::no_leader_controller);
    }

    vassert(
      controller_leader.value() == _self,
      "Truncate dispatched to non-leader: TODO: support rpc dispatch");

    vassert(
      ss::this_shard_id() == suffix_truncation_shard,
      "This method can only be called on the suffix truncation shard");

    if (auto ec = co_await insert_barrier(); ec != errc::success) {
        co_return std::unexpected(ec);
    }

    if (trunc.empty()) {
        co_return std::unexpected(errc::suffix_truncation_invalid);
    }

    if (auto v_err = _table->local().validate(trunc); !v_err) {
        vlog(
          st_log.warn,
          "suffix_truncation {} validation error - {}",
          trunc,
          v_err.error());
        co_return std::unexpected(v_err.error());
    }

    auto id = _table->local().get_next_id();

    if (auto ec = co_await replicate_and_wait(
          *_controller,
          _as,
          suffix_truncation_truncate_cmd(
            0, /* ignored */
            truncate_cmd_data{
              .id = id,
              .truncation = std::move(trunc),
              .op_timestamp = model::timestamp::now(),
            }),
          _operation_timeout + model::timeout_clock::now());
        ec != errc::success) {
        co_return std::unexpected(ec);
    }

    co_return id;
}

ss::future<std::error_code> frontend::insert_barrier() {
    const auto barrier_deadline = _operation_timeout
                                  + model::timeout_clock::now();
    /**
     * Inject linearizable barrier before creating a new migration. This is not
     * required for correctness but allows the fronted to do more accurate
     * preliminary validation.
     */
    static_assert(controller_stm_shard == suffix_truncation_shard);
    auto barrier_result
      = co_await _controller->local().insert_linearizable_barrier(
        _operation_timeout + model::timeout_clock::now());
    if (!barrier_result) {
        co_return barrier_result.error();
    }
    auto [barrier_offset, _] = barrier_result.value();
    try {
        co_await _controller->local().wait(barrier_offset, barrier_deadline);
    } catch (...) {
        co_return errc::timeout;
    }
    co_return errc::success;
}

} // namespace cluster::suffix_truncation
