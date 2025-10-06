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

    co_return invalid_id;
}

} // namespace cluster::suffix_truncation
