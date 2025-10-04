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
#include "rpc/connection_cache.h"

namespace {
inline constexpr ss::shard_id suffix_truncation_shard = 0;
}

namespace cluster::suffix_truncation {
frontend::frontend(
  model::node_id self,
  ss::sharded<controller_stm>& controller,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _controller(&controller)
  , _connections(&connections)
  , _leaders_table(&leaders)
  , _as(as) {}

} // namespace cluster::suffix_truncation
