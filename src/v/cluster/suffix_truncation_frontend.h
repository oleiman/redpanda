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

#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster::suffix_truncation {

class frontend : public ss::peering_sharded_service<frontend> {
public:
    explicit frontend(
      model::node_id,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<ss::abort_source>&);

private:
    model::node_id _self;
    ss::sharded<controller_stm>* _controller;
    ss::sharded<rpc::connection_cache>* _connections;
    ss::sharded<partition_leaders_table>* _leaders_table;
    ss::sharded<ss::abort_source>& _as;

    ss::gate _gate;
};

} // namespace cluster::suffix_truncation
