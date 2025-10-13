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

#include "cloud_storage/fwd.h"
#include "cluster/fwd.h"
#include "cluster/suffix_truncation_table.h"
#include "cluster/suffix_truncation_types.h"
#include "model/fundamental.h"

#include <functional>
#include <optional>

namespace cluster::suffix_truncation {

class backend {
public:
    backend(
      model::node_id,
      table& table,
      frontend& frontend,
      partition_leaders_table& leaders_table,
      std::optional<std::reference_wrapper<cloud_storage::remote>>
        cloud_storage_api,
      ss::abort_source& as);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> handle_truncation(id id) noexcept;
    [[maybe_unused]] model::node_id _self;
    [[maybe_unused]] table* _table;
    [[maybe_unused]] frontend* _frontend;
    [[maybe_unused]] partition_leaders_table* _leaders_table;
    [[maybe_unused]] std::optional<cloud_storage::remote*> _cloud_storage_api{};
    [[maybe_unused]] ss::abort_source& _as;

    ss::gate _gate;

    table::notification_id _table_notification;
};

} // namespace cluster::suffix_truncation
