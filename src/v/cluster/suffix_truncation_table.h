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

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/suffix_truncation_types.h"

namespace cluster::suffix_truncation {

class table {
public:
    static constexpr auto commands
      = make_commands_list<suffix_truncation_truncate_cmd>();

    explicit table(ss::sharded<topic_table>& topics);

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type
               == model::record_batch_type::suffix_truncation_cmd;
    }
    // an entry point for controller stm, receives a record batch if is
    // applicable to data migration state machine.
    ss::future<std::error_code> apply_update(model::record_batch);

    /**
     * Fills the snapshot with data from the migration table when requested to
     * do so by the controller state machine
     */
    ss::future<> fill_snapshot(controller_snapshot&) const { return ss::now(); }
    /**
     * Called withe controller snapshot when the controller state machine is
     * replied.
     */
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&) {
        return ss::now();
    }
    /**
     * Returns a single migration with requested id or empty optional if no
     * migration is found.
     */

    id get_next_id() { return _next_id++; }

    std::expected<void, errc> validate(const suffix_truncation&);

    ss::future<> stop();

private:
    ss::future<std::error_code> apply(suffix_truncation_truncate_cmd cmd);

    ss::sharded<topic_table>* _topics;

    id _next_id{0};
    id _last_applied{invalid_id};
};

} // namespace cluster::suffix_truncation
