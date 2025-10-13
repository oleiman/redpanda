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
#include "utils/named_type.h"
#include "utils/notification_list.h"

#include <seastar/util/noncopyable_function.hh>

namespace cluster::suffix_truncation {

class table {
public:
    using notification_id = named_type<int64_t, struct notification_id_tag>;
    using notification_cb = ss::noncopyable_function<void(id)>;
    static constexpr auto commands = make_commands_list<
      suffix_truncation_truncate_cmd,
      suffix_truncation_update_cmd>();

    explicit table(ss::sharded<topic_table>&, ss::sharded<tracker>&);

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

    static bool is_valid_state_transition(state current, state target);

    ss::future<> stop();

    notification_id register_cb(notification_cb cb) {
        return _callbacks.register_cb(std::move(cb));
    }

    void unregister_cb(notification_id id) { _callbacks.unregister_cb(id); }

private:
    ss::future<std::error_code> apply(suffix_truncation_truncate_cmd cmd);
    ss::future<std::error_code> apply(suffix_truncation_update_cmd cmd);

    ss::sharded<topic_table>* _topics;
    ss::sharded<tracker>* _tracker;
    notification_list<notification_cb, notification_id> _callbacks;

    id _next_id{0};
    id _last_applied{invalid_id};

    absl::node_hash_map<id, truncation_meta> _truncations;
};

} // namespace cluster::suffix_truncation
