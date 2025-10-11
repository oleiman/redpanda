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

#include "cluster/suffix_truncation_table.h"

#include "cluster/logger.h"
#include "cluster/topic_table.h"

namespace cluster::suffix_truncation {
table::table(ss::sharded<topic_table>& topics)
  : _topics(&topics) {}

ss::future<std::error_code> table::apply_update(model::record_batch batch) {
    auto cmd = co_await deserialize(std::move(batch), commands);

    co_return co_await std::visit(
      [this](auto cmd) { return apply(std::move(cmd)); }, std::move(cmd));
}

ss::future<std::error_code> table::apply(suffix_truncation_truncate_cmd cmd) {
    auto truncation = std::move(cmd.value.truncation);
    auto id = cmd.value.id;
    auto create_ts = cmd.value.op_timestamp;

    vlog(st_log.debug, "applying create data migration: {}", cmd.value);

    if (id <= _last_applied) {
        co_return errc::suffix_truncation_already_exists;
    }

    auto err = validate(truncation);
    if (!err.has_value()) {
        // TODO: error detail
        co_return err.error();
    }

    auto [it, success] = _truncations.try_emplace(
      id,
      truncation_meta{
        .id = id,
        .truncation = std::move(truncation),
        .created = create_ts,
      });

    if (!success) {
        co_return errc::suffix_truncation_already_exists;
    }
    _last_applied = id;

    // TODO: apply update to node-wide data structure
    // TODO: notify backend

    co_return errc::success;
}

std::expected<void, errc> table::validate(const suffix_truncation& trunc) {
    if (trunc.empty()) {
        return std::unexpected(errc::suffix_truncation_invalid);
    }

    for (const auto& t : trunc.topics) {
        if (t.nt.ns != model::kafka_namespace) {
            vlog(
              st_log.warn, "{}: topic is not in the default namespace", t.nt);
            return std::unexpected(errc::suffix_truncation_invalid);
        }
        auto maybe_topic_cfg = _topics->local().get_topic_cfg(t.nt);
        if (!maybe_topic_cfg.has_value()) {
            vlog(st_log.warn, "{}: topic does not exist", t.nt);
            return std::unexpected(errc::topic_not_exists);
        }
        // TODO: should we figure out remote / local right here? i guess that
        // could change while we replicate the command.

        // TODO: Need to implement resources data structure and check whether
        // these truncations are already in it or whatever
    }
    return {};
}

ss::future<> table::stop() { return ss::now(); }

} // namespace cluster::suffix_truncation
