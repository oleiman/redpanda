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
#include "cluster/suffix_truncation_tracker.h"
#include "cluster/topic_table.h"

#include <utility>

namespace cluster::suffix_truncation {

table::table(ss::sharded<topic_table>& topics, ss::sharded<tracker>& tracker)
  : _topics(&topics)
  , _tracker(&tracker) {}

ss::future<std::error_code> table::apply_update(model::record_batch batch) {
    auto cmd = co_await deserialize(std::move(batch), commands);

    co_return co_await std::visit(
      [this](auto cmd) { return apply(std::move(cmd)); }, std::move(cmd));
}

ss::future<std::error_code> table::apply(suffix_truncation_truncate_cmd cmd) {
    // TODO(oren): remove (maybe)
    vassert(
      ss::this_shard_id() == suffix_truncation_shard,
      "Should only run on shard {}",
      suffix_truncation_shard);
    vlog(st_log.debug, "applying create truncation: {}", cmd.value);

    auto truncation = std::move(cmd.value.truncation);
    auto t_id = cmd.value.id;
    auto create_ts = cmd.value.op_timestamp;

    if (t_id <= _last_applied) {
        vlog(st_log.warn, "Truncation already exists: {}", t_id);
        co_return errc::suffix_truncation_already_exists;
    }

    auto err = validate(truncation);
    if (!err.has_value()) {
        // TODO: error detail
        vlog(st_log.warn, "Invalid truncation: {}", err.error());
        co_return err.error();
    }

    auto [it, success] = _truncations.try_emplace(
      t_id,
      truncation_meta{
        .id = t_id,
        .truncation = std::move(truncation),
        .created = create_ts,
      });

    if (!success) {
        vlog(st_log.warn, "Truncation already exists: {}", t_id);
        co_return errc::suffix_truncation_already_exists;
    }
    _last_applied = t_id;
    _next_id = std::max(_next_id, _last_applied + id{1});

    co_await _tracker->invoke_on_all(
      [&meta = it->second](tracker& t) { t.apply_update(meta); });


    _callbacks.notify(t_id);
    co_return errc::success;
}

std::optional<truncation_meta> table::get_truncation(id id) const {
    return get_truncation_ref(id).transform(
      [](auto ref) -> truncation_meta { return ref.get().copy(); });
}

std::optional<state> table::get_truncation_state(id id) const {
    return get_truncation_ref(id).transform(
      [](auto ref) { return ref.get().state; });
}

std::optional<std::reference_wrapper<const truncation_meta>>
table::get_truncation_ref(id id) const {
    if (auto it = _truncations.find(id); it != _truncations.end()) {
        return std::cref(it->second);
    }
    return std::nullopt;
}

ss::future<std::error_code> table::apply(suffix_truncation_update_cmd cmd) {
    const auto [t_id, desired_state, op_ts] = cmd.value;

    vlog(st_log.debug, "update truncaiton state {}", cmd.value);
    auto it = _truncations.find(t_id);
    if (it == _truncations.end()) {
        vlog(st_log.warn, "Not found: {}", t_id);
        co_return errc::suffix_truncation_not_exists;
    }

    auto& current_state = it->second.state;

    if (!is_valid_state_transition(current_state, desired_state)) {
        vlog(
          st_log.info,
          "Invalid state transition {} -> {}",
          current_state,
          desired_state);
        // TODO(oren): want another error code
        co_return errc::suffix_truncation_invalid;
    }
    current_state = desired_state;

    if (current_state == state::done) {
        it->second.completed = op_ts;
    }

    co_await _tracker->invoke_on_all(
      [&meta = it->second](tracker& t) { t.apply_update(meta); });

    _callbacks.notify(t_id);

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

bool table::is_valid_state_transition(state current, state target) {
    // NOTE(oren): probably don't even need specific transitions since we'll
    // just step through the stages. don't think we need any conept of
    // cancelling or whatever
    switch (current) {
        using enum state;
    case init:
        return target == preparing;
    case preparing:
        return target == truncating;
    case truncating:
        return target == finishing;
    case finishing:
        return target == done;
    case done:
        return false;
    }
    std::unreachable();
}

ss::future<> table::stop() { return ss::now(); }

} // namespace cluster::suffix_truncation
