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

#include "cluster/suffix_truncation_types.h"

namespace cluster::suffix_truncation {
fmt::iterator partition_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{pid: {}, offset: {}}}", pid, offset);
}
topic_truncation topic_truncation::copy() const {
    return topic_truncation{.nt = nt, .partitions = partitions.copy()};
}
fmt::iterator topic_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{name: {}, partitions: {}}}", nt, partitions);
}

suffix_truncation suffix_truncation::copy() const {
    return {.topics = topics.copy()};
}
fmt::iterator suffix_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{topics_to_truncate: {}}}", topics);
}
fmt::iterator truncation_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}}}", truncation);
}
fmt::iterator truncation_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id: {}, ec: {}}}", id, ec);
}
fmt::iterator truncate_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, truncation: {}, op_ts: {}}}",
      id,
      truncation,
      op_timestamp);
}
fmt::iterator update_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, desired_state: {}, op_ts: {}}}",
      id,
      desired_state,
      op_timestamp);
}
truncation_meta truncation_meta::copy() const {
    return {
      .id = id,
      .truncation = truncation.copy(),
      .state = state,
      .created = created,
      .completed = completed,
    };
}
fmt::iterator truncation_meta::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, topic_truncation: {}, state: {}, created: {}, completed{}}}",
      id,
      truncation,
      state,
      created,
      completed);
}

} // namespace cluster::suffix_truncation

auto fmt::formatter<cluster::suffix_truncation::state>::format(
  const cluster::suffix_truncation::state& s, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    constexpr std::string_view base = "cluster::suffix_truncation::{}";
    switch (s) {
        using enum cluster::suffix_truncation::state;
    case init:
        return fmt::format_to(ctx.out(), base, "init");
    case preparing:
        return fmt::format_to(ctx.out(), base, "preparing");
    case truncating:
        return fmt::format_to(ctx.out(), base, "truncating");
    case finishing:
        return fmt::format_to(ctx.out(), base, "finishing");
    case done:
        return fmt::format_to(ctx.out(), base, "done");
    }
}
