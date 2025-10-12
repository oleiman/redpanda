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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "cluster/errc.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/vector.h"

#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/format.h>

namespace cluster::suffix_truncation {

using id = named_type<int64_t, struct suffix_truncation_id_tag>;
inline constexpr id invalid_id{-1};

inline constexpr ss::shard_id suffix_truncation_shard = 0;

enum class state : uint8_t {
    init,
    preparing,
    truncating,
    finishing,
    done,
};

using topic_blocked = ss::bool_class<struct topic_blocked_tag>;

// TODO: partition state stored in backend

enum class topic_state : uint8_t {
    non_restricted,
    read_only,
};

struct partition_truncation
  : serde::envelope<
      partition_truncation,
      serde::version<0>,
      serde::compat_version<0>> {
    model::partition_id pid;
    kafka::offset offset;

    auto serde_fields() { return std::tie(pid, offset); }
    friend bool
    operator==(const partition_truncation&, const partition_truncation&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct topic_truncation
  : serde::
      envelope<topic_truncation, serde::version<0>, serde::compat_version<0>> {
    model::topic_namespace nt;
    chunked_vector<partition_truncation> partitions;

    bool empty() const { return partitions.empty(); }

    topic_truncation copy() const;

    auto serde_fields() { return std::tie(nt, partitions); }
    friend bool operator==(const topic_truncation&, const topic_truncation&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct suffix_truncation
  : serde::
      envelope<suffix_truncation, serde::version<0>, serde::compat_version<0>> {
    chunked_vector<topic_truncation> topics;

    bool empty() const {
        return topics.empty()
               || std::ranges::all_of(
                 topics, [](const topic_truncation& t) { return t.empty(); });
    }

    auto serde_fields() { return std::tie(topics); }
    friend bool operator==(const suffix_truncation&, const suffix_truncation&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct truncation_request
  : serde::envelope<
      truncation_request,
      serde::version<0>,
      serde::compat_version<0>> {
    suffix_truncation truncation;

    auto serde_fields() { return std::tie(truncation); }
    friend bool operator==(const truncation_request&, const truncation_request&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct truncation_reply
  : serde::
      envelope<truncation_reply, serde::version<0>, serde::compat_version<0>> {
    id id{invalid_id};
    cluster::errc ec;

    auto serde_fields() { return std::tie(id, ec); }
    friend bool operator==(const truncation_reply&, const truncation_reply&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct truncate_cmd_data
  : serde::
      envelope<truncate_cmd_data, serde::version<0>, serde::compat_version<0>> {
    id id;
    suffix_truncation truncation;
    model::timestamp op_timestamp{};

    auto serde_fields() { return std::tie(id, truncation, op_timestamp); }
    friend bool operator==(const truncate_cmd_data&, const truncate_cmd_data&)
      = default;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct update_cmd_data
  : serde::
      envelope<update_cmd_data, serde::version<0>, serde::compat_version<0>> {
    id id;
    state desired_state;
    model::timestamp op_timestamp{};

    auto serde_fields() { return std::tie(id, desired_state, op_timestamp); }
    friend bool operator==(const update_cmd_data&, const update_cmd_data&)
      = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct truncation_meta
  : serde::
      envelope<truncation_meta, serde::version<0>, serde::compat_version<0>> {
    id id;
    suffix_truncation truncation;

    state state{state::init};
    // populated on creation
    model::timestamp created{};
    // populated once finished or cancelled state is reached
    model::timestamp completed{};

    auto serde_fields() {
        return std::tie(id, truncation, state, created, completed);
    }

    auto topics() const {
        return truncation.topics
               | std::views::transform([](const topic_truncation& tt) {
                     return model::topic_namespace_view{tt.nt};
                 });
    }

    friend bool operator==(const truncation_meta&, const truncation_meta&)
      = default;

    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace cluster::suffix_truncation

template<>
struct fmt::formatter<cluster::suffix_truncation::state>
  : fmt::formatter<std::string_view> {
    auto format(
      const cluster::suffix_truncation::state&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
