/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <cstdint>

namespace config {

/// One entry in `cloud_io_scheduler_min_share`: the target_reserved value
/// for a single group_id, keyed by name. Unknown group_name entries are
/// ignored by the scheduler factory (with a warning) so that the schema
/// survives forward/backward upgrades that add or remove groups.
struct min_share_group_target {
    ss::sstring group_name;
    uint32_t target_reserved{0};

    friend bool operator==(
      const min_share_group_target&, const min_share_group_target&) = default;

    fmt::iterator format_to(fmt::iterator it) const;
};

template<>
consteval std::string_view
detail::property_type_name<min_share_group_target>() {
    return "config::min_share_group_target";
}

} // namespace config

namespace YAML {
template<>
struct convert<config::min_share_group_target> {
    using type = config::min_share_group_target;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};
} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::min_share_group_target& v);

} // namespace json
