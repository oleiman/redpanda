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

#include "config/min_share_group_target.h"

#include "json/json.h"

#include <fmt/format.h>

namespace config {

fmt::iterator min_share_group_target::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group_name: {}, target_reserved: {}}}",
      group_name,
      target_reserved);
}

} // namespace config

namespace YAML {

Node convert<config::min_share_group_target>::encode(const type& rhs) {
    Node node;
    node["group_name"] = rhs.group_name;
    node["target_reserved"] = rhs.target_reserved;
    return node;
}

bool convert<config::min_share_group_target>::decode(
  const Node& node, type& rhs) {
    if (!node.IsMap()) {
        return false;
    }
    const auto name_node = node["group_name"];
    if (!name_node) {
        return false;
    }
    rhs.group_name = name_node.as<std::string>();
    if (const auto target_node = node["target_reserved"]; target_node) {
        rhs.target_reserved = target_node.as<uint32_t>();
    } else {
        rhs.target_reserved = 0;
    }
    return true;
}

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::min_share_group_target& v) {
    w.StartObject();
    w.Key("group_name");
    w.String(v.group_name);
    w.Key("target_reserved");
    w.Uint(v.target_reserved);
    w.EndObject();
}

} // namespace json
