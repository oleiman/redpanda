/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/convert.h"
#include "model/fundamental.h"
#include "ssx/sformat.h"

#include <yaml-cpp/yaml.h>

#include <iostream>

namespace config {
namespace detail {

template<typename V>
requires std::is_same_v<V, model::node_uuid>
         || std::is_same_v<V, model::node_id>
struct id_override_impl {
    using value_type = V;
    id_override_impl() = default;
    id_override_impl(model::node_uuid key, value_type value)
      : key(key)
      , value(value) {}
    model::node_uuid key;
    value_type value;

private:
    friend std::ostream&
    operator<<(std::ostream& os, const id_override_impl<V>& v) {
        return os << ssx::sformat("{{ {}:{} }}", v.key, v.value);
    }

    friend std::istream& operator>>(std::istream& is, id_override_impl<V>& v) {
        std::string s;
        is >> s;
        char delim = ':';
        auto delim_i = s.find_first_of(delim);
        if (auto j = s.find_last_of(delim); delim_i == std::string::npos
                                            || delim_i + 1 > s.size()
                                            || delim_i != j) {
            throw std::runtime_error("Format: '<current UUID>:<UUID|ID>'");
        }

        auto first = std::string_view{s}.substr(0, delim_i);
        auto second = std::string_view{s}.substr(delim_i + 1);

        v.key = boost::lexical_cast<model::node_uuid>(first);
        v.value = boost::lexical_cast<value_type>(second);
        return is;
    }

    friend bool operator==(const id_override_impl&, const id_override_impl&)
      = default;
};

template<typename T>
struct yaml_converter {
    using type = T;
    using value_type = T::value_type;
    static inline YAML::Node encode(const type& rhs) {
        YAML::Node node;
        node["current_uuid"] = ssx::sformat("{}", rhs.key);
        node[val_key.data()] = ssx::sformat("{}", rhs.value);
        return node;
    }

    static inline bool decode(const YAML::Node& node, type& rhs) {
        if (!node["current_uuid"] || !node[val_key.data()]) {
            return false;
        }
        rhs.key = node["current_uuid"].as<model::node_uuid>();
        rhs.value = node[val_key.data()].template as<value_type>();
        return true;
    }

    static constexpr auto val_key = []() -> std::string_view {
        if constexpr (std::is_same_v<value_type, model::node_uuid>) {
            return "new_uuid";
        } else if constexpr (std::is_same_v<value_type, model::node_id>) {
            return "new_id";
        }
    }();
};

} // namespace detail

using uuid_override = detail::id_override_impl<model::node_uuid>;
using id_override = detail::id_override_impl<model::node_id>;

struct node_override_store {
    node_override_store() = default;
    void maybe_set_overrides(
      model::node_uuid node_uuid,
      const std::vector<uuid_override>& uuid_overrides,
      const std::vector<id_override>& id_overrides) {
        vassert(ss::this_shard_id() == 0, "Only set overrides on shard 0");
        for (const auto& u : uuid_overrides) {
            if (_uuid_override.has_value()) {
                break;
            } else if (u.key == node_uuid) {
                _uuid_override.emplace(u.value);
            }
        }

        for (const auto& u : id_overrides) {
            if (_id_override.has_value()) {
                break;
            } else if (u.key == node_uuid) {
                _id_override.emplace(u.value);
            }
        }
    }

    std::optional<model::node_uuid> node_uuid() const {
        vassert(ss::this_shard_id() == 0, "Only get overrides on shard 0");
        return _uuid_override;
    }

    std::optional<model::node_id> node_id() const {
        vassert(ss::this_shard_id() == 0, "Only get overrides on shard 0");
        return _id_override;
    }

private:
    std::optional<model::node_uuid> _uuid_override;
    std::optional<model::node_id> _id_override;
};

} // namespace config

namespace YAML {

template<>
struct convert<config::uuid_override>
  : config::detail::yaml_converter<config::uuid_override> {};

template<>
struct convert<config::id_override>
  : config::detail::yaml_converter<config::id_override> {};
} // namespace YAML
