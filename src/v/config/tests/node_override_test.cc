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

#include "config/configuration.h"
#include "config/node_overrides.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/yaml.h>

YAML::Node
read_from_yaml(const ss::sstring& yaml_string, std::string_view key) {
    auto node = YAML::Load(yaml_string);
    return node[key.data()];
}

std::vector<config::uuid_override>
read_uuid_from_yaml(ss::sstring yaml_string) {
    return read_from_yaml(yaml_string, "node_uuid_overrides")
      .as<std::vector<config::uuid_override>>();
}

std::vector<config::id_override> read_id_from_yaml(ss::sstring yaml_string) {
    return read_from_yaml(yaml_string, "node_id_overrides")
      .as<std::vector<config::id_override>>();
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode_empty) {
    auto empty_uuid = "node_uuid_overrides: []\n";
    auto empty_uuid_cfg = read_uuid_from_yaml(empty_uuid);
    BOOST_CHECK(empty_uuid_cfg.empty());

    auto empty_id = "node_id_overrides: []\n";
    auto empty_id_cfg = read_id_from_yaml(empty_id);
    BOOST_CHECK(empty_id_cfg.empty());
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode) {
    static model::node_uuid node_uuid{uuid_t::create()};
    static model::node_uuid other_uuid{uuid_t::create()};
    static auto some_uuid = ssx::sformat(
      "node_uuid_overrides:\n"
      "  - current_uuid: {}\n"
      "    new_uuid: {}\n"
      "  - current_uuid: {}\n"
      "    new_uuid: {}\n",
      node_uuid,
      other_uuid,
      node_uuid,
      other_uuid);
    auto some_uuid_cfg = read_uuid_from_yaml(some_uuid);
    BOOST_CHECK_EQUAL(some_uuid_cfg.size(), 2);
    for (const auto& u : some_uuid_cfg) {
        BOOST_CHECK_EQUAL(u.key, node_uuid);
        BOOST_CHECK_EQUAL(u.value, other_uuid);
    }

    auto node_id = model::node_id{0};
    auto some_id = ssx::sformat(
      "node_id_overrides:\n"
      "  - current_uuid: {}\n"
      "    new_id: {}\n"
      "  - current_uuid: {}\n"
      "    new_id: {}\n",
      node_uuid,
      node_id,
      node_uuid,
      node_id);
    auto some_id_cfg = read_id_from_yaml(some_id);
    BOOST_CHECK_EQUAL(some_id_cfg.size(), 2);
    for (const auto& i : some_id_cfg) {
        BOOST_CHECK_EQUAL(i.key, node_uuid);
        BOOST_CHECK_EQUAL(i.value, node_id);
    }
}

SEASTAR_THREAD_TEST_CASE(test_overrides_decode_errors) {
    static constexpr std::string_view entry_fmt = "node_{}_overrides:\n"
                                                  "  - current_uuid: {}\n"
                                                  "    new_{}: {}\n";

    BOOST_CHECK_THROW(
      read_uuid_from_yaml(fmt::format(
        entry_fmt,
        "uuid",
        model::node_uuid{uuid_t::create()},
        "uuid",
        23 /* does not parse to uuid */)),
      YAML::TypedBadConversion<model::node_uuid>);

    BOOST_CHECK_THROW(
      read_uuid_from_yaml(fmt::format(
        entry_fmt,
        "uuid",
        model::node_uuid{uuid_t::create()},
        "id" /* 'value' field should be UUID */,
        23)),
      YAML::TypedBadConversion<config::uuid_override>);

    BOOST_CHECK_THROW(
      read_id_from_yaml(fmt::format(
        entry_fmt,
        "id",
        model::node_uuid{uuid_t::create()},
        "id",
        model::node_uuid{uuid_t::create()} /* does not parse to node ID */)),
      YAML::TypedBadConversion<model::node_id::type>);

    BOOST_CHECK_THROW(
      read_id_from_yaml(fmt::format(
        entry_fmt,
        "id",
        model::node_uuid{uuid_t::create()},
        "uuid" /* should be 'id' */,
        model::node_uuid{uuid_t::create()})),
      YAML::TypedBadConversion<config::id_override>);
}

SEASTAR_THREAD_TEST_CASE(test_overrides_store) {
    static model::node_uuid some_uuid{uuid_t::create()};
    static model::node_uuid other_uuid{uuid_t::create()};
    static constexpr model::node_id some_id{23};
    static constexpr model::node_id other_id{0};

    std::vector<config::uuid_override> uuid_vec{
      config::uuid_override{some_uuid, some_uuid},
      config::uuid_override{other_uuid, other_uuid},
    };
    std::vector<config::id_override> id_vec{
      config::id_override{some_uuid, some_id},
      config::id_override{other_uuid, other_id},
    };

    {
        config::node_override_store store;
        store.maybe_set_overrides(some_uuid, uuid_vec, id_vec);
        // TODO(oren): does boost do this for you? or is that gtest...
        BOOST_CHECK(store.node_uuid().has_value());
        BOOST_CHECK_EQUAL(store.node_uuid(), some_uuid);
        BOOST_CHECK_EQUAL(store.node_id(), some_id);
    }

    {
        config::node_override_store store;
        store.maybe_set_overrides(other_uuid, uuid_vec, id_vec);
        // TODO(oren): does boost do this for you? or is that gtest...
        BOOST_CHECK(store.node_uuid().has_value());
        BOOST_CHECK_EQUAL(store.node_uuid(), other_uuid);
        BOOST_CHECK_EQUAL(store.node_id(), other_id);
    }

    {
        config::node_override_store store;
        store.maybe_set_overrides(
          model::node_uuid{uuid_t::create()}, uuid_vec, id_vec);
        BOOST_CHECK(!store.node_uuid().has_value());
    }
}
