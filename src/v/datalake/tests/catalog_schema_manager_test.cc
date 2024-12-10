/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "datalake/catalog_schema_manager.h"
#include "gtest/gtest.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/table_identifier.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {
const auto table_ident = table_identifier{.ns = {"redpanda"}, .table = "foo"};
} // namespace

class CatalogSchemaManagerTestBase : public s3_imposter_fixture {
public:
    static constexpr std::string_view base_location{"test"};
    CatalogSchemaManagerTestBase()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location))
      , schema_mgr(catalog) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    void reset_field_ids(struct_type& type) {
        chunked_vector<nested_field*> to_visit;
        for (auto& f : std::ranges::reverse_view(type.fields)) {
            to_visit.emplace_back(f.get());
        }
        while (!to_visit.empty()) {
            auto* f = to_visit.back();
            f->id = nested_field::id_t{0};
            to_visit.pop_back();
            std::visit(reverse_field_collecting_visitor{to_visit}, f->type);
        }
    }

    void create_nested_table() {
        create_table(std::get<struct_type>(test_nested_schema_type()));
    }

    void create_table(const struct_type& type) {
        schema s{
          .schema_struct = type.copy(),
          .schema_id = schema::id_t{1},
          .identifier_field_ids{},
        };
        auto create_res
          = catalog.create_table(table_ident, s, partition_spec{}).get();
        ASSERT_FALSE(create_res.has_error());
    }

    ss::future<std::optional<schema>>
    load_table_schema(const table_identifier& table_ident) {
        auto load_res = catalog.load_table(table_ident).get();
        if (!load_res.has_value()) {
            co_return std::nullopt;
        }
        auto& table = load_res.value();
        EXPECT_NE(table.current_schema_id, schema::unassigned_id);
        auto schema_it = std::ranges::find(
          table.schemas, table.current_schema_id, &schema::schema_id);
        if (schema_it == table.schemas.end()) {
            co_return std::nullopt;
        }
        co_return std::move(*schema_it);
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    filesystem_catalog catalog;
    catalog_schema_manager schema_mgr;
};

class CatalogSchemaManagerTest
  : public CatalogSchemaManagerTestBase
  , public ::testing::Test {};

TEST_F(CatalogSchemaManagerTest, TestCreateTable) {
    auto type = std::get<struct_type>(test_nested_schema_type());
    std::cerr << type << std::endl;
    reset_field_ids(type);

    // Create the table
    auto create_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(create_res.has_error());

    // Fill the field IDs in `type`.
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

    auto table_ident = table_identifier{.ns = {"redpanda"}, .table = "foo"};
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());
    EXPECT_EQ(type, schema->schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillFromExistingTable) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Even if the table already exists, we should be able to fill fields IDs
    // without trouble.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSubset) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Remove a field from the set that we want to fill.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    type.fields.pop_back();

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());

    schema.value().schema_struct.fields.pop_back();
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillNestedSubset) {
    create_nested_table();
    auto schema = load_table_schema(table_ident).get();
    ASSERT_TRUE(schema.has_value());

    // Remove a subfield from the set that we want to fill.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::get<struct_type>(type.fields.back()->type).fields.pop_back();

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(res.has_error());

    std::get<struct_type>(schema.value().schema_struct.fields.back()->type)
      .fields.pop_back();
    EXPECT_EQ(type, schema.value().schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSuperset) {
    create_nested_table();

    // Add a couple nested fields to the desired type.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    for (size_t i = 0; i < 2; ++i) {
        struct_type nested;
        for (size_t j = 0; j < 10; ++j) {
            nested.fields.emplace_back(nested_field::create(
              0,
              fmt::format("inner-{}", j),
              field_required::no,
              boolean_type{}));
        }
        type.fields.emplace_back(nested_field::create(
          0,
          fmt::format("nested-{}", i),
          field_required::no,
          std::move(nested)));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids in `type`
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

    // Check the resulting schema.
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    EXPECT_EQ(39, s.highest_field_id());

    // Sanity check: the field IDs should match what is in the catalog.
    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, s.schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestFillSupersetSubtype) {
    create_nested_table();

    // Add a couple fields to a subfield of the desired type.
    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    for (size_t i = 0; i < 2; ++i) {
        std::get<struct_type>(type.fields.back()->type)
          .fields.emplace_back(nested_field::create(
            0,
            fmt::format("extra-nested-{}", i),
            field_required::no,
            int_type{}));
    }
    // Alter the table schema
    auto ensure_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();
    ASSERT_FALSE(ensure_res.has_error());

    // Fill the ids
    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_FALSE(fill_res.has_error());

    // Check the resulting schema.
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    EXPECT_EQ(19, s.highest_field_id());

    // Sanity check: the field IDs should match what is in the catalog.
    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, s.schema_struct);
}

TEST_F(CatalogSchemaManagerTest, TestOptionalMismatch) {
    struct_type type;
    type.fields.emplace_back(
      nested_field::create(0, "required", field_required::yes, int_type{}));
    type.fields.emplace_back(
      nested_field::create(0, "optional", field_required::no, int_type{}));
    create_table(type);

    // Make the destinations both optional.
    type.fields[0]->required = field_required::no;
    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);

    // Make the destinations both required.
    type.fields[0]->required = field_required::yes;
    type.fields[1]->required = field_required::yes;
    res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);
}

TEST_F(CatalogSchemaManagerTest, TestTypeMismatch) {
    create_nested_table();

    auto type = std::get<struct_type>(test_nested_schema_type());
    reset_field_ids(type);
    std::swap(type.fields.front(), type.fields.back());

    auto res = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema_manager::errc::not_supported);
}

struct type_promotion_case {
    using is_legal = ss::bool_class<struct is_legal_tag>;
    primitive_type source;
    primitive_type dest;
    is_legal legal;

    friend std::ostream&
    operator<<(std::ostream& os, const type_promotion_case& tc) {
        fmt::print(
          os,
          "{{promote {} to {} [{}]}}",
          tc.source,
          tc.dest,
          tc.legal ? "LEGAL" : "ILLEGAL");
        return os;
    }
};

class PrimitiveTypePromotionTest
  : public CatalogSchemaManagerTestBase
  , public testing::TestWithParam<type_promotion_case> {
public:
    primitive_type source_field_type() const {
        return make_copy(GetParam().source);
    }
    primitive_type dest_field_type() const {
        return make_copy(GetParam().dest);
    }
    bool expect_allowed() const { return (bool)GetParam().legal; }
    void append_field(struct_type& type, field_type field) const {
        // TODO(oren): maybe should compute the field id
        type.fields.emplace_back(nested_field::create(
          18, "some_test_field", field_required::no, std::move(field)));
    }
    void promote(struct_type& type) const {
        type.fields.back()->type = dest_field_type();
    }
    struct_type get_source_struct() const {
        auto type = std::get<struct_type>(test_nested_schema_type());
        append_field(type, source_field_type());
        return type;
    }
    struct_type get_dest_struct() const {
        auto type = std::get<struct_type>(test_nested_schema_type());
        append_field(type, dest_field_type());
        return type;
    }
};

INSTANTIATE_TEST_SUITE_P(
  ImplementsPrimitiveTypePromotion,
  PrimitiveTypePromotionTest,
  ::testing::Values(
    type_promotion_case{
      .source = int_type{},
      .dest = long_type{},
      .legal = type_promotion_case::is_legal::yes,
    },
    type_promotion_case{
      .source = date_type{},
      .dest = timestamp_type{},
      .legal = type_promotion_case::is_legal::yes,
    },
    type_promotion_case{
      .source = float_type{},
      .dest = double_type{},
      .legal = type_promotion_case::is_legal::yes,
    },
    type_promotion_case{
      .source = decimal_type{.precision = 10, .scale = 2},
      .dest = decimal_type{.precision = 20, .scale = 2},
      .legal = type_promotion_case::is_legal::yes,
    },
    type_promotion_case{
      .source = int_type{},
      .dest = string_type{},
      .legal = type_promotion_case::is_legal::no,
    },
    type_promotion_case{
      .source = double_type{},
      .dest = float_type{},
      .legal = type_promotion_case::is_legal::no,
    },
    type_promotion_case{
      .source = decimal_type{.precision = 10, .scale = 2},
      .dest = decimal_type{.precision = 10, .scale = 3},
      .legal = type_promotion_case::is_legal::no,
    },
    type_promotion_case{
      .source = decimal_type{.precision = 10, .scale = 2},
      .dest = decimal_type{.precision = 5, .scale = 2},
      .legal = type_promotion_case::is_legal::no,
    },
    type_promotion_case{
      .source = date_type{},
      .dest = timestamptz_type{},
      .legal = type_promotion_case::is_legal::no,
    },
    type_promotion_case{
      .source = fixed_type{.length = 32},
      .dest = fixed_type{.length = 64},
      .legal = type_promotion_case::is_legal::no,
    })); // TODO(oren): add some more illegal cases

TEST_P(PrimitiveTypePromotionTest, CanDoTypePromotion) {
    auto type = get_source_struct();
    create_table(type);
    promote(type);
    reset_field_ids(type);

    std::cerr << type << std::endl;

    auto ensure_res
      = schema_mgr.ensure_table_schema(model::topic{"foo"}, type).get();

    if (expect_allowed()) {
        ASSERT_FALSE(ensure_res.has_error()) << ensure_res.error();
    } else {
        ASSERT_TRUE(ensure_res.has_error());
        EXPECT_EQ(ensure_res.error(), schema_manager::errc::not_supported)
          << ensure_res.error();
    }

    // TODO(oren): how do we confirm that the evolution is valid. like
    // what does it look like? basically the fields should all be exactly
    // the same (ids and that) but with the promoted type.

    auto fill_res
      = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
    if (expect_allowed()) {
        ASSERT_FALSE(fill_res.has_error()) << fill_res.error();
    } else {
        ASSERT_TRUE(fill_res.has_error());
        EXPECT_EQ(fill_res.error(), schema_manager::errc::not_supported)
          << fill_res.error();
    }

    // check that the table schema was updated appropriately

    // TODO(oren): need to implement the actual transaction for these to
    // work for legal ones

    if (!expect_allowed()) {
        // In this case, we want to assert that the table schema didn't
        // change
        type = get_source_struct();
        reset_field_ids(type);
        fill_res
          = schema_mgr.get_registered_ids(model::topic{"foo"}, type).get();
        ASSERT_FALSE(fill_res.has_error()) << fill_res.error();
    }

    auto loaded_table = load_table_schema(table_ident).get();
    ASSERT_TRUE(loaded_table.has_value());
    ASSERT_EQ(loaded_table.value().schema_struct, type);
}
