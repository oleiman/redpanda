// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/compatibility.h"
#include "iceberg/datatypes.h"
#include "iceberg/datatypes_json.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/tests/test_schemas.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iostream>
#include <ranges>

using namespace iceberg;

namespace {
// TODO(oren): move to iceberg utils
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
} // namespace

TEST(StructCompatibilityTest, CanGeneratePlan) {
    schema original_type{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::unassigned_id,
      .identifier_field_ids = {},
    };

    struct_type type_copy = original_type.schema_struct.copy();
    reset_field_ids(type_copy);
    // swap foo & baz
    std::swap(type_copy.fields[0], type_copy.fields[2]);
    // promote bar from int to long
    type_copy.fields[1]->type = long_type{};

    // swap foo (new position) and person
    // std::swap(type_copy.fields[2], type_copy.fields.back());

    // drop foo from the end of the struct
    // TODO(oren): bad optional access for some reason
    // type_copy.fields.pop_back();

    // swap person[name] and person[age]
    std::swap(
      std::get<struct_type>(type_copy.fields[6]->type).fields[0],
      std::get<struct_type>(type_copy.fields[6]->type).fields[1]);

    // fmt::print(std::cerr, "{}", type_copy);

    // and add a decimal type on the end
    type_copy.fields.emplace_back(nested_field::create(
      0,
      "added_field",
      field_required::no,
      decimal_type{.precision = 10, .scale = 2}));

    auto plan_res = is_compatible(type_copy, original_type);
    ASSERT_FALSE(plan_res.has_error());

    fmt::print(std::cerr, "{}\n", plan_res.value());

    auto apply_res = apply(type_copy, plan_res.value());
    ASSERT_FALSE(apply_res.has_error());

    // Now all the reorder and rename type stuff should be gone
    // plan_res = is_compatible(type_copy, original_type);
    // ASSERT_FALSE(plan_res.has_error());
    // ASSERT_EQ(plan_res.value().actions.size(), 1) << plan_res.value();
}
