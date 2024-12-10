// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/outcome.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"

#include <iosfwd>

namespace iceberg {
bool is_primitive_type_promotion(
  const iceberg::field_type& src, const iceberg::field_type& dst);

bool satisfies_type_promotion_policy(
  const iceberg::field_type& src, const iceberg::field_type& dst);

enum class evo_operation {
    reorder,
    update,
    rename,
    add,
    remove,
    fill,
};

struct evo_action {
    evo_action(
      evo_operation op,
      std::optional<const nested_field*> src_field,
      std::optional<nested_field*> dest_field)
      : op(op)
      , src_field(src_field)
      , dest_field(dest_field) {}
    evo_operation op;
    std::optional<const nested_field*> src_field;
    std::optional<nested_field*> dest_field;
};

enum class compat_errc {
    success,
    incompatible,
    mismatch,
};

// TODO(oren): we really want to generate some kind of tree-shaped update plan
// here. node based I guess.
struct compat_plan {
    chunked_vector<evo_action> actions;
    // used to assign IDs to new columns and avoid reusing IDs for incompatible
    // types.
    nested_field::id_t source_highest_id;
};

std::ostream& operator<<(std::ostream&, const evo_operation&);
std::ostream& operator<<(std::ostream&, const evo_action&);
std::ostream& operator<<(std::ostream&, const compat_plan&);

checked<compat_plan, compat_errc>
is_compatible(struct_type& dest, const schema& source);

checked<std::nullopt_t, compat_errc> apply(struct_type& dest, compat_plan&);

} // namespace iceberg
