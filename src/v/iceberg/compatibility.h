// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes.h"

namespace iceberg {
bool is_primitive_type_promotion(
  const iceberg::field_type& src, const iceberg::field_type& dst);

bool satisfies_type_promotion_policy(
  const iceberg::field_type& src, const iceberg::field_type& dst);
} // namespace iceberg
