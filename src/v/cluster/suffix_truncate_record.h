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

#include "model/fundamental.h"
#include "serde/envelope.h"

namespace cluster {

enum class suffix_truncation_key : std::uint8_t {
    truncate = 0,
};

struct suffix_truncate_record
  : public serde::envelope<
      suffix_truncate_record,
      serde::version<0>,
      serde::compat_version<0>> {
    model::offset rp_end_offset{};
    kafka::offset kafka_end_offset{};

    auto serde_fields() { return std::tie(rp_end_offset, kafka_end_offset); }

    static constexpr auto key{suffix_truncation_key::truncate};
};

} // namespace cluster
