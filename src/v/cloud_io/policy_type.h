/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"

#include <cstdint>
#include <string_view>

namespace cloud_io {

/// Selector for the cloud_io::scheduler admission policy.
/// Chosen at construction; changing requires a rolling restart.
enum class policy_type : uint8_t {
    null,
};

constexpr std::string_view to_string_view(policy_type t) {
    switch (t) {
    case policy_type::null:
        return "null";
    }
    return "unknown";
}

inline fmt::iterator format_to(policy_type t, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(t));
}

} // namespace cloud_io
