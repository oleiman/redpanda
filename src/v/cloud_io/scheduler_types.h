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
#include <utility>

namespace cloud_io {

/// Selects the cloud_io::scheduler admission policy at construction.
/// Changing the active policy requires a rolling restart.
enum class policy_type : uint8_t {
    /// No-op admission gate; the client pool's capacity is the only
    /// constraint.
    null,
    /// Weighted fair-share-by-occupancy admission policy.
    fair,
};

constexpr std::string_view to_string_view(policy_type t) {
    switch (t) {
    case policy_type::null:
        return "null";
    case policy_type::fair:
        return "fair";
    }
    std::unreachable();
}

inline fmt::iterator format_to(policy_type t, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(t));
}

/// Caller-supplied intent label for a cloud_io operation. Used as the
/// scheduling key for cloud_io::scheduler.
enum class group_id : uint8_t {
    /// Object uploads on the Kafka produce path. Latency-critical.
    producer_upload,
    /// Reads serving Kafka fetch requests. Latency-critical.
    consumer_fetch,
    /// Everything else: manifest I/O, archival writes, hydration,
    /// replication, housekeeping.
    default_group,
};

inline constexpr size_t num_group_ids = 3;

constexpr std::string_view to_string_view(group_id g) {
    switch (g) {
    case group_id::producer_upload:
        return "producer_upload";
    case group_id::consumer_fetch:
        return "consumer_fetch";
    case group_id::default_group:
        return "default_group";
    }
    std::unreachable();
}

inline fmt::iterator format_to(group_id g, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(g));
}

} // namespace cloud_io
