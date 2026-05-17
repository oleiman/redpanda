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

/// Caller-supplied intent label for a cloud_io operation.
///
/// Identifies the kind of work being performed, irrespective of the
/// subsystem implementing it. Used as the scheduling key for the
/// cloud_io::scheduler.
///
/// Mapping is by function, not by subsystem: a fetch read coming
/// from cloud_topics L0 or L1 both map to consumer_fetch.
enum class group_id : uint8_t {
    /// Uploads on the Kafka produce path (cloud_topics L0 PUT).
    /// Latency-critical.
    producer_upload,
    /// Reads serving Kafka fetch requests (cloud_topics L0/L1 data).
    /// Latency-critical.
    consumer_fetch,
    /// Everything else: manifest/metastore I/O, archival writes,
    /// hydration, replication, housekeeping. The bulk.
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
    return "unknown";
}

inline fmt::iterator format_to(group_id g, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(g));
}

} // namespace cloud_io
