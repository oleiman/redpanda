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

#pragma once

#include "base/format_to.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <optional>

namespace cloud_topics::l1 {

// An object ID is a unique identifier for a cloud topic L1 object.
using object_id = named_type<uuid_t, struct l1_object_id_tag>;

inline object_id create_object_id() { return object_id{uuid_t::create()}; }

// Hint to file_io::read_object: in addition to serving the immediate
// byte range, kick off a background download of this larger
// partition-segment region (capped by config). Subsequent reads of
// any byte range within [segment_position, segment_position +
// segment_size) hit the prefetched file via
// cache_service::get_stream_range.
struct partition_prefetch_hint {
    size_t segment_position = 0;
    size_t segment_size = 0;

    bool operator==(const partition_prefetch_hint&) const = default;
};

// An extent of a remote object, which is a pair of offset and size.
struct object_extent {
    object_id id;
    size_t position = 0;
    size_t size = 0;
    // Optional hint for K-fanout broadening; absent = byte-range-only
    // read, no prefetch.
    std::optional<partition_prefetch_hint> prefetch_hint;

    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace cloud_topics::l1
