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

#include "cluster/suffix_truncation_types.h"

namespace cluster::suffix_truncation {
fmt::iterator partition_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{pid: {}, offset: {}}}", pid, offset);
}
fmt::iterator topic_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{name: {}, partitions: {}}}", nt, partitions);
}
fmt::iterator suffix_truncation::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "topics_to_truncate: {}", topics);
}
fmt::iterator truncation_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", truncation);
}
fmt::iterator truncation_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "id: {}, ec: {}", id, ec);
}
fmt::iterator truncate_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "id: {}, truncation: {}, op_ts: {}", id, truncation, op_timestamp);
}
} // namespace cluster::suffix_truncation
