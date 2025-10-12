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

#include "cluster/fwd.h"
#include "cluster/suffix_truncation_types.h"
#include "container/chunked_hash_map.h"
#include "model/metadata.h"

namespace cluster::suffix_truncation {

class tracker {
public:
    struct topic_meta {
        id truncation_id;
        topic_blocked is_blocked;
    };

    bool is_blocked(model::topic_namespace_view) const;
    bool has_truncation(model::topic_namespace_view) const;
    void apply_update(const truncation_meta&);

private:
    void remove(const truncation_meta&);
    chunked_hash_map<
      model::topic_namespace,
      id,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _topics;
};

} // namespace cluster::suffix_truncation
