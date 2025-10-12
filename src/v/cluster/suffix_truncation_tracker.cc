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

#include "cluster/suffix_truncation_tracker.h"

namespace cluster::suffix_truncation {

bool tracker::is_blocked(model::topic_namespace_view tp_ns) const {
    return has_truncation(tp_ns);
}
bool tracker::has_truncation(model::topic_namespace_view tp_ns) const {
    return _topics.contains(tp_ns);
}
void tracker::apply_update(const truncation_meta& meta) {
    if (meta.state == state::done) {
        remove(meta);
        return;
    }
    for (const auto t : meta.topics()) {
        _topics[t] = meta.id;
    };
}

void tracker::remove(const truncation_meta& meta) {
    std::ranges::for_each(
      meta.topics(), [this, id = meta.id](model::topic_namespace_view tp_ns) {
          if (auto it = _topics.find(tp_ns);
              it != _topics.end() && it->second == id) {
              _topics.erase(it);
          }
      });
}
} // namespace cluster::suffix_truncation
