/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "base/vlog.h"
#include "cloud_io/logger.h"
#include "cloud_io/null_policy.h"
#include "cloud_io/scheduler.h"

namespace cloud_io {

std::unique_ptr<scheduler_policy>
make_scheduler_policy(policy_type t, scheduler& shell, size_t capacity) {
    switch (t) {
    case policy_type::null:
        return std::make_unique<null_policy>(shell, capacity);
    }
    vlog(
      log.warn,
      "Unknown cloud_io scheduler policy_type {}, defaulting to null",
      static_cast<uint8_t>(t));
    return std::make_unique<null_policy>(shell, capacity);
}

} // namespace cloud_io
