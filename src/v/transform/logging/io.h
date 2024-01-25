/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/record.h"
#include "transform/logging/event.h"

#pragma once

namespace transform::logging {
class sink {
public:
    sink() = default;
    sink(const sink&) = delete;
    sink& operator=(const sink&) = delete;
    sink(sink&&) = delete;
    sink& operator=(sink&&) = delete;
    virtual ~sink() = default;

    // TODO(oren): consider a return code here. but is it actionable?
    virtual ss::future<>
    write(model::transform_name_view name, ss::chunked_fifo<iobuf>) = 0;
};
} // namespace transform::logging
