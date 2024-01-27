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

#include "cluster/errc.h"
#include "model/record.h"
#include "transform/logging/event.h"

#pragma once

namespace transform::logging {

namespace io {
struct json_batch {
    json_batch(model::transform_name n, ss::chunked_fifo<iobuf> e)
      : name(std::move(n))
      , events(std::move(e)) {}
    model::transform_name name;
    ss::chunked_fifo<iobuf> events;
};
} // namespace io

class client {
public:
    client() = default;
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    virtual ~client() = default;

    // TODO(oren): consider a return code here. but is it actionable?
    virtual ss::future<>
      write(model::partition_id, ss::chunked_fifo<io::json_batch>) = 0;

    virtual model::partition_id
    compute_output_partition(model::transform_name_view name)
      = 0;
};
} // namespace transform::logging
