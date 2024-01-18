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

#pragma once

#include "model/transform.h"
#include "transform/commit_batcher.h"
#include "transform/rpc/client.h"
#include "wasm/logger.h"

#include <seastar/core/lowres_clock.hh>

#include <absl/container/flat_hash_map.h>

namespace transform {

class sink;
class log_manager {
    using meta = wasm::logger::meta;

public:
    log_manager() = delete;
    ~log_manager();
    log_manager(const log_manager&) = delete;
    log_manager& operator=(const log_manager&) = delete;
    log_manager(log_manager&&) = delete;
    log_manager& operator=(log_manager&&) = delete;

    explicit log_manager(
      model::node_id self,
      rpc::client* rpc_client,
      cluster::topic_table* topic_table);

    ss::future<> start();
    ss::future<> stop();

    void enqueue_log(meta metadata, std::string_view message);

private:
    ss::future<> flush();

    [[maybe_unused]] model::node_id _self;
    [[maybe_unused]] rpc::client* _rpc_client;
    [[maybe_unused]] cluster::topic_table* _topic_table;
    [[maybe_unused]] std::unique_ptr<transform::sink> _log_sink;
    // TODO(oren): maybe a seastar queue, but probably not
    absl::
      flat_hash_map<ss::sstring, ss::chunked_fifo<model::transform_log_event>>
        _event_queue;
    ss::gate _gate;
    [[maybe_unused]] ss::abort_source _as;
    ss::timer<> _flush_timer;
};

} // namespace transform
