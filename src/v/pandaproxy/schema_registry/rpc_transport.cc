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

#include "pandaproxy/schema_registry/rpc_transport.h"

#include "kafka/data/rpc/client.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <stdexcept>

namespace pandaproxy::schema_registry {

namespace {

bool is_retriable(cluster::errc ec) {
    return ec == cluster::errc::not_leader || ec == cluster::errc::timeout;
}

constexpr int max_retries = 5;
constexpr auto initial_backoff = std::chrono::milliseconds(100);
constexpr auto max_backoff = std::chrono::milliseconds(1000);

} // namespace

rpc_transport::rpc_transport(kafka::data::rpc::client& client)
  : _client(client) {}

ss::future<produce_result>
rpc_transport::produce(model::record_batch batch) {
    auto res = co_await _client.produce_with_offset(
      model::schema_registry_internal_tp, std::move(batch));
    if (res.ec != cluster::errc::success) {
        throw std::runtime_error(
          fmt::format("RPC produce failed: {}", res.ec));
    }
    if (!res.base_offset.has_value()) {
        throw std::runtime_error(
          "RPC produce succeeded but base_offset not available");
    }
    co_return produce_result{.base_offset = *res.base_offset};
}

ss::future<model::offset> rpc_transport::get_high_watermark() {
    auto backoff = initial_backoff;
    for (int attempt = 0;; ++attempt) {
        kafka::data::rpc::topic_partitions tp;
        tp.topic = model::schema_registry_internal_tp.topic;
        tp.partitions.push_back(
          model::schema_registry_internal_tp.partition);
        chunked_vector<kafka::data::rpc::topic_partitions> topics;
        topics.push_back(std::move(tp));

        auto result = co_await _client.get_partition_offsets(
          std::move(topics));
        if (result.has_error()) {
            if (is_retriable(result.error()) && attempt < max_retries) {
                vlog(
                  srlog.debug,
                  "RPC get_partition_offsets transient error (attempt "
                  "{}/{}): {}",
                  attempt + 1,
                  max_retries,
                  result.error());
                co_await ss::sleep(backoff);
                backoff = std::min(backoff * 2, max_backoff);
                continue;
            }
            throw std::runtime_error(fmt::format(
              "RPC get_partition_offsets failed: {}", result.error()));
        }

        const auto& offsets_map = result.value();
        auto topic_it = offsets_map.find(
          model::schema_registry_internal_tp.topic);
        if (topic_it == offsets_map.end()) {
            throw std::runtime_error(
              "RPC get_partition_offsets: topic not found in response");
        }
        auto partition_it = topic_it->second.find(
          model::schema_registry_internal_tp.partition);
        if (partition_it == topic_it->second.end()) {
            throw std::runtime_error(
              "RPC get_partition_offsets: partition not found in response");
        }
        const auto& offset_result = partition_it->second;
        if (offset_result.err != cluster::errc::success) {
            if (is_retriable(offset_result.err) && attempt < max_retries) {
                vlog(
                  srlog.debug,
                  "RPC get_partition_offsets partition transient error "
                  "(attempt {}/{}): {}",
                  attempt + 1,
                  max_retries,
                  offset_result.err);
                co_await ss::sleep(backoff);
                backoff = std::min(backoff * 2, max_backoff);
                continue;
            }
            throw std::runtime_error(fmt::format(
              "RPC get_partition_offsets partition error: {}",
              offset_result.err));
        }
        co_return kafka::offset_cast(offset_result.offsets.high_watermark);
    }
}

ss::future<> rpc_transport::consume_range(
  model::offset start,
  model::offset end,
  ss::noncopyable_function<ss::future<>(model::record_batch)> consumer) {
    // The RPC consume API may not return all records in a single call,
    // so loop until we've consumed up to the desired end offset.
    constexpr size_t max_bytes = 1 << 20; // 1 MiB per fetch
    auto current = start;
    auto backoff = initial_backoff;
    while (current < end) {
        auto result = co_await _client.consume(
          model::schema_registry_internal_tp,
          offset_cast(current),
          offset_cast(end),
          1,
          max_bytes,
          std::chrono::seconds(5));
        if (result.has_error()) {
            if (is_retriable(result.error()) && backoff <= max_backoff) {
                vlog(
                  srlog.debug,
                  "RPC consume transient error: {}",
                  result.error());
                co_await ss::sleep(backoff);
                backoff = std::min(backoff * 2, max_backoff);
                continue;
            }
            throw std::runtime_error(
              fmt::format("RPC consume failed: {}", result.error()));
        }
        auto& reply = result.value();
        if (reply.err != cluster::errc::success) {
            if (is_retriable(reply.err) && backoff <= max_backoff) {
                vlog(
                  srlog.debug,
                  "RPC consume transient error: {}",
                  reply.err);
                co_await ss::sleep(backoff);
                backoff = std::min(backoff * 2, max_backoff);
                continue;
            }
            throw std::runtime_error(
              fmt::format("RPC consume error: {}", reply.err));
        }
        // Reset backoff on success
        backoff = initial_backoff;
        if (reply.batches.empty()) {
            break;
        }
        for (auto& batch : reply.batches) {
            auto last = batch.last_offset();
            co_await consumer(std::move(batch));
            current = last + model::offset{1};
        }
    }
}

} // namespace pandaproxy::schema_registry
