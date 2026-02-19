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

#include "pandaproxy/schema_registry/kafka_client_transport.h"

#include "kafka/client/client.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "model/namespace.h"

#include <seastar/core/coroutine.hh>

namespace pandaproxy::schema_registry {

kafka_client_transport::kafka_client_transport(
  ss::sharded<kafka::client::client>& client)
  : _client(client) {}

ss::future<produce_result>
kafka_client_transport::produce(model::record_batch batch) {
    auto res = co_await _client.local().produce_record_batch(
      model::schema_registry_internal_tp, std::move(batch));
    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(
          res.error_code, res.error_message.value_or(""));
    }
    co_return produce_result{.base_offset = res.base_offset};
}

ss::future<model::offset> kafka_client_transport::get_high_watermark() {
    auto offsets = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);
    if (
      offsets.data.topics.size() != 1
      || offsets.data.topics[0].partitions.size() != 1) {
        throw kafka::exception(
          kafka::error_code::unknown_server_error,
          "Malformed ListOffsets Kafka response for internal topic");
    }
    co_return offsets.data.topics[0].partitions[0].offset;
}

ss::future<> kafka_client_transport::consume_range(
  model::offset start,
  model::offset end,
  ss::noncopyable_function<ss::future<>(model::record_batch)> consumer) {
    struct batch_consumer {
        ss::noncopyable_function<ss::future<>(model::record_batch)> fn;
        ss::future<ss::stop_iteration>
        operator()(model::record_batch batch) {
            return fn(std::move(batch)).then(
              [] { return ss::stop_iteration::no; });
        }
        void end_of_stream() {}
    };
    auto rdr = kafka::client::make_client_fetch_batch_reader(
      _client.local(),
      model::schema_registry_internal_tp,
      start,
      end);
    co_await std::move(rdr).consume(
      batch_consumer{std::move(consumer)}, model::no_timeout);
}

} // namespace pandaproxy::schema_registry
