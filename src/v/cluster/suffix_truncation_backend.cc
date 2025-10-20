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

#include "cluster/suffix_truncation_backend.h"

#include "cluster/logger.h"
#include "ssx/future-util.h"

#include <seastar/core/loop.hh>

namespace cluster::suffix_truncation {

backend::backend(
  model::node_id self,
  table& table,
  frontend& frontend,
  partition_leaders_table& leaders_table,
  std::optional<std::reference_wrapper<cloud_storage::remote>>
    cloud_storage_api,
  ss::abort_source& as)
  : _self(self)
  , _table(&table)
  , _frontend(&frontend)
  , _leaders_table(&leaders_table)
  , _cloud_storage_api(cloud_storage_api.and_then(
      [](std::reference_wrapper<cloud_storage::remote> csa) {
          return std::make_optional(&csa.get());
      }))
  , _as(as) {}

ss::future<> backend::start() {
    vlog(st_log.info, "backend starting");
    vassert(
      ss::this_shard_id() == suffix_truncation_shard,
      "backend should only run on shard {}",
      suffix_truncation_shard);
    // TODO(oren): store off term for controller leader
    // TODO(oren): set a notification for controller leadership change

    // NOTE: register table notifications unconditionally. exact approach
    // for local-only truncation is TBD

    _table_notification = _table->register_cb([this](id id) {
        vlog(st_log.info, "Calling back truncation {}", id);
        ssx::spawn_with_gate(
          _gate, [this, id]() { return handle_truncation(id); });
    });

    vlog(st_log.info, "Registered notification: {}", _table_notification);

    co_await ss::do_with(
      _table->get_truncations(), [this](const chunked_vector<id>& tids) {
          // TODO(oren): could kick off in parallel
          // TODO(oren): handle might not even need async at this point if we're
          // kicking it off to some work queue or similar.
          return ss::do_for_each(tids, [this](id id) -> ss::future<> {
              return handle_truncation(id);
          });
      });

    // TODO(oren): spawn a fiber to consume truncation steps

    co_return;
}
ss::future<> backend::stop() {
    vlog(st_log.info, "backend stopping");
    _table->unregister_cb(_table_notification);
    co_await _gate.close();
}

ss::future<> backend::handle_truncation(id id) noexcept {
    // TODO(oren): kick off to a work queue or something
    if (_gate.is_closed()) {
        co_return;
    }
    auto h = _gate.hold();
    auto guard = co_await _mutex.get_units(_as);

    // TODO(oren); seems like you could just store one metadata object on the
    // backend, skip the table, and just reject any commands w/ the wrong ID.
    auto trunc = _table->get_truncation(id);
    if (!trunc.has_value()) {
        vlog(st_log.warn, "Truncation not found: {}", id);
        co_return;
    }

    vlog(st_log.info, "Handling truncation {} {}", id, trunc);

    auto [it, added] = _outstanding.try_emplace(id, truncation_work{});

    /*
        topic_work work;
        for (const auto& tp : trunc.value().truncation.topics) {
            partition_work pw;
            for (const auto& p : tp.partitions) {
              pw.partition_ops
            }
        }
      _outstanding.emplace(id, work);
     */

    co_return;
}

} // namespace cluster::suffix_truncation
