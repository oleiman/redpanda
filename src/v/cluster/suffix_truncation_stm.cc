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

#include "cluster/suffix_truncation_stm.h"

#include "cluster/logger.h"
#include "cluster/suffix_truncate_record.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"

namespace cluster {

namespace {
struct local_snapshot
  : serde::
      envelope<local_snapshot, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(last_offset); }

    kafka::offset last_offset;
};

} // namespace

const char* suffix_truncation_stm::errc_category::name() const noexcept {
    return "suffix_truncation_error";
}

std::string suffix_truncation_stm::errc_category::message(int c) const {
    switch (static_cast<errc>(c)) {
    case errc::success:
        return "Success";
    case errc::invalid_offset:
        return "Invalid write offset";
    case errc::replicate_exception:
        return "Exception thrown during replication";
    case errc::invalid_input:
        return "Invalid input provided to write at offset state machine";
    }
    std::unreachable();
}

const std::error_category& suffix_truncation_stm::error_category() noexcept {
    static errc_category e;
    return e;
}

std::error_code suffix_truncation_stm::make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}

suffix_truncation_stm::suffix_truncation_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  std::vector<model::record_batch_type> offset_translated_batches)
  : base_t("suffix_truncation_stm.snapshot", logger, raft, kvstore)
  , _offset_translated_batches(std::move(offset_translated_batches))
  , _sync_lock("suffix-truncate") {}

ss::future<std::expected<kafka::offset, std::error_code>>
suffix_truncation_stm::get_expected_last_offset(
  model::timeout_clock::duration sync_timeout) {
    auto u = co_await _sync_lock.get_units();
    auto sync_result = co_await sync(sync_timeout);
    if (!sync_result) {
        vlog(
          _log.info,
          "unable to retrieve expected last offset. State machine sync failed");
        co_return std::unexpected{raft::errc::not_leader};
    }
    co_return expected_last_offset();
}

raft::replicate_stages suffix_truncation_stm::truncate(
  model::offset rp_end_offset,
  kafka::offset kafka_end_offset,
  kafka::offset expected_end_offset,
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    ss::promise<> enqueued_promise;
    auto f = enqueued_promise.get_future();
    return raft::replicate_stages{
      std::move(f),
      do_truncate(
        rp_end_offset,
        kafka_end_offset,
        expected_end_offset,
        timeout,
        as,
        std::move(enqueued_promise))};
}

ss::future<result<raft::replicate_result>> suffix_truncation_stm::do_truncate(
  model::offset rp_end_offset,
  kafka::offset kafka_end_offset,
  kafka::offset prev_end_offset,
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as,
  ss::promise<> enqueued_promise) {
    auto u = co_await _sync_lock.get_units();
    const auto prev_insync_term = _insync_term;

    auto sync_result = co_await sync(timeout);
    if (!sync_result) {
        _inflight_truncation_offset.reset();
        enqueued_promise.set_value();
        co_return raft::errc::not_leader;
    }
    const auto current_insync_term = _insync_term;

    if (prev_insync_term != current_insync_term) {
        _inflight_truncation_offset.reset();
    }

    const auto stm_last_offset = expected_last_offset();
    vlog(
      _log.trace,
      "Requested truncate at offset: {} with previous end offset: {}. stm last "
      "offset: {} [inflight_last_offset: {}, last_offset: {}]",
      kafka_end_offset,
      prev_end_offset,
      stm_last_offset,
      _inflight_truncation_offset,
      _last_offset);

    /*
     * Truncation can only proceed if the prev_end_offset matches
     * stm_last_offset
     */
    if (prev_end_offset != stm_last_offset) {
        enqueued_promise.set_value();
        vlog(
          _log.debug,
          "Expected previous end offset: {} does not match with last stm "
          "tracked offset: {}",
          prev_end_offset,
          stm_last_offset);
        co_return make_error_code(errc::invalid_offset);
    }

    // TODO(oren): need to assert something here?

    _inflight_truncation_offset = term_offset{
      .offset = kafka_end_offset,
      .in_sync_term = _insync_term,
    };

    storage::record_batch_builder builder(
      model::record_batch_type::suffix_truncate, model::offset(0));

    suffix_truncate_record val;
    val.rp_end_offset = rp_end_offset;
    val.kafka_end_offset = kafka_end_offset;
    builder.add_raw_kv(
      serde::to_iobuf(suffix_truncate_record::key), serde::to_iobuf(val));
    auto cmd_batch = std::move(builder).build();
    auto stages = try_replicate_in_stages(std::move(cmd_batch), as);

    // TODO(oren): log?
    auto enq = std::move(stages.request_enqueued).finally([u = std::move(u)] {
    });

    std::move(enq).forward_to(std::move(enqueued_promise));
    auto r_fut = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    const bool needs_inflight_reset = inflight_truncation_offset_needs_reset(
      current_insync_term);

    if (r_fut.failed()) {
        vlog(
          _log.warn,
          "Replication failed with exception: {}, needs inflight truncation "
          "offset reset: {}, inflight truncation offset: {}",
          r_fut.get_exception(),
          needs_inflight_reset,
          _inflight_truncation_offset);
        if (needs_inflight_reset) {
            _inflight_truncation_offset.reset();
        }
        co_await _raft->step_down("suffix_truncation_replication_failed");
        co_return make_error_code(errc::replicate_exception);
    }

    auto result = r_fut.get();
    if (result.has_error()) {
        vlog(
          _log.warn,
          "Replication failed with an error: {}, needs inflight truncation "
          "offset reset: {}, inflight truncation offset: {}",
          result.error().message(),
          needs_inflight_reset,
          _inflight_truncation_offset);
        if (needs_inflight_reset) {
            _inflight_truncation_offset.reset();
        }

        co_await _raft->step_down("suffix_truncation_replication_failed");
    }
    co_return result;
}

kafka::offset suffix_truncation_stm::expected_last_offset() const {
    if (!_inflight_truncation_offset.has_value()) {
        return _last_offset;
    }

    if (_inflight_truncation_offset->in_sync_term == _insync_term) {
        return std::min(_last_offset, _inflight_truncation_offset->offset);
    }

    return _last_offset;
}

raft::replicate_stages suffix_truncation_stm::try_replicate_in_stages(
  model::record_batch batch,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    try {
        return _raft->replicate_in_stages(
          _insync_term,
          chunked_vector<model::record_batch>::single(std::move(batch)),
          raft::replicate_options(raft::consistency_level::quorum_ack, as));
    } catch (...) {
        vlog(
          _log.warn,
          "Replicate in stages failed with exception - {}",
          std::current_exception());
        return {
          ss::now(),
          ssx::now<result<raft::replicate_result>>(
            make_error_code(errc::replicate_exception))};
    }
}

ss::future<> suffix_truncation_stm::apply_raft_snapshot(const iobuf&) {
    // TODO(oren): i really don't know what the semantics are here. going to
    // have to do some thankin' about when we apply snapshots and whatnot.
    auto start_k_offset = _raft->log()->from_log_offset(_raft->start_offset());
    _last_offset = kafka::prev_offset(model::offset_cast(start_k_offset));
    return ss::now();
}

ss::future<> suffix_truncation_stm::do_apply(const model::record_batch& b) {
    if (b.header().type == model::record_batch_type::suffix_truncate)
      [[unlikely]] {
        co_await apply_suffix_truncate_batch(b);
        co_return;
    }
    if (is_offset_translated_batch(b)) [[unlikely]] {
        co_return;
    }

    _last_offset = model::offset_cast(
      _raft->log()->from_log_offset(b.last_offset()));

    // NOTE(oren): I think we just want to unconditionally reset the inflight if
    // there is one. maybe should log? overall point being that if we get some
    // kafka data while there is a truncate request in flight, that invalidates
    // the command, right?

    if (_inflight_truncation_offset.has_value()) {
        vlog(
          _log.warn,
          "Applied data batch during suffix truncation, aborting. "
          "inflight_truncation_offset: {}, incoming batch kafka offset: {}",
          _inflight_truncation_offset,
          _last_offset);
    }

    _inflight_truncation_offset.reset();

    // TODO(oren): remove probably
    // if (!_inflight_truncation_offset.has_value()) {
    //     co_return;
    // }
    // if (
    //   _last_offset >= _inflight_truncation_offset->offset
    //   || b.term() > _inflight_truncation_offset->in_sync_term) {
    //     _inflight_truncation_offset.reset();
    // }
    co_return;
}

ss::future<> suffix_truncation_stm::apply_suffix_truncate_batch(
  const model::record_batch& b) const {
    vlog(_log.debug, "Applying suffix truncate batch: {}", b.header());
    return ss::now();
}

ss::future<raft::local_snapshot_applied>
suffix_truncation_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& data) {
    _last_offset
      = serde::from_iobuf<local_snapshot>(std::move(data)).last_offset;
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
suffix_truncation_stm::take_local_snapshot(ssx::semaphore_units) {
    auto data = serde::to_iobuf(local_snapshot{.last_offset = _last_offset});
    raft::stm_snapshot_header hdr{
      .version = 0,
      .snapshot_size = static_cast<int32_t>(data.size_bytes()),
      .offset = model::prev_offset(next())};

    co_return raft::stm_snapshot{.header = hdr, .data = std::move(data)};
}

bool suffix_truncation_stm::is_offset_translated_batch(
  const model::record_batch& batch) const {
    return std::ranges::find(_offset_translated_batches, batch.header().type)
           != _offset_translated_batches.end();
}

std::ostream&
operator<<(std::ostream& o, const suffix_truncation_stm::term_offset& to) {
    fmt::print(
      o, "{{offset: {}, in_sync_term: {}}}", to.offset, to.in_sync_term);
    return o;
}

suffix_truncation_stm_factory::suffix_truncation_stm_factory(
  storage::kvstore& kvstore,
  std::vector<model::record_batch_type> offset_translated_batches)
  : _kvstore(&kvstore)
  , _offset_translated_batches(std::move(offset_translated_batches)) {}

bool suffix_truncation_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return model::is_user_topic(cfg.ntp());
}

void suffix_truncation_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<suffix_truncation_stm>(
      raft, clusterlog, *_kvstore, _offset_translated_batches);
    raft->log()->stm_manager()->add_stm(std::move(stm));
}

} // namespace cluster
