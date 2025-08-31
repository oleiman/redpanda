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

#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/persisted_stm.h"
#include "raft/replicate.h"
#include "storage/kvstore.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <expected>

namespace cluster {

/**
 * Responsible for driving suffix truncation, needed for reconciling out of sync
 * partitions across a cluster link.
 */

class suffix_truncation_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "suffix_truncation_stm";
    /**
     * State machine specific error codes.
     */
    enum class errc : int8_t {
        success = 0,
        invalid_offset,
        replicate_exception,
        invalid_input,
    };
    struct errc_category final : public std::error_category {
        const char* name() const noexcept final;

        std::string message(int c) const final;
    };

    const std::error_category& error_category() noexcept;

    std::error_code make_error_code(errc e) noexcept;

    using offset_result = std::expected<model::offset, std::error_code>;
    suffix_truncation_stm(
      raft::consensus*,
      ss::logger&,
      storage::kvstore&,
      std::vector<model::record_batch_type>);

    // ss::future<> start() override;
    // ss::future<> stop() override;

    raft::replicate_stages truncate(
      model::offset rp_end_offset,
      kafka::offset kafka_end_offset,
      kafka::offset expected_end_offset,
      model::timeout_clock::duration timeout = 10s,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    ss::future<iobuf> take_raft_snapshot(model::offset) final {
        co_return iobuf{};
    }
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        // TODO(oren): might be "skip_to_end"...just guessing at this point
        return raft::stm_initial_recovery_policy::read_everything;
    }

    ss::future<std::expected<kafka::offset, std::error_code>>
    get_expected_last_offset(model::timeout_clock::duration sync_timeout);

private:
    ss::future<> do_apply(const model::record_batch& b) final;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) final;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) final;

    bool is_offset_translated_batch(const model::record_batch& batch) const;

    kafka::offset expected_last_offset() const;

    ss::future<> apply_suffix_truncate_batch(const model::record_batch&) const;

    bool
    inflight_truncation_offset_needs_reset(model::term_id insync_term) const {
        return _inflight_truncation_offset.has_value()
               && _inflight_truncation_offset->in_sync_term == insync_term;
    }

    raft::replicate_stages try_replicate_in_stages(
      model::record_batch batches,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

    ss::future<result<raft::replicate_result>> do_truncate(
      model::offset rp_end_offset,
      kafka::offset kafka_end_offset,
      kafka::offset expected_end_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as,
      ss::promise<> enqueued_promise);
    using base_t = raft::persisted_stm<raft::kvstore_backed_stm_snapshot>;

    struct term_offset {
        kafka::offset offset;
        model::term_id in_sync_term;
    };
    friend std::ostream& operator<<(std::ostream&, const term_offset&);

    std::optional<term_offset> _inflight_truncation_offset;
    std::vector<model::record_batch_type> _offset_translated_batches;
    mutex _sync_lock;
    kafka::offset _last_offset;
};

class suffix_truncation_stm_factory : public state_machine_factory {
public:
    explicit suffix_truncation_stm_factory(
      storage::kvstore&, std::vector<model::record_batch_type>);

    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft,
      const cluster::stm_instance_config&) final;

private:
    // TODO(oren): could maybe embed the cluster link table here? or the
    // manager or whatever
    storage::kvstore* _kvstore;
    std::vector<model::record_batch_type> _offset_translated_batches;
};

} // namespace cluster
