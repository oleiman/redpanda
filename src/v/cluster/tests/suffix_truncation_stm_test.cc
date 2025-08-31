// Copyright 2025 Redpanda Data, Inc.
//
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/suffix_truncation_stm.h"
#include "raft/consensus.h"
#include "raft/tests/stm_test_fixture.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>

using namespace testing;

ss::logger t_log{"test_log"};

using fixture_base_t = raft::stm_raft_fixture<cluster::suffix_truncation_stm>;

struct SuffixTruncationStmFixture : public fixture_base_t {
    fixture_base_t::stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return std::make_tuple(
          builder.create_stm<cluster::suffix_truncation_stm>(
            node.raft().get(),
            t_log,
            node.get_kvstore(),
            model::offset_translator_batch_types()));
    }

    std::optional<fixture_base_t::stm_shptrs_t> get_leader_stms() {
        auto leader_id = get_leader();
        if (!leader_id) {
            return std::nullopt;
        }
        return std::make_tuple(node(*leader_id)
                                 .raft()
                                 ->stm_manager()
                                 ->get<cluster::suffix_truncation_stm>());
    }
    void prepare_raft_group() {
        enable_offset_translation();
        initialize_state_machines(3).get();
        leader_id = wait_for_leader(10s).get();

        auto first_ts = model::timestamp::now();
        for (int i = 0; i < 5; ++i) {
            node(leader_id)
              .raft()
              ->replicate(
                make_batches(
                  100,
                  [first_ts](auto) {
                      return make_batches_with_timestamp(first_ts);
                  }),
                raft::replicate_options(raft::consistency_level::quorum_ack))
              .get();
            node(leader_id).raft()->step_down("test").get();
            leader_id = wait_for_leader(10s).get();
        }
        wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 10s)
          .get();
    }

    static model::record_batch
    make_batches_with_timestamp(model::timestamp ts) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(iobuf::from("key"), iobuf::from("value"));
        auto batch = std::move(builder).build();

        batch.header().first_timestamp = ts;
        batch.header().max_timestamp = ts;

        return batch;
    }

    model::node_id leader_id{};
};

TEST_F(SuffixTruncationStmFixture, test_suffix_truncation_happy_path) {
    prepare_raft_group();
    auto stms = get_leader_stms();
    ASSERT_TRUE(stms.has_value());
    [[maybe_unused]] auto [truncate_stm] = std::move(stms).value();

    auto raft = node(leader_id).raft();

    auto rp_committed = model::prev_offset(raft->committed_offset());
    auto kafka_committed = raft->log()->from_log_offset(rp_committed);

    vlog(
      t_log.debug,
      "Initial end offsets: rp: {}, k: {}\n",
      rp_committed,
      kafka_committed);

    auto kafka_trunc = kafka_committed - model::offset{100};
    auto rp_trunc = raft->log()->to_log_offset(kafka_trunc);

    auto res = truncate_stm->truncate(
      rp_trunc,
      model::offset_cast(kafka_trunc),
      model::offset_cast(kafka_committed),
      10s);

    res.request_enqueued.get();
    auto repl_result = res.replicate_finished.get();

    ASSERT_FALSE(repl_result.has_error()) << repl_result.error();

    vlog(
      t_log.debug,
      "RESULT: o: {} t: {}",
      repl_result.value().last_offset,
      repl_result.value().last_term);
}
