# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.utils import LogSearchLocal
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.utils.util import wait_until


class CloudStoragePropsWarnTest(RedpandaTest):
    SNAPSHOT_MAX_AGE_S = 5

    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=3,
                         extra_rp_conf={
                             'controller_snapshot_max_age_sec':
                             self.SNAPSHOT_MAX_AGE_S
                         },
                         si_settings=SISettings(
                             test_context=test_context,
                             cloud_storage_enable_remote_read=False,
                             cloud_storage_enable_remote_write=False,
                             fast_uploads=True))
        self.admin = Admin(self.redpanda)
        self.log_searcher = LogSearchLocal(test_context, [],
                                           self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)
        self._rpk = RpkTool(self.redpanda)

    # TODO(oren): check si mode
    def scrape_warning(self, nodes: list[ClusterNode]) -> list[str]:
        matches: list[str] = []
        WARNING_LOG = "'Cloud storage not fully enabled for topic'"
        for n in nodes:
            matches += self.log_searcher._capture_log(n, WARNING_LOG)
        # basically sort by timestamp rather than by node of origin
        return sorted(matches)

    def wait_for_logs(self, expected: int, prev_n: int,
                      nodes: list[ClusterNode]) -> int:
        def _check():
            logs = self.scrape_warning(nodes)
            diff_n = len(logs) - prev_n
            new_warnings = logs[-diff_n:] if diff_n > 0 else []
            self.logger.warn(
                f"NEW WARNINGS (expect {expected}): {json.dumps(new_warnings, indent=1)}"
            )
            assert diff_n <= expected, f"Too many new warnings! Expected {diff_n} got {len(new_warnings)}"
            return diff_n == expected, logs

        return len(
            wait_until_result(lambda: _check(),
                              timeout_sec=10,
                              backoff_sec=1,
                              err_msg="Didn't get the right number"))

    def get_controller_leader(self) -> ClusterNode:
        id = self.admin.await_stable_leader('controller',
                                            partition=0,
                                            namespace="redpanda",
                                            timeout_s=30)
        return self.redpanda.get_node(id)

    @cluster(num_nodes=3)
    @matrix(
        rr=[
            # True,
            False,
        ],
        rw=[
            # True,
            False,
        ],
        rd=[
            # True,
            False,
        ],
    )
    def test_create_topic(self, rr, rw, rd):
        topic_props = {
            'redpanda.remote.read': str(rr).lower(),
            'redpanda.remote.write': str(rw).lower(),
            'redpanda.remote.delete': str(rd).lower(),
        }

        full = all([rr, rw])

        self._rpk.create_topic('foo', partitions=1, config=topic_props)

        leader = self.get_controller_leader()

        n_logs = self.wait_for_logs(1 if not full else 0,
                                    prev_n=0,
                                    nodes=[leader])

        self.logger.debug(
            "Confirm that the warning is written out again after a restart.")
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        leader = self.get_controller_leader()

        n_logs = self.wait_for_logs(1 if not full else 0,
                                    prev_n=n_logs,
                                    nodes=[leader])

        if full:
            self.logger.debug(
                "Full cloud storage already enabled, so we're done. Remaining cases check fixing things up."
            )
            return

        self.logger.debug(
            "Now fix the topic properties, one by one, to enable FULL mode...")

        if not rw:
            self._rpk.alter_topic_config('foo', 'redpanda.remote.write',
                                         'true')

        n_logs = self.wait_for_logs(1 if not rr else 0, n_logs, nodes=[leader])

        if not rr:
            self._rpk.alter_topic_config('foo', 'redpanda.remote.read', 'true')

        self.logger.debug(
            "Cloud storage should be fully enabled now, the final alter-config producing no new warnings"
        )
        n_logs = self.wait_for_logs(0, prev_n=n_logs, nodes=[leader])

        for n in self.redpanda.nodes:
            self.redpanda.wait_for_controller_snapshot(n)

        # for n in self.redpanda.nodes:
        #     self.redpanda.wait_for_controller_snapshot(n)

        self.logger.debug(
            "Restart the cluster again. Original CREATE warning should be snapshotted away."
        )

        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        # TODO(oren): maybe we should get the topic leader?
        leader = self.get_controller_leader()

        # TODO(oren): we should be able to snapshot far enough that this is 0 eventually, no?
        n_logs = self.wait_for_logs(1, prev_n=n_logs, nodes=[leader])
