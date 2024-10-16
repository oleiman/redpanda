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

# from rptest.tests.prealloc_nodes import PreallocNodesTest
# from rptest.utils.mode_checks import skip_debug_mode

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.utils.util import wait_until


# PreallocNodesTest
class CloudStoragePropsWarnTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=3,
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
        for n in self.redpanda.nodes:
            matches += self.log_searcher._capture_log(n, WARNING_LOG)
        # basically sort by timestamp rather than by node of origin
        return sorted(matches)

    @cluster(num_nodes=3)
    @matrix(
        rr=[
            True,
            False,
        ],
        rw=[
            True,
            False,
        ],
        rd=[
            True,
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
        disabled = not any([rr, rw])

        self._rpk.create_topic('foo', partitions=1, config=topic_props)

        # Scrape logs for the topic config warning and assert that there
        # are some new occurrences (or not)
        def _check_logs(expect_warning: bool, prev_n: int = 0):
            logs = self.scrape_warning(self.redpanda.nodes)
            print(f"LOOGS: {len(logs)} {json.dumps(logs, indent=1)}")
            total_n = len(logs)
            diff_n = total_n - prev_n
            if expect_warning:
                assert diff_n > 0, f"Expected new warning, got none"
            else:
                assert diff_n == 0, f"Unexpected {diff_n} warnings: {json.dumps(logs[-diff_n:], indent=1)}"
            return total_n

        n_logs = _check_logs(not full)

        self.logger.debug(
            "Confirm that the warning is written out again after a restart.")
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        n_logs = _check_logs(not full, prev_n=n_logs)

        if full:
            return

        self.logger.debug(
            "Now fix the topic properties, one by one, to enable FULL mode...")

        if not rw:
            self._rpk.alter_topic_config('foo', 'redpanda.remote.write',
                                         'true')

        n_logs = _check_logs(disabled, n_logs)

        if not rr:
            self._rpk.alter_topic_config('foo', 'redpanda.remote.read', 'true')

        self.logger.debug(
            "Cloud storage should be fully enabled now, the final alter-config producing no warnings"
        )
        n_logs = _check_logs(False, prev_n=n_logs)

        # self.logger.debug(
        #     "Restart the cluster again. We shouldn't see any new warnings.")

        # self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        # n_logs = _check_logs(False, prev_n=n_logs)
