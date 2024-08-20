# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random
import requests
from enum import IntEnum

import numpy as np

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.util import expect_exception
from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.mark import parametrize

from rptest.services.utils import LogSearchLocal
from rptest.util import wait_until_result, wait_until


class AdminUUIDOperationsTest(RedpandaTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, num_brokers=3)
        self.admin = Admin(self.redpanda)
        self.log_searcher = LogSearchLocal(ctx, [], self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)

    def setUp(self):
        self.redpanda.start(auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        self._create_initial_topics()

    @cluster(num_nodes=3)
    def test_getting_node_id_to_uuid_map(self):
        admin = Admin(self.redpanda)
        uuids = self.admin.get_broker_uuids()
        assert len(uuids) == 3, "UUID map should contain 3 brokers"
        all_ids = set()
        for n in uuids:
            assert 'node_id' in n
            assert 'uuid' in n
            all_ids.add(n['node_id'])

        brokers = self.admin.get_brokers()
        for b in brokers:
            assert b['node_id'] in all_ids

    def _uuids_updated(self, nodes_n=4):
        uuids = self.admin.get_broker_uuids()
        if len(uuids) != nodes_n:
            return False, None

        return True, uuids

    @cluster(num_nodes=3)
    def test_overriding_node_id(self):
        to_stop = self.redpanda.nodes[0]
        initial_to_stop_id = self.redpanda.node_id(to_stop)
        # Stop node and clear its data directory
        self.redpanda.stop_node(to_stop)
        self.redpanda.clean_node(to_stop,
                                 preserve_current_install=True,
                                 preserve_logs=False)

        self.redpanda.start_node(to_stop,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)

        # wait for the node to join with new ID
        uuids = wait_until_result(
            lambda: self._uuids_updated(),
            timeout_sec=30,
            err_msg="Node was unable to join the cluster")

        uuids = self.admin.get_broker_uuids()
        old_uuid = None

        for n in uuids:
            id = n['node_id']
            if id == initial_to_stop_id:
                old_uuid = n['uuid']

        # get current node id and UUID
        current = self.admin.get_broker_uuid(to_stop)

        self.admin.override_node_id(to_stop,
                                    current_uuid=current['node_uuid'],
                                    new_node_id=initial_to_stop_id,
                                    new_node_uuid=old_uuid)

        self.redpanda.restart_nodes(to_stop,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)

        after_restart = self.admin.get_broker_uuid(to_stop)

        assert after_restart['node_id'] == initial_to_stop_id
        assert after_restart['node_uuid'] == old_uuid

    def scrape_uuid(self, node: ClusterNode) -> str | None:
        UUID_LOG = "Generated new UUID for node"
        lines = [
            s.strip() for s in self.log_searcher._capture_log(node, UUID_LOG)
            if UUID_LOG in s
        ]
        print(lines)
        if len(lines) < 1:
            return None
        assert len(lines) == 1, f"Too many: {json.dumps(lines, indent=1)}"
        return lines[0].split(":")[-1].strip()

    def _restart_node(self,
                      node: ClusterNode,
                      overrides: dict | None = None,
                      extra_cli: list[str] = [],
                      drop_disk: bool = False):
        self.redpanda.stop_node(node)
        if drop_disk:
            self.redpanda.clean_node(node,
                                     preserve_current_install=True,
                                     preserve_logs=False)

        self.redpanda.start_node(node,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False,
                                 override_cfg_params=overrides,
                                 extra_cli=extra_cli)

    def _wait_until_healthy(self,
                            node: ClusterNode,
                            expect_healthy: bool = True) -> dict | None:
        def is_healthy():
            report = self.admin.get_cluster_health_overview(node=node)
            if report['is_healthy'] is not expect_healthy:
                return False, None
            return True, report

        report = wait_until_result(lambda: is_healthy(),
                                   timeout_sec=30,
                                   backoff_sec=2,
                                   err_msg="Cluster did not stabilize")

        self.logger.debug(
            f"Health Report ({node.name}): {json.dumps(report, indent=1)}")

    @cluster(num_nodes=3)
    @parametrize(mode="cfg-override")
    @parametrize(mode="cli-override")
    @parametrize(mode="no-override")
    def test_ghost_node_join_fail(self, mode):
        to_stop = self.redpanda.nodes[0]
        initial_to_stop_id = self.redpanda.node_id(to_stop)

        self._restart_node(to_stop, drop_disk=True)

        # wait for the node to join with new ID
        uuids = wait_until_result(
            lambda: self._uuids_updated(),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Node was unable to join the cluster")

        old_uuid = None
        for n in uuids:
            id = n['node_id']
            if id == initial_to_stop_id:
                old_uuid = n['uuid']

        assert old_uuid is not None, "Old uuid unexpectedly None"

        ghost_node_id = self.admin.get_broker_uuid(to_stop)['node_id']

        self.logger.debug(
            "When we drop the disk again, node restart should fail (controller will have lost consensus)"
        )
        with expect_exception(TimeoutError, lambda _: True):
            self._restart_node(to_stop, drop_disk=True)

        self.logger.debug(
            "Grab the last generated UUID from logs since the node was not able to join the cluster"
        )
        current_uuid = self.scrape_uuid(to_stop)
        assert current_uuid is not None, "Didn't find UUID in logs"

        self.logger.debug("Restart the node again (but keep the disk)")

        THE_OVERRIDE = f"{current_uuid} -> ID: '{initial_to_stop_id}' ; UUID: '{old_uuid}'"
        if mode == "cfg-override":
            self.logger.debug(
                f"Override with known-good uuid/id via node config: {THE_OVERRIDE}"
            )
            self._restart_node(
                to_stop,
                dict(
                    node_id_overrides=[
                        dict(current_uuid=current_uuid,
                             new_id=initial_to_stop_id)
                    ],
                    node_uuid_overrides=[
                        dict(current_uuid=current_uuid, new_uuid=old_uuid)
                    ],
                ),
                drop_disk=False,
            )
        elif mode == "cli-override":
            self.logger.debug(
                f"Override with known-good uuid/id via command line options: {THE_OVERRIDE}"
            )
            self._restart_node(
                to_stop,
                extra_cli=[
                    "--node-id-overrides",
                    f"{current_uuid}:{initial_to_stop_id}",
                    "--node-uuid-overrides",
                    f"{current_uuid}:{old_uuid}",
                ],
                drop_disk=False,
            )
        elif mode == "no-override":
            self.logger.debug(
                "Omit the override to confirm that we're still stuck in that case"
            )
            with expect_exception(TimeoutError, lambda _: True):
                self._restart_node(to_stop, drop_disk=False)
            self.logger.debug("And short circuit the test case")
            return
        else:
            assert False, f"Unexpected mode: '{mode}'"

        self.logger.debug(
            "Wait until the target node reflects the given overrides")

        wait_until(lambda: self.admin.get_broker_uuid(to_stop)['node_id'] ==
                   initial_to_stop_id,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg=f"{to_stop.name} did not take the ID override")

        wait_until(lambda: self.admin.get_broker_uuid(to_stop)['node_uuid'] ==
                   old_uuid,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg=f"{to_stop.name} did not take the UUID override")

        self.logger.debug(
            f"Decommission ghost node [{ghost_node_id}] and force controller leadership transfer..."
        )
        self.admin.decommission_broker(ghost_node_id, node=to_stop)
        self.admin.transfer_leadership_to(namespace='redpanda',
                                          topic='controller',
                                          partition=0)

        self.logger.debug(f"...and wait for the cluster to become healthy")

        self._wait_until_healthy(to_stop)

        self.logger.debug(
            "Check that all this state sticks across a rolling restart")

        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            auto_assign_node_id=True)

        after_restart = self.admin.get_broker_uuid(to_stop)

        assert after_restart[
            'node_id'] == initial_to_stop_id, f"Bad node id after override: {after_restart['node_id']}"
        assert after_restart[
            'node_uuid'] == old_uuid, f"Bad node uuid after override: {after_restart['node_uuid']}"

        self._wait_until_healthy(to_stop)

    @cluster(num_nodes=3)
    def test_multiple_node_ghost_fail(self):
        to_stop = self.redpanda.nodes[1:]
        initial_to_stop_ids = [self.redpanda.node_id(n) for n in to_stop]

        self.logger.debug("Kill one node, all is good")

        self._restart_node(to_stop[0], drop_disk=True)

        uuids = wait_until_result(
            lambda: self._uuids_updated(),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Node was unable to join the cluster")

        ghost_node_id = self.admin.get_broker_uuid(to_stop[0])['node_id']

        old_uuids = {}
        for n in uuids:
            id = n['node_id']
            if id in initial_to_stop_ids:
                old_uuids[id] = n['uuid']

        assert len(
            old_uuids) == 2, f"Unexpected old_uuids: {json.dumps(old_uuids)}"

        self.logger.debug("Drop another node, this time restart should fail")

        for n in to_stop:
            with expect_exception(TimeoutError, lambda _: True):
                self._restart_node(n, drop_disk=True)

        current_uuids = [self.scrape_uuid(n) for n in to_stop]
        assert len(current_uuids
                   ) == 2, f"Missing some UUIDs: {json.dumps(current_uuids)}"

        self.logger.debug(
            "Restart both nodes again, with overrides. Keep both disks")

        self.redpanda.restart_nodes(
            to_stop,
            override_cfg_params=dict(
                node_id_overrides=[
                    dict(current_uuid=current_uuids[n],
                         new_id=initial_to_stop_ids[n])
                    for n in range(0, len(to_stop))
                ],
                node_uuid_overrides=[
                    dict(current_uuid=current_uuids[n],
                         new_uuid=old_uuids[initial_to_stop_ids[n]])
                    for n in range(0, len(to_stop))
                ],
            ))

        self.logger.debug(
            f"Decommission ghost node [{ghost_node_id}] and force controller leadership transfer..."
        )

        self.admin.decommission_broker(ghost_node_id, node=to_stop[0])
        self.admin.transfer_leadership_to(namespace='redpanda',
                                          topic='controller',
                                          partition=0)

        self.logger.debug("...and wait for the cluster to become healthy")

        self._wait_until_healthy(to_stop[0])
        self._wait_until_healthy(to_stop[1])
