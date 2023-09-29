# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SecurityConfig, make_redpanda_service
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

EXAMPLE_TOPIC = 'foo'


class SASLReauthBase(RedpandaTest):
    def __init__(self, conn_max_reauth=8000, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(
            num_brokers=3,
            security=security,
            extra_rp_conf={'connections_max_reauth_ms': conn_max_reauth},
            **kwargs)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def make_su_client(self):
        return PythonLibrdkafka(self.redpanda,
                                username=self.su_username,
                                password=self.su_password,
                                algorithm=self.su_algorithm)


class SASLReauthenticationTest(SASLReauthBase):
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context=test_context,
                         conn_max_reauth=8000,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_scram_reauth(self):
        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(0.0)

        for i in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(0.1)

        producer.flush(timeout=2)

        reauths = {}
        for node in self.redpanda.nodes:
            reauths[node.name] = self.redpanda.count_log_node(
                node, "SASL reauthentication")

        self.logger.debug(f"SCRAM reauths: {json.dumps(reauths, indent=1)}")
        assert (
            any(reauths[n.name] > 0 for n in self.redpanda.nodes)
        ), f"Expected client reauth on some broker...Reauths: {json.dumps(reauths, indent=1)}"

        exps = {}
        for node in self.redpanda.nodes:
            exps[node.name] = self.redpanda.count_log_node(
                node, "Session expired")

        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Client should reauth before session expiry...Expirations: {json.dumps(exps, indent=1)}"


class ReauthDisabledTest(SASLReauthBase):
    """
    Tests that connections_max_reauth_ms{0} produces original behavior
    i.e. no reauth, no session expiry
    """
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context=test_context,
                         conn_max_reauth=0,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_scram(self):
        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(0.0)

        for i in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(0.1)

        producer.flush(timeout=2)

        reauths = {}
        for node in self.redpanda.nodes:
            reauths[node.name] = self.redpanda.count_log_node(
                node, "SASL reauthentication")

        assert (
            all(reauths[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Expected no client reauth...Reauths: {json.dumps(reauths, indent=1)}"

        exps = {}
        for node in self.redpanda.nodes:
            exps[node.name] = self.redpanda.count_log_node(
                node, "Session expired")
        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"SCRAM sessions should not expire...Expirations: {json.dumps(exps, indent=1)}"
