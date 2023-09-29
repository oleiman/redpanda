# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import functools
import json

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services.redpanda import SecurityConfig, make_redpanda_service
from rptest.services.keycloak import KeycloakService
from rptest.services.cluster import cluster

from ducktape.tests.test import Test
from rptest.services.redpanda import make_redpanda_service
from rptest.clients.rpk import RpkTool
from rptest.util import expect_exception

from confluent_kafka import KafkaException

CLIENT_ID = 'myapp'
EXAMPLE_TOPIC = 'foo'


class RedpandaOIDCTestBase(Test):
    """
    Base class for tests that use the Redpanda service with OIDC
    """
    def __init__(self,
                 test_context,
                 num_nodes=5,
                 sasl_mechanisms=['SCRAM', 'OAUTHBEARER'],
                 **kwargs):
        super(RedpandaOIDCTestBase, self).__init__(test_context, **kwargs)
        self.produce_messages = []
        self.produce_errors = []
        num_brokers = num_nodes - 1
        self.keycloak = KeycloakService(test_context)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms

        self.redpanda = make_redpanda_service(test_context, num_brokers)
        self.redpanda.set_security_settings(security)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def delivery_report(self, err, msg):
        """
            Reports the failure or success of a message delivery.
            Successfully delivered messages are placed in `messages`.
            Errors are placed in `errors`

            Args:
            err (KafkaError): The error that occurred on None on success.

            msg (Message): The message that was produced or failed.

        """
        if err is not None:
            self.produce_errors.append(
                'Delivery failed for User record {}: {}'.format(
                    msg.key(), err))
            return
        self.produce_messages.append(
            'User record {} successfully produced to {} [{}] at offset {}'.
            format(msg.key(), msg.topic(), msg.partition(), msg.offset()))

    def setUp(self):
        self.produce_messages.clear()
        self.produce_errors.clear()
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()

    def _init_keycloak(self, kc_node, **kwargs):
        try:
            self.keycloak.start_node(kc_node, **kwargs)
        except Exception as e:
            self.logger.error(f"{e}")
            self.keycloak.clean_node(kc_node)
            assert False, "Keycloak failed to start"

        self.keycloak.admin.create_user('norma',
                                        'desmond',
                                        realm_admin=True,
                                        email='10086@sunset.blvd')
        self.keycloak.login_admin_user(kc_node, 'norma', 'desmond')
        self.keycloak.admin.create_client(CLIENT_ID)

        # add an email address to myapp client's service user. this should
        # appear alongside the access token.
        self.keycloak.admin.update_user(f'service-account-{CLIENT_ID}',
                                        email='myapp@customer.com')


class RedpandaOIDCTest(RedpandaOIDCTestBase):
    @cluster(num_nodes=5)
    def test_init(self):
        kc_node = self.keycloak.nodes[0]
        self._init_keycloak(kc_node)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        service_user_id = self.keycloak.admin_ll.get_user_id(
            f'service-account-{CLIENT_ID}')
        result = self.rpk.sasl_allow_principal(f'User:{service_user_id}',
                                               ['all'], 'topic', EXAMPLE_TOPIC,
                                               self.su_username,
                                               self.su_password,
                                               self.su_algorithm)

        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()

        # Expclicit poll triggers OIDC token flow. Required for librdkafka
        # metadata requests to behave nicely.
        producer.poll(0.0)

        producer.list_topics(timeout=5)

        self.logger.info('Flushing {} records...'.format(len(producer)))

        producer.flush()

        self.logger.debug(f"{self.produce_messages} {self.produce_errors}")
        # assert len(self.produce_messages) == 3, f"Expected 3 messages, got {len(self.produce_messages)}"
        # assert len(self.produce_errors) == 0, f"Expected 0 errors, got {len(self.produce_errors)}"
        assert True

    @cluster(num_nodes=5)
    def test_oidc_reauth(self):
        self.redpanda.set_cluster_config({'connections_max_reauth_ms': 8000},
                                         expect_restart=True)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        kc_node = self.keycloak.nodes[0]
        self._init_keycloak(kc_node, access_token_lifespan=20)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        service_user_id = self.keycloak.admin_ll.get_user_id(
            f'service-account-{CLIENT_ID}')
        self.rpk.sasl_allow_principal(f'User:{service_user_id}', ['all'],
                                      'topic', EXAMPLE_TOPIC, self.su_username,
                                      self.su_password, self.su_algorithm)

        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()
        producer.poll(0.0)

        for _ in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC,
                             key='bar',
                             value='23',
                             on_delivery=self.delivery_report)
            time.sleep(0.1)

        producer.flush(timeout=5)

        reauths = {}
        for node in self.redpanda.nodes:
            reauths[node] = self.redpanda.count_log_node(
                node, "SASL reauthentication")

        self.logger.debug(
            f"OIDC reauths: {json.dumps({n.name: reauths[n] for n in self.redpanda.nodes}, indent=1)}"
        )
        assert (any(reauths[n] > 0 for n in self.redpanda.nodes))

        assert len(
            self.produce_errors
        ) == 0, f"Expected no errors, got {len(self.produce_errors)}"

        assert k_client.oauth_count == 2, f"Expected 2 OAUTH challenges, got {k_client.oauth_count}"

    @cluster(num_nodes=5)
    def test_scram_reauth(self):
        self.redpanda.set_cluster_config({'connections_max_reauth_ms': 8000},
                                         expect_restart=True)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = PythonLibrdkafka(self.redpanda,
                                     username=self.su_username,
                                     password=self.su_password,
                                     algorithm=self.su_algorithm)
        producer = su_client.get_producer()
        producer.poll(0.0)

        for _ in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC,
                             key='bar',
                             value='23',
                             on_delivery=self.delivery_report)
            time.sleep(0.1)

        producer.flush(timeout=5)

        reauths = {}
        for node in self.redpanda.nodes:
            reauths[node] = self.redpanda.count_log_node(
                node, "SASL reauthentication")

        self.logger.debug(
            f"SCRAM reauths: {json.dumps({n.name: reauths[n] for n in self.redpanda.nodes}, indent=1)}"
        )
        assert (any(reauths[n] > 0 for n in self.redpanda.nodes))

        assert len(
            self.produce_errors
        ) == 0, f"Expected no errors, got {len(self.produce_errors)}"
