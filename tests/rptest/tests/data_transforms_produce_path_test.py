# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.data_transforms_test import BaseDataTransformsTest


class BaseDataTransformsProducePathTest(BaseDataTransformsTest):
    """
    Base class for produce-path transform tests.

    Produce-path transforms intercept the Kafka produce path and replace
    the original batch with the transform's output before replication.
    Unlike sidecar transforms, there are no output topics -- the
    transformed data is written to the input topic itself.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(  # type: ignore[reportUnknownMemberType]
            test_context,
            extra_rp_conf={
                "data_transforms_produce_path_enabled": True,
            },
        )

    def _deploy_produce_path_wasm(
        self,
        name: str,
        input_topic: TopicSpec,
        file: str = "tinygo/identity.wasm",
    ):
        """
        Deploy a produce-path transform.

        During development, all transforms default to produce_path mode on
        the server side. The rpk CLI still requires an output topic, so we
        pass the input topic as a dummy -- the server strips output_topics
        for produce-path transforms. Produce-path transforms don't create
        sidecar processors, so we skip the wait-for-running check.
        """
        self._deploy_wasm(
            name=name,
            input_topic=input_topic,
            output_topic=input_topic,
            file=file,
            wait_running=False,
        )

    def _produce_and_verify(self, topic: TopicSpec, num_records: int = 100):
        """
        Produce records via rpk, consume them back, and verify count matches.
        """
        for i in range(num_records):
            self._rpk.produce(topic.name, f"key-{i}", f"value-{i}")

        # rpk consume returns raw output; use json format and count lines
        output = self._rpk.consume(
            topic.name, n=num_records, format="%v\\n", timeout=30
        )
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == num_records, (
            f"expected {num_records} records, got {len(lines)}"
        )
        return lines


class DataTransformsProducePathTest(BaseDataTransformsProducePathTest):
    """
    Tests for produce-path WASM transforms.
    """

    topics = [TopicSpec(partition_count=1)]

    @cluster(num_nodes=3)
    def test_identity_logging(self):
        """
        Deploy an identity-with-logging transform on the produce path.
        Verify that log records appear in _redpanda.transform_logs,
        proving the WASM function actually executed during produce.
        """
        logs_topic = "_redpanda.transform_logs"
        topic = self.topics[0]
        self._deploy_produce_path_wasm(
            name="produce-path-logging",
            input_topic=topic,
            file="tinygo/identity_logging.wasm",
        )

        # Produce a few records
        num_records = 10
        for i in range(num_records):
            self._rpk.produce(topic.name, f"key-{i}", f"value-{i}")

        # Verify records landed in the data topic
        output = self._rpk.consume(
            topic.name, n=num_records, format="%v\\n", timeout=30
        )
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == num_records

        # Verify log records appeared in the transform logs topic.
        # The identity_logging transform writes a log line per record,
        # so we should see at least one log record.
        def logs_appeared():
            try:
                log_output = self._rpk.consume(
                    logs_topic, n=1, format="%v\\n", timeout=10
                )
                log_lines = [l for l in log_output.strip().split("\n") if l]
                return len(log_lines) > 0
            except Exception:
                return False

        wait_until(
            logs_appeared,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="no log records appeared in transform logs topic",
        )
        self.logger.info(
            "produce-path logging: WASM execution confirmed via log records"
        )

    @cluster(num_nodes=3)
    def test_no_transform_passthrough(self):
        """
        Produce to a topic with no produce-path transform deployed.
        Verify data is written unchanged -- regression test for the
        hot-path lookup returning nullopt.
        """
        topic = self.topics[0]
        consumed = self._produce_and_verify(topic)
        self.logger.info(f"passthrough: produced and consumed {len(consumed)} records")

    @cluster(num_nodes=3)
    def test_deploy_delete_deploy(self):
        """
        Deploy a logging transform, delete it, re-deploy it.

        Uses the identity_logging transform which writes a log line per
        record to _redpanda.transform_logs. After delete, verifies that
        producing does NOT generate new log entries -- proving the engine
        was evicted, not just the index entry removed.
        """
        topic = self.topics[0]
        logs_topic = "_redpanda.transform_logs"

        num_records = 10

        # Deploy logging transform and produce through it
        self._deploy_produce_path_wasm(
            name="produce-path-logging",
            input_topic=topic,
            file="tinygo/identity_logging.wasm",
        )
        self._produce_and_verify(topic, num_records=num_records)

        # Wait for all log records to be flushed. The logging transform
        # writes one log line per record, and the log flush interval is
        # 500ms, so we need to wait for all 10 to land before we can
        # take a clean HWM baseline.
        def all_logs_flushed():
            hwm = sum(p.high_watermark for p in self._rpk.describe_topic(logs_topic))
            return hwm >= num_records

        wait_until(
            all_logs_flushed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="not all log records flushed before delete",
        )

        log_hwm = list(self._rpk.describe_topic(logs_topic))

        # Delete the transform
        self._delete_wasm(name="produce-path-logging")

        # Produce more records -- these should NOT go through a transform
        self._produce_and_verify(topic, num_records=num_records)

        # Verify no NEW log records appeared after delete. If the engine
        # was not evicted, the logging transform would still run and
        # produce log entries.
        new_log_hwm = list(self._rpk.describe_topic(logs_topic))

        old_end = sum(p.high_watermark for p in log_hwm)
        new_end = sum(p.high_watermark for p in new_log_hwm)
        assert new_end == old_end, (
            f"transform still running after delete: "
            f"log hwm advanced from {old_end} to {new_end}"
        )
        self.logger.info("engine eviction verified: no logs after delete")

        # Re-deploy and verify it works again
        self._deploy_produce_path_wasm(
            name="produce-path-logging",
            input_topic=topic,
            file="tinygo/identity_logging.wasm",
        )
        self._produce_and_verify(topic, num_records=num_records)

    @cluster(num_nodes=3)
    def test_identity_preserves_headers(self):
        """
        Verify that a produce-path identity transform preserves record
        headers. Produces records with a custom header via rpk, consumes
        them back, and checks the header survived the transform.

        This replaces the TransformVerifierService-based tests which
        were designed for the sidecar model (separate clean output topic)
        and don't work for produce-path (same topic, mixed with internal
        batches for transactional produces).
        """
        topic = self.topics[0]
        self._deploy_produce_path_wasm(
            name="produce-path-identity",
            input_topic=topic,
        )

        num_records = 100
        header_key = "test-hdr"
        for i in range(num_records):
            self._rpk.produce(
                topic.name,
                f"key-{i}",
                f"value-{i}",
                headers=[f"{header_key}:v{i}"],
            )

        output = self._rpk.consume(
            topic.name,
            n=num_records,
            format="%v|%H|%h{%k=%v }\\n",
            timeout=30,
        )
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == num_records, (
            f"expected {num_records} records, got {len(lines)}"
        )

        missing_headers = 0
        for i, line in enumerate(lines):
            if f"{header_key}=" not in line:
                self.logger.warning(f"record {i} missing header: {line}")
                missing_headers += 1

        assert missing_headers == 0, (
            f"{missing_headers}/{num_records} records lost headers"
        )
        self.logger.info(
            f"identity transform preserved headers on all {num_records} records"
        )

    @cluster(num_nodes=3)
    def test_identity_preserves_values(self):
        """
        Verify that a produce-path identity transform preserves record
        keys and values exactly. Produces known data, consumes it back,
        and checks byte-for-byte equality.
        """
        topic = self.topics[0]
        self._deploy_produce_path_wasm(
            name="produce-path-identity",
            input_topic=topic,
        )

        num_records = 100
        for i in range(num_records):
            self._rpk.produce(topic.name, f"key-{i}", f"value-{i}")

        output = self._rpk.consume(
            topic.name,
            n=num_records,
            format="%k|%v\\n",
            timeout=30,
        )
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == num_records, (
            f"expected {num_records} records, got {len(lines)}"
        )

        for i, line in enumerate(lines):
            expected = f"key-{i}|value-{i}"
            assert line == expected, f"record {i}: expected '{expected}', got '{line}'"

    @cluster(num_nodes=3)
    def test_deploy_with_output_topics(self):
        """
        Verify that produce-path transforms can be deployed with
        declared output topics (fan-out targets). The identity
        transform only writes to the default (input) topic, so
        records should still land there and the output topic should
        be empty.
        """
        input_topic = self.topics[0]
        output_topic = TopicSpec(partition_count=1)
        self._rpk.create_topic(
            output_topic.name,
            partitions=output_topic.partition_count,
            replicas=3,
        )
        self._deploy_wasm(
            name="produce-path-fanout",
            input_topic=input_topic,
            output_topic=output_topic,
            file="tinygo/identity.wasm",
            wait_running=False,
        )

        num_records = 10
        for i in range(num_records):
            self._rpk.produce(input_topic.name, f"key-{i}", f"val-{i}")

        # Records land on input topic (identity writes to default)
        output = self._rpk.consume(
            input_topic.name, n=num_records, format="%v\\n", timeout=30
        )
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == num_records, (
            f"expected {num_records} on input topic, got {len(lines)}"
        )

        # Output topic should be empty (identity doesn't route)
        out_hwm = sum(
            p.high_watermark for p in self._rpk.describe_topic(output_topic.name)
        )
        assert out_hwm == 0, f"expected empty output topic, got hwm={out_hwm}"

    @cluster(num_nodes=3)
    def test_fanout_routing(self):
        """
        Deploy the fanout transform which writes each record to both
        the default (input topic) AND to declared output topics.
        Verify records appear on both topics.
        """
        input_topic = self.topics[0]
        output_topic = TopicSpec(partition_count=1)
        self._rpk.create_topic(
            output_topic.name,
            partitions=output_topic.partition_count,
            replicas=3,
        )
        self._deploy_wasm(
            name="produce-path-fanout",
            input_topic=input_topic,
            output_topic=output_topic,
            file="tinygo/fanout.wasm",
            wait_running=False,
        )

        num_records = 10
        for i in range(num_records):
            self._rpk.produce(input_topic.name, f"key-{i}", f"val-{i}")

        # Verify records landed on the input topic
        input_out = self._rpk.consume(
            input_topic.name, n=num_records, format="%v\\n", timeout=30
        )
        input_lines = [l for l in input_out.strip().split("\n") if l]
        assert len(input_lines) == num_records, (
            f"expected {num_records} on input topic, got {len(input_lines)}"
        )

        # Verify records also landed on the output topic
        def output_has_records():
            hwm = sum(
                p.high_watermark for p in self._rpk.describe_topic(output_topic.name)
            )
            return hwm >= num_records

        wait_until(
            output_has_records,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"expected {num_records} records on output topic",
        )

        output_out = self._rpk.consume(
            output_topic.name, n=num_records, format="%k|%v\\n", timeout=30
        )
        output_lines = [l for l in output_out.strip().split("\n") if l]
        assert len(output_lines) == num_records, (
            f"expected {num_records} on output topic, got {len(output_lines)}"
        )
        self.logger.info(f"fan-out routing: {num_records} records on both topics")
