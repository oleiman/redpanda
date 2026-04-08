# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import MetricsEndpoint, SISettings, get_cloud_storage_type
from rptest.tests.redpanda_test import RedpandaTest

from ducktape.mark import matrix
from ducktape.utils.util import wait_until


class ABSOAuthDeletionTest(RedpandaTest):
    """
    Verify that tiered storage GC works correctly when Redpanda
    authenticates to Azure Blob Storage via OAuth (AKS OIDC federation).

    This catches bugs like x-ms-version leaking into batch delete
    sub-requests, which only manifests with OAuth credentials.

    CDT-only: skipped when not running against real Azure (Azurite
    does not support Bearer auth).
    """

    segment_size = 1024 * 1024  # 1 MiB
    topics = [TopicSpec(partition_count=1, replication_factor=3)]

    def __init__(self, test_context):
        # Inject ABS type for SISettings. On non-Azure environments the
        # @matrix decorator produces zero variants so __init__ never runs.
        if (
            not hasattr(test_context, "injected_args")
            or test_context.injected_args is None
        ):
            test_context.injected_args = {}
        test_context.injected_args["cloud_storage_type"] = CloudStorageType.ABS

        si_settings = SISettings(
            test_context,
            cloud_storage_credentials_source="azure_vm_instance_metadata",
            log_segment_size=self.segment_size,
            fast_uploads=True,
        )
        extra_rp_conf = {
            "cloud_storage_housekeeping_interval_ms": 5000,
        }

        super().__init__(
            test_context=test_context,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
            num_brokers=3,
        )

    @cluster(num_nodes=4)  # 3 brokers + 1 kgo-verifier
    @matrix(
        cloud_storage_type=get_cloud_storage_type(
            applies_only_on=[CloudStorageType.ABS], docker_use_arbitrary=True
        )
    )
    def test_batch_delete_with_oauth(self, cloud_storage_type):
        """Produce data, trigger retention, verify segments are deleted
        without batch delete partial failures."""
        topic = self.topics[0].name
        rpk = RpkTool(self.redpanda)

        # Set tight retention to trigger GC quickly
        rpk.alter_topic_config(topic, "retention.bytes", str(self.segment_size * 3))
        rpk.alter_topic_config(
            topic, "retention.local.target.bytes", str(self.segment_size * 2)
        )

        # Produce enough data to create multiple cloud segments
        msg_size = 1024
        msg_count = self.segment_size * 10 // msg_size
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=msg_size,
            msg_count=msg_count,
        )

        # Wait for segments to be uploaded to cloud storage
        def uploaded_enough():
            return (
                self.redpanda.metric_sum("vectorized_cloud_storage_successful_uploads")
                > 5
            )

        wait_until(
            uploaded_enough,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Segments not uploaded to cloud storage",
        )

        # Wait for GC to delete at least one segment.
        def segments_deleted():
            n = self.redpanda.metric_sum(
                "redpanda_cloud_storage_deleted_segments_total",
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
            )
            self.logger.info(f"deleted_segments_total = {n}")
            return n > 0

        wait_until(
            segments_deleted,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="GC did not delete any cloud segments",
        )

        # The key assertion: no batch delete partial failures.
        # This metric fires when the HTTP request succeeds but the
        # multipart response indicates undeleted keys -- the exact
        # failure mode caused by x-ms-version leaking into sub-requests.
        failures = self.redpanda.metric_sum(
            "vectorized_cloud_storage_batch_delete_errors"
        )
        assert failures == 0, (
            f"Expected 0 batch delete partial failures, got {failures}"
        )
