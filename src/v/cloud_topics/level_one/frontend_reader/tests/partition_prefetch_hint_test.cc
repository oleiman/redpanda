/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_topics;

namespace {

// Two fixed topic IDs for use across tests.
const model::topic_id topic_id_a{uuid_t::create()};
const model::topic_id topic_id_b{uuid_t::create()};

model::topic_id_partition make_tidp(const model::topic_id& tid, int partition) {
    return {tid, model::partition_id(partition)};
}

l1::footer::partition make_segment(size_t pos, size_t length) {
    l1::footer::partition p;
    p.file_position = pos;
    p.length = length;
    return p;
}

} // namespace

TEST(PartitionPrefetchHintTest, SinglePartitionSegmentSeekInside) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(1000, 8 * 1024 * 1024));

    auto hint = compute_partition_prefetch_hint(
      footer, tidp, /*seek=*/1500, /*max=*/16 * 1024 * 1024);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->segment_position, 1000);
    EXPECT_EQ(hint->segment_size, 8 * 1024 * 1024);
}

TEST(PartitionPrefetchHintTest, MaxBytesZeroDisables) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(0, 1024));
    auto hint = compute_partition_prefetch_hint(footer, tidp, 100, 0);
    EXPECT_FALSE(hint.has_value());
}

TEST(PartitionPrefetchHintTest, PartitionNotInFooter) {
    l1::footer footer;
    auto other = make_tidp(topic_id_b, 0);
    footer.partitions.emplace(other, make_segment(0, 1024));
    auto absent = make_tidp(topic_id_a, 0);
    auto hint = compute_partition_prefetch_hint(footer, absent, 100, 1024);
    EXPECT_FALSE(hint.has_value());
}

TEST(PartitionPrefetchHintTest, SeekOutsideAnySegment) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(0, 1024));
    footer.partitions.emplace(tidp, make_segment(5000, 1024));

    // Seek between the two segments.
    auto hint = compute_partition_prefetch_hint(footer, tidp, 3000, 16 * 1024);
    EXPECT_FALSE(hint.has_value());
}

TEST(PartitionPrefetchHintTest, MultiSegmentPicksContainingOne) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(0, 1024));
    footer.partitions.emplace(tidp, make_segment(5000, 2048));

    auto hint = compute_partition_prefetch_hint(footer, tidp, 5500, 16 * 1024);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->segment_position, 5000);
    EXPECT_EQ(hint->segment_size, 2048);
}

TEST(PartitionPrefetchHintTest, SegmentLargerThanMaxBytesSkipsPrefetch) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(0, 64 * 1024 * 1024));

    // Capping would produce a truncated prefetch file shorter than the
    // consumer's required byte range (which extends up to
    // partition.length per seek_res.length), causing silent short-reads
    // at the consumer. The hint computation skips prefetch entirely
    // for oversized partitions.
    auto hint = compute_partition_prefetch_hint(
      footer, tidp, 100, 16 * 1024 * 1024);
    EXPECT_FALSE(hint.has_value());
}

TEST(PartitionPrefetchHintTest, SeekAtSegmentStart) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(1000, 1024));
    auto hint = compute_partition_prefetch_hint(footer, tidp, 1000, 4096);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->segment_position, 1000);
}

TEST(PartitionPrefetchHintTest, SeekAtSegmentEndExcluded) {
    l1::footer footer;
    auto tidp = make_tidp(topic_id_a, 0);
    footer.partitions.emplace(tidp, make_segment(1000, 1024));
    // seek_position must be < pos + length; exactly at end is excluded.
    auto hint = compute_partition_prefetch_hint(footer, tidp, 2024, 4096);
    EXPECT_FALSE(hint.has_value());
}
