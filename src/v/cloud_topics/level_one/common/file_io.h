/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_io_probe.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <seastar/core/shared_future.hh>

#include <optional>

namespace cloud_topics::l1 {

// The IO implementation that hides caching and other complexities of
// interacting with persistent storage of L1 objects.
//
// For writing this persists the file to the local disk, then writes it
// to object storage.
//
// Reads are cached locally on disk in the cloud cache before being returned.
class file_io : public io {
public:
    file_io(
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::cache* cache);
    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>>
    read_object(object_extent, ss::abort_source*) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override;

    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id, size_t part_size, ss::abort_source*) override;

private:
    ss::future<uint64_t> save_to_cache(
      ss::input_stream<char>,
      cloud_io::space_reservation_guard*,
      std::filesystem::path,
      uint64_t content_length);

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    std::filesystem::path _staging_dir;
    cloud_io::cache* _cache;

    // In-flight L1 download dedup. When two consumers on the same
    // shard miss the cache on the same (oid, position, size) extent
    // within a tiny window, the first one becomes the leader and the
    // others wait on the shared_promise instead of triggering
    // duplicate S3 GETs + cache writes. The promise resolves with
    // std::nullopt on success (waiters retry the cache lookup) or with
    // an errc on failure (waiters propagate the same error). Mirrors
    // the L0 read_merge pattern at the L1 cache-miss boundary; the
    // 2026-05-07 IO analysis doc previously flagged "L1 has no
    // equivalent" to L0's cross-consumer coalescing, and 2026-05-12
    // K=3 Mode A isolation bench data confirmed the K-fanout write
    // amplification this addresses.
    chunked_hash_map<
      std::filesystem::path,
      ss::shared_promise<std::optional<errc>>>
      _inflight_downloads;

    file_io_probe _probe;
};

} // namespace cloud_topics::l1
