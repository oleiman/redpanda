/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io.h"

#include "base/vassert.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/client.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/defer.hh>

#include <memory>
#include <optional>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {

class staging_file_impl : public staging_file {
public:
    explicit staging_file_impl(std::filesystem::path path)
      : _path(std::move(path)) {}

    ss::future<size_t> size() override { return ss::file_size(_path.native()); }
    ss::future<ss::output_stream<char>> output_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(),
          ss::open_flags::rw | ss::open_flags::truncate
            | ss::open_flags::create);
        ss::file_output_stream_options options{};
        // The read buffer size also makes sense as the write buffer here
        // (default 128KiB).
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        // Defaults to 1, which is reasonable for write-behind as well.
        options.write_behind
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return co_await ss::make_file_output_stream(
          std::move(file), std::move(options));
    }
    ss::future<> remove() override { return ss::remove_file(_path.native()); }
    ss::future<ss::input_stream<char>> input_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(), ss::open_flags::ro);
        ss::file_input_stream_options options{};
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        options.read_ahead
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return ss::make_file_input_stream(
          std::move(file), std::move(options));
    }

private:
    std::filesystem::path _path;
};

// TODO: deduplicate, expose from cloud storage
struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s)
      : _st(std::move(s)) {}

    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(_st, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (_st.has_value()) {
            return _st->close().then([this] { _st = std::nullopt; });
        }
        return ss::now();
    }
    std::optional<ss::input_stream<char>> _st;
};

} // namespace

file_io::file_io(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::cache* cache)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _staging_dir(std::move(staging_dir))
  , _cache(cache) {}

ss::future<> file_io::stop() {
    _background_abort.request_abort();
    co_await _background_gate.close();
}

ss::future<std::expected<std::unique_ptr<staging_file>, io::errc>>
file_io::create_tmp_file() {
    co_return std::make_unique<staging_file_impl>(
      _staging_dir / fmt::format("{}.tmp", uuid_t::create()));
}

ss::future<std::expected<void, io::errc>>
file_io::put_object(object_id oid, staging_file* file, ss::abort_source* as) {
    auto file_size = co_await file->size();
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->upload_stream(
          cloud_io::transfer_details{
            .bucket = _bucket,
            .key = object_path_factory::level_one_path(oid),
            .parent_rtc = root,
          },
          file_size,
          [this, file]() {
              return io::read_file(file).then(
                [](ss::input_stream<char> stream)
                  -> std::unique_ptr<stream_provider> {
                    return std::make_unique<one_time_stream_provider>(
                      std::move(stream));
                });
          },
          las,
          "l1_file_upload",
          std::nullopt));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error uploading file: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        // TODO(cloud_topics): Consider preemptively putting the object in the
        // cache
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<uint64_t> file_io::save_to_cache(
  ss::input_stream<char> stream,
  cloud_io::space_reservation_guard* reservation,
  std::filesystem::path cache_key,
  uint64_t content_length) {
    co_await _cache->put(std::move(cache_key), stream, *reservation);
    co_return content_length;
}

ss::future<> file_io::download_partition_segment(
  std::filesystem::path prefetch_key,
  object_id id,
  size_t segment_position,
  size_t segment_size) {
    // 30s timeout (vs read_object's 10s) — partition segments are up
    // to ~16 MiB by default, larger than typical byte-range slices,
    // so allow more time for the transfer plus retry against
    // transient S3 errors. Also bounds the reserve_space wait below
    // so a stuck cache can't pin a background fiber past shutdown.
    static constexpr auto timeout = 30s;
    static constexpr auto backoff = 100ms;

    // Per-fiber retry chain rooted in the background abort source so
    // shutdown interrupts in-flight downloads.
    retry_chain_node root(
      _background_abort, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[this] {
        return _background_abort.abort_requested()
                 ? std::make_optional("shutdown")
                 : std::nullopt;
    }};

    // failure_errc is captured by cleanup so the promise resolves
    // even if we co_return early before the success arm.
    std::optional<io::errc> failure_errc = io::errc::file_io_error;
    auto cleanup = ss::defer([this, prefetch_key, &failure_errc]() {
        auto it = _inflight_prefetches.find(prefetch_key);
        if (it == _inflight_prefetches.end()) {
            return;
        }
        if (!it->second.available()) {
            it->second.set_value(failure_errc);
        }
        _inflight_prefetches.erase(it);
    });

    // Reserve cache space for the partition segment. Pass an explicit
    // deadline so the call is bounded: without it, the 2-arg
    // reserve_space overload waits on _block_puts_cond.wait() forever
    // when the cache is in block-puts state, which would hang
    // _background_gate.close() during shutdown.
    auto reservation_fut
      = co_await ss::coroutine::as_future<cloud_io::space_reservation_guard>(
        _cache->reserve_space(
          segment_size, 1, ss::lowres_clock::now() + timeout));
    if (reservation_fut.failed()) {
        auto ex = reservation_fut.get_exception();
        vlog(
          cd_log.warn,
          "Prefetch reservation failed for {}: {}",
          prefetch_key.native(),
          ex);
        _probe.register_prefetch_reservation_failure();
        failure_errc = io::errc::file_io_error;
        co_return;
    }

    cloud_io::try_consume_stream consumer =
      [this, r = reservation_fut.get(), prefetch_key](
        uint64_t content_length, ss::input_stream<char> stream) mutable {
          return save_to_cache(
            std::move(stream), &r, prefetch_key, content_length);
      };

    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::download_result>(
        _remote->download_stream(
          cloud_io::transfer_details{
            .bucket = _bucket,
            .key = object_path_factory::level_one_path(id),
            .parent_rtc = root,
          },
          consumer,
          "l1_partition_segment_prefetch",
          /*acquire_hydration_units=*/true,
          cloud_storage_clients::http_byte_range{
            segment_position, segment_position + segment_size - 1}));

    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(
          cd_log.warn,
          "Prefetch download failed for {}: {}",
          prefetch_key.native(),
          ex);
        _probe.register_prefetch_download_failure();
        failure_errc = io::errc::cloud_op_error;
        co_return;
    }

    switch (result_fut.get()) {
    case cloud_io::download_result::success:
        // Resolve in-flight with success so waiters retry cache lookup
        // (which now hits via get_stream_range).
        if (
          auto it = _inflight_prefetches.find(prefetch_key);
          it != _inflight_prefetches.end()) {
            it->second.set_value(std::nullopt);
        }
        failure_errc = std::nullopt; // cleanup is a no-op on success
        co_return;
    case cloud_io::download_result::notfound:
        _probe.register_prefetch_download_failure();
        failure_errc = io::errc::cloud_missing_object;
        co_return;
    case cloud_io::download_result::timedout:
        _probe.register_prefetch_download_failure();
        failure_errc = io::errc::cloud_op_timeout;
        co_return;
    case cloud_io::download_result::failed:
        _probe.register_prefetch_download_failure();
        failure_errc = io::errc::cloud_op_error;
        co_return;
    }
    std::unreachable();
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
file_io::read_object(object_extent extent, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};

    std::filesystem::path byte_range_key = fmt::format(
      "l1_{}_position_{}_size_{}.partial",
      extent.id,
      extent.position,
      extent.size);

    std::optional<std::filesystem::path> prefetch_key;
    if (extent.prefetch_hint.has_value()) {
        prefetch_key = fmt::format(
          "l1_{}_prefetch_{}_size_{}.partial",
          extent.id,
          extent.prefetch_hint->segment_position,
          extent.prefetch_hint->segment_size);
    }

    // compute_partition_prefetch_hint already returns nullopt when
    // cloud_topics_l1_partition_prefetch_max_bytes is 0 (and when the
    // partition's data in this L1 object exceeds the cap), so the
    // presence of a hint is sufficient to gate the prefetch path.
    bool prefetch_enabled = prefetch_key.has_value();

    while (true) {
        // Step 2: prefetch cache hit (broader coverage).
        if (prefetch_enabled) {
            auto stream_fut = co_await ss::coroutine::as_future<
              std::optional<cloud_io::cache_item_stream>>(
              _cache->get_stream_range(
                *prefetch_key,
                extent.position - extent.prefetch_hint->segment_position,
                extent.size,
                config::shard_local_cfg().storage_read_buffer_size(),
                config::shard_local_cfg().storage_read_readahead_count()));
            if (stream_fut.failed()) {
                auto ex = stream_fut.get_exception();
                vlog(
                  cd_log.warn,
                  "Error reading prefetch cache for {}: {}",
                  extent,
                  ex);
                // Fall through to byte-range path; non-fatal here.
            } else if (auto stream = stream_fut.get(); stream) {
                _probe.register_prefetch_cache_hit();
                co_return std::move(stream->body);
            }
        }

        // Step 3: byte-range cache hit (existing fast path).
        auto stream_fut = co_await ss::coroutine::as_future<
          std::optional<cloud_io::cache_item_stream>>(_cache->get_stream(
          byte_range_key,
          config::shard_local_cfg().storage_read_buffer_size(),
          config::shard_local_cfg().storage_read_readahead_count()));
        if (stream_fut.failed()) {
            auto ex = stream_fut.get_exception();
            vlog(
              cd_log.warn, "Error reading from cache for {}: {}", extent, ex);
            co_return std::unexpected(io::errc::file_io_error);
        }
        auto stream = stream_fut.get();
        if (stream) {
            co_return std::move(stream->body);
        }

        // Step 4: in-flight prefetch — waiter path.
        if (prefetch_enabled) {
            if (
              auto it = _inflight_prefetches.find(*prefetch_key);
              it != _inflight_prefetches.end()) {
                vlog(
                  cd_log.debug, "Waiting on in-flight prefetch for {}", extent);
                _probe.register_prefetch_waiter();
                // shared_promise::get_shared_future(abort_source&) returns
                // a future that resolves with the leader's value if already
                // available, or with ss::abort_requested_exception when
                // the abort fires before the value lands. Leaders only
                // resolve via set_value (never set_exception), so a failed
                // future here is unambiguously the abort path.
                auto fut = co_await ss::coroutine::as_future(
                  it->second.get_shared_future(*as));
                if (fut.failed()) {
                    fut.ignore_ready_future();
                    _probe.register_dedup_waiter_abort();
                    co_return std::unexpected(io::errc::cloud_op_timeout);
                }
                auto result = fut.get();
                if (result.has_value()) {
                    // Prefetch leader failed. The prefetch is best-effort
                    // (it only adds K-fanout coverage), so we fall through
                    // to the byte-range path which is required for the
                    // caller. The byte-range cache may have been populated
                    // by a separate caller in the meantime, or we'll
                    // cold-miss and become byte-range leader ourselves.
                    continue;
                }
                // Leader succeeded; retry the cache lookups.
                continue;
            }
        }

        // Step 5: in-flight byte-range — waiter path.
        if (
          auto it = _inflight_downloads.find(byte_range_key);
          it != _inflight_downloads.end()) {
            vlog(
              cd_log.debug,
              "Merging L1 read for {} into in-flight download",
              extent);
            _probe.register_dedup_waiter();
            auto fut = co_await ss::coroutine::as_future(
              it->second.get_shared_future(*as));
            if (fut.failed()) {
                fut.ignore_ready_future();
                _probe.register_dedup_waiter_abort();
                co_return std::unexpected(io::errc::cloud_op_timeout);
            }
            auto result = fut.get();
            if (result.has_value()) {
                co_return std::unexpected(*result);
            }
            continue;
        }

        // Step 6: cold miss — leader path.
        auto [insert_it, inserted] = _inflight_downloads.emplace(
          byte_range_key, ss::shared_promise<std::optional<io::errc>>{});
        vassert(
          inserted,
          "concurrent insert into _inflight_downloads for {}",
          byte_range_key.native());
        _probe.register_dedup_leader();
        // Default to a real failure errc so that if we exit before
        // reaching the download (e.g. the prefetch spawn below throws,
        // unwinding past the byte-range path), the cleanup defer
        // resolves waiters with a failure rather than misreporting
        // success via the prior std::nullopt default.
        std::optional<io::errc> failure_errc = io::errc::file_io_error;
        auto cleanup = ss::defer([this, byte_range_key, &failure_errc]() {
            auto it = _inflight_downloads.find(byte_range_key);
            if (it == _inflight_downloads.end()) {
                return;
            }
            if (!it->second.available()) {
                it->second.set_value(failure_errc);
            }
            _inflight_downloads.erase(it);
        });

        // Spawn the prefetch in background if enabled and not already
        // in-flight or cached. ssx::spawn_with_gate throws
        // gate_closed_exception synchronously if _background_gate is
        // closed (in-flight shutdown); wrap in try/catch so the
        // _inflight_prefetches entry doesn't leak past unwind.
        if (prefetch_enabled && !_inflight_prefetches.contains(*prefetch_key)) {
            auto [pf_it, pf_inserted] = _inflight_prefetches.emplace(
              *prefetch_key, ss::shared_promise<std::optional<io::errc>>{});
            vassert(
              pf_inserted,
              "concurrent insert into _inflight_prefetches for {}",
              prefetch_key->native());
            _probe.register_prefetch_leader();
            try {
                ssx::spawn_with_gate(
                  _background_gate,
                  [this,
                   key = *prefetch_key,
                   id = extent.id,
                   seg_pos = extent.prefetch_hint->segment_position,
                   seg_size = extent.prefetch_hint->segment_size](
                    this auto) -> ss::future<> {
                      co_await download_partition_segment(
                        std::move(key), id, seg_pos, seg_size);
                  });
            } catch (...) {
                // Spawn never made it to the background fiber (the
                // gate is closed). Resolve waiters with a failure
                // and remove the entry; nobody else owns it.
                vlog(
                  cd_log.warn,
                  "Failed to spawn prefetch for {}: {}",
                  prefetch_key->native(),
                  std::current_exception());
                pf_it->second.set_value(io::errc::file_io_error);
                _inflight_prefetches.erase(pf_it);
            }
        }

        // Fire the byte-range download (existing path, unchanged).
        auto reservation_fut = co_await ss::coroutine::as_future<
          cloud_io::space_reservation_guard>(
          _cache->reserve_space(extent.size, 1));
        if (reservation_fut.failed()) {
            auto ex = reservation_fut.get_exception();
            vlog(
              cd_log.warn,
              "Error reserving cache space for download of {}: {}",
              extent,
              ex);
            failure_errc = io::errc::file_io_error;
            co_return std::unexpected(io::errc::file_io_error);
        }
        cloud_io::try_consume_stream consumer =
          [this, r = reservation_fut.get(), &byte_range_key](
            uint64_t content_length, ss::input_stream<char> stream) mutable {
              return save_to_cache(
                std::move(stream), &r, byte_range_key, content_length);
          };
        auto result_fut
          = co_await ss::coroutine::as_future<cloud_io::download_result>(
            _remote->download_stream(
              cloud_io::transfer_details{
                .bucket = _bucket,
                .key = object_path_factory::level_one_path(extent.id),
                .parent_rtc = root,
              },
              consumer,
              "l1_file_download",
              /*acquire_hydration_units=*/true,
              cloud_storage_clients::http_byte_range{
                extent.position, extent.position + extent.size - 1}));
        if (result_fut.failed()) {
            auto ex = result_fut.get_exception();
            vlog(cd_log.warn, "Error downloading object {}: {}", extent, ex);
            failure_errc = io::errc::cloud_op_error;
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        switch (result_fut.get()) {
        case cloud_io::download_result::success:
            if (
              auto it = _inflight_downloads.find(byte_range_key);
              it != _inflight_downloads.end()) {
                it->second.set_value(std::nullopt);
            }
            continue;
        case cloud_io::download_result::notfound:
            failure_errc = io::errc::cloud_missing_object;
            co_return std::unexpected(io::errc::cloud_missing_object);
        case cloud_io::download_result::timedout:
            failure_errc = io::errc::cloud_op_timeout;
            co_return std::unexpected(io::errc::cloud_op_timeout);
        case cloud_io::download_result::failed:
            failure_errc = io::errc::cloud_op_error;
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        std::unreachable();
    }
}

ss::future<std::expected<void, io::errc>>
file_io::delete_objects(chunked_vector<object_id> ids, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    chunked_vector<cloud_storage_clients::object_key> keys;
    for (const auto& id : ids) {
        keys.push_back(object_path_factory::level_one_path(id));
    }
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->delete_objects(
          _bucket, std::move(keys), root, [](size_t retry_count) {
              std::ignore = retry_count;
          }));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error deleting objects: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, io::errc>>
file_io::create_multipart_upload(
  object_id oid, size_t part_size, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    auto key = object_path_factory::level_one_path(oid);
    auto result_fut = co_await ss::coroutine::as_future(
      _remote->initiate_multipart_upload(_bucket, key, part_size, timeout));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error initiating multipart upload: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    auto result = result_fut.get();
    if (!result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to initiate multipart upload for {}: {}",
          oid,
          result.error());
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::move(result.value());
}

} // namespace cloud_topics::l1
