/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_io/tests/cache_test_fixture.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

// White-box fixture combining cloud_io::cache_test_fixture (real cache,
// no remote) with seastar_test (gtest + coroutine support). file_io is
// constructed with a null remote so the tests can exercise steps 2-5 of
// the hierarchical lookup without touching object storage. Step 6 (cold
// miss leader) is bench-validated, not exercised here.
class file_io_test_fixture
  : public cloud_io::cache_test_fixture
  , public seastar_test {
public:
    file_io_test_fixture() {
        _staging_dir = test_dir.get_path() / "staging";
        std::filesystem::create_directories(_staging_dir);
        _bucket = cloud_storage_clients::bucket_name("test-bucket");
        _file_io = std::make_unique<file_io>(
          _staging_dir, /*remote*/ nullptr, _bucket, &sharded_cache.local());
    }

    ss::future<> TearDownAsync() override {
        if (_file_io) {
            co_await _file_io->stop();
        }
    }

    // Friend access into file_io internals.
    auto& inflight_downloads() { return _file_io->_inflight_downloads; }
    auto& inflight_prefetches() { return _file_io->_inflight_prefetches; }
    file_io& io() { return *_file_io; }

    void put_byte_range(
      const object_id& id, size_t pos, size_t size, char fill_char) {
        auto key = std::filesystem::path(
          fmt::format("l1_{}_position_{}_size_{}.partial", id, pos, size));
        put_into_cache(create_data_string(fill_char, size), key);
    }

    // Async variant of put_byte_range — usable from a resumed coroutine
    // context where the synchronous .get()-based put_into_cache would
    // hit future_base::do_wait's seastar::thread assertion.
    ss::future<> put_byte_range_async(
      const object_id& id, size_t pos, size_t size, char fill_char) {
        auto key = std::filesystem::path(
          fmt::format("l1_{}_position_{}_size_{}.partial", id, pos, size));
        auto data_string = create_data_string(fill_char, size);
        iobuf buf;
        buf.append(data_string.data(), data_string.length());
        auto reservation = co_await sharded_cache.local().reserve_space(
          buf.size_bytes(), 1);
        auto input = make_iobuf_input_stream(std::move(buf));
        co_await sharded_cache.local().put(key, input, reservation);
    }

    void put_prefetch(
      const object_id& id, size_t seg_pos, size_t seg_size, char fill_char) {
        auto key = std::filesystem::path(
          fmt::format(
            "l1_{}_prefetch_{}_size_{}.partial", id, seg_pos, seg_size));
        put_into_cache(create_data_string(fill_char, seg_size), key);
    }

    static std::filesystem::path
    byte_range_key(const object_id& id, size_t pos, size_t size) {
        return std::filesystem::path(
          fmt::format("l1_{}_position_{}_size_{}.partial", id, pos, size));
    }

    static std::filesystem::path
    prefetch_key(const object_id& id, size_t seg_pos, size_t seg_size) {
        return std::filesystem::path(
          fmt::format(
            "l1_{}_prefetch_{}_size_{}.partial", id, seg_pos, seg_size));
    }

private:
    std::filesystem::path _staging_dir;
    cloud_storage_clients::bucket_name _bucket;
    std::unique_ptr<file_io> _file_io;
};

namespace {

// Read the first byte from the stream and close it. Helps assert which
// of two same-sized payloads we got.
ss::future<char>
read_first_byte_and_close(ss::input_stream<char> stream, size_t total_size) {
    auto buf = co_await read_iobuf_exactly(stream, total_size);
    co_await stream.close();
    iobuf::iterator_consumer cons{buf.cbegin(), buf.cend()};
    char first = 0;
    cons.consume_to(1, &first);
    co_return first;
}

} // namespace

TEST_F_CORO(file_io_test_fixture, Step2PrefetchCacheHit) {
    auto id = create_object_id();
    constexpr size_t seg_pos = 1000;
    constexpr size_t seg_size = 4096;
    put_prefetch(id, seg_pos, seg_size, 'A');

    object_extent extent{
      .id = id,
      .position = 2000,
      .size = 100,
      .prefetch_hint = partition_prefetch_hint{
        .segment_position = seg_pos,
        .segment_size = seg_size,
      },
    };
    ss::abort_source as;
    auto stream_result = co_await io().read_object(extent, &as);
    ASSERT_TRUE_CORO(stream_result.has_value());

    auto first = co_await read_first_byte_and_close(
      std::move(stream_result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'A');
}

TEST_F_CORO(file_io_test_fixture, Step3ByteRangeCacheHit) {
    auto id = create_object_id();
    constexpr size_t pos = 100;
    constexpr size_t size = 512;
    put_byte_range(id, pos, size, 'B');

    object_extent extent{
      .id = id,
      .position = pos,
      .size = size,
      .prefetch_hint = std::nullopt,
    };
    ss::abort_source as;
    auto stream_result = co_await io().read_object(extent, &as);
    ASSERT_TRUE_CORO(stream_result.has_value());

    auto first = co_await read_first_byte_and_close(
      std::move(stream_result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'B');
}

TEST_F_CORO(file_io_test_fixture, Step2WinsOverStep3) {
    auto id = create_object_id();
    constexpr size_t seg_size = 4096;
    constexpr size_t size = 256;
    // Both files present; prefetch fills with 'P', byte-range with 'R'.
    put_prefetch(id, /*seg_pos=*/0, seg_size, 'P');
    put_byte_range(id, /*pos=*/0, size, 'R');

    object_extent extent{
      .id = id,
      .position = 0,
      .size = size,
      .prefetch_hint = partition_prefetch_hint{
        .segment_position = 0,
        .segment_size = seg_size,
      },
    };
    ss::abort_source as;
    auto stream_result = co_await io().read_object(extent, &as);
    ASSERT_TRUE_CORO(stream_result.has_value());

    auto first = co_await read_first_byte_and_close(
      std::move(stream_result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'P');
}

TEST_F_CORO(file_io_test_fixture, Step4PrefetchWaiterResolvesSuccess) {
    auto id = create_object_id();
    constexpr size_t seg_size = 4096;
    constexpr size_t size = 256;
    auto pf_key = prefetch_key(id, /*seg_pos=*/0, seg_size);
    auto [it, inserted] = inflight_prefetches().emplace(
      pf_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    object_extent extent{
      .id = id,
      .position = 0,
      .size = size,
      .prefetch_hint = partition_prefetch_hint{
        .segment_position = 0,
        .segment_size = seg_size,
      },
    };
    ss::abort_source as;
    auto fut = io().read_object(extent, &as);

    // Populate the prefetch cache then resolve the waiter with success.
    // The waiter retries the lookup hierarchy; step 2 will now hit.
    put_prefetch(id, /*seg_pos=*/0, seg_size, 'W');
    co_await ss::sleep(5ms);
    it->second.set_value(std::nullopt);
    inflight_prefetches().erase(it);

    auto result = co_await std::move(fut);
    ASSERT_TRUE_CORO(result.has_value());
    auto first = co_await read_first_byte_and_close(
      std::move(result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'W');
}

TEST_F_CORO(
  file_io_test_fixture, Step4PrefetchWaiterResolvesErrorFallsThrough) {
    auto id = create_object_id();
    constexpr size_t seg_size = 4096;
    constexpr size_t size = 256;
    auto pf_key = prefetch_key(id, /*seg_pos=*/0, seg_size);
    auto [it, inserted] = inflight_prefetches().emplace(
      pf_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    // Crucially do NOT pre-populate byte-range here — that would make
    // step 3 hit before step 4 is ever reached. We need read_object to
    // suspend in step 4 first; only after suspension do we populate
    // byte-range and resolve the prefetch promise with an error. The
    // retry loop should then fall through to step 3 and serve 'F'.
    object_extent extent{
      .id = id,
      .position = 0,
      .size = size,
      .prefetch_hint = partition_prefetch_hint{
        .segment_position = 0,
        .segment_size = seg_size,
      },
    };
    ss::abort_source as;
    auto fut = io().read_object(extent, &as);
    // Yield so read_object reaches step 4 and suspends on the
    // prefetch promise. Use the async put helper — the synchronous
    // put_byte_range internally calls .get() which asserts when
    // invoked from a resumed coroutine context.
    co_await ss::sleep(5ms);
    co_await put_byte_range_async(id, /*pos=*/0, size, 'F');
    it->second.set_value(io::errc::cloud_op_error);
    inflight_prefetches().erase(it);

    auto result = co_await std::move(fut);
    ASSERT_TRUE_CORO(result.has_value());
    auto first = co_await read_first_byte_and_close(
      std::move(result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'F');
}

TEST_F_CORO(file_io_test_fixture, Step5ByteRangeWaiterResolvesSuccess) {
    auto id = create_object_id();
    constexpr size_t pos = 100;
    constexpr size_t size = 256;
    auto br_key = byte_range_key(id, pos, size);
    auto [it, inserted] = inflight_downloads().emplace(
      br_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    object_extent extent{
      .id = id,
      .position = pos,
      .size = size,
      .prefetch_hint = std::nullopt,
    };
    ss::abort_source as;
    auto fut = io().read_object(extent, &as);

    put_byte_range(id, pos, size, 'X');
    co_await ss::sleep(5ms);
    it->second.set_value(std::nullopt);
    inflight_downloads().erase(it);

    auto result = co_await std::move(fut);
    ASSERT_TRUE_CORO(result.has_value());
    auto first = co_await read_first_byte_and_close(
      std::move(result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'X');
}

TEST_F_CORO(file_io_test_fixture, WaiterAborted) {
    auto id = create_object_id();
    constexpr size_t seg_size = 4096;
    auto pf_key = prefetch_key(id, /*seg_pos=*/0, seg_size);
    auto [pf_it, pf_inserted] = inflight_prefetches().emplace(
      pf_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(pf_inserted);

    object_extent extent{
      .id = id,
      .position = 0,
      .size = 256,
      .prefetch_hint = partition_prefetch_hint{
        .segment_position = 0,
        .segment_size = seg_size,
      },
    };
    ss::abort_source as;
    auto fut = io().read_object(extent, &as);

    co_await ss::sleep(5ms);
    as.request_abort();

    auto result = co_await std::move(fut);
    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), io::errc::cloud_op_timeout);

    // Clean up so the entry doesn't outlive the fixture; the leader
    // (synthesized by the test) needs to resolve and be erased.
    pf_it->second.set_value(std::nullopt);
    inflight_prefetches().erase(pf_it);
}

} // namespace cloud_topics::l1
