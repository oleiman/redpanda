/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_future.hh>

#include <filesystem>
#include <optional>
#include <variant>

namespace cloud_topics::l1 {

/// Per-shard single-flight coordinator for in-flight L1 downloads.
///
/// When two reads on the same shard miss the cloud cache on the same
/// extent, only one should trigger a download. Subsequent reads merge
/// onto a shared future and wait for the leader to publish an outcome.
///
/// The map size is an opportunistic deduplication bound, not a
/// concurrency control: the cloud_io::scheduler + client pool below
/// are the real concurrency throttles. Past `max_entries`, callers
/// receive `at_capacity` and proceed with an uncoordinated download.
/// In that regime a duplicate S3 GET is possible but correctness is
/// preserved (mirrors the pre-coordinator behavior).
class inflight_download_map {
public:
    /// Promise/future payload: nullopt on success (merged caller can
    /// expect a warm cache on retry), otherwise the errc to propagate.
    using outcome = std::optional<io::errc>;
    /// Future a merging caller awaits. Named for the role rather than
    /// the type (seastar's `shared_promise<T>::get_shared_future` in
    /// fact returns `future<T>`, not `shared_future<T>`).
    using merge_future = ss::future<outcome>;

    /// RAII handle for the leader of an in-flight download.
    ///
    /// Destruction publishes the captured outcome to merged callers
    /// and erases the map entry. The outcome defaults to
    /// `errc::file_io_error` so a leader that omits `resolve()`
    /// surfaces failure rather than spurious success.
    class leader_guard {
    public:
        leader_guard() = default;
        ~leader_guard() noexcept;

        leader_guard(leader_guard&&) noexcept;
        leader_guard& operator=(leader_guard&&) noexcept;

        leader_guard(const leader_guard&) = delete;
        leader_guard& operator=(const leader_guard&) = delete;

        /// Override the outcome the dtor will publish.
        void resolve(outcome) noexcept;

    private:
        friend class inflight_download_map;
        leader_guard(
          inflight_download_map* map, std::filesystem::path key) noexcept;

        inflight_download_map* _map{nullptr};
        std::filesystem::path _key;
        outcome _outcome{io::errc::file_io_error};
    };

    /// Sentinel returned by `join_or_lead` when the map is full.
    struct at_capacity_t {};
    static constexpr at_capacity_t at_capacity{};

    using join_result
      = std::variant<leader_guard, merge_future, at_capacity_t>;

    /// Default bound on concurrent distinct extents tracked per shard.
    /// Comfortably above realistic peaks (~1000 cf waiters seen in the
    /// 1000-partition bench); see the timeout-cascade addendum for the
    /// max observed wait population.
    static constexpr size_t default_max_entries = 4096;

    explicit inflight_download_map(
      size_t max_entries = default_max_entries) noexcept
      : _max_entries(max_entries) {}

    /// Atomic find-or-insert for `key`. May NOT be split by a co_await.
    ///
    /// `as` is wired into the abort path of the returned shared_future
    /// (merge case). For the leader case it is unused — leaders
    /// publish their outcome unconditionally via `leader_guard`.
    ///
    /// Returns one of:
    ///   - `leader_guard`: caller is the new leader for this extent;
    ///     must perform the download and call `resolve()` before the
    ///     guard is destroyed (the dtor publishes whatever outcome was
    ///     last set).
    ///   - `shared_future_t`: another fiber on this shard is already
    ///     downloading; await the future to observe its outcome.
    ///   - `at_capacity_t`: map is at `_max_entries`; the caller should
    ///     proceed with an uncoordinated download (no merge, no
    ///     cleanup). Correctness is preserved at the cost of a
    ///     duplicate S3 GET.
    join_result
    join_or_lead(std::filesystem::path key, ss::abort_source& as);

    size_t in_flight() const noexcept { return _entries.size(); }
    size_t capacity() const noexcept { return _max_entries; }

private:
    friend class leader_guard;
    /// Called by leader_guard's dtor; publishes the captured outcome
    /// and erases the entry. The entry must exist (the type owns the
    /// invariant: only `join_or_lead` inserts, only `release` removes).
    void
    release(const std::filesystem::path& key, outcome o) noexcept;

    chunked_hash_map<
      std::filesystem::path,
      ss::shared_promise<outcome>>
      _entries;
    size_t _max_entries;
};

} // namespace cloud_topics::l1
