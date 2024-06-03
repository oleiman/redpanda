/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recursive_directory_walker.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/logger.h"
#include "ssx/watchdog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <chrono>
#include <deque>
#include <string_view>
#include <vector>

namespace cloud_storage {

struct walk_accumulator {
    walk_accumulator(
      ss::sstring start_dir,
      const access_time_tracker& t,
      std::optional<recursive_directory_walker::filter_type> collect_filter)
      : tracker(t)
      , dirlist({std::move(start_dir)})
      , filter(std::move(collect_filter)) {}

    ss::future<> visit(ss::sstring const& target, ss::directory_entry entry) {
        auto entry_path = fmt::format("{}/{}", target, entry.name);
        if (entry.type && entry.type == ss::directory_entry_type::regular) {
            auto file_stats = co_await ss::file_stat(entry_path);

            vlog(
              cst_log.debug,
              "Regular file found {} ({})",
              entry_path,
              file_stats.size);

            auto last_access_timepoint = tracker.estimate_timestamp(entry_path)
                                           .value_or(file_stats.time_accessed);

            current_cache_size += static_cast<uint64_t>(file_stats.size);

            if (!filter || filter.value()(entry_path)) {
                files.push_back(
                  {last_access_timepoint,
                   (std::filesystem::path(target) / entry.name.data()).native(),
                   static_cast<uint64_t>(file_stats.size)});
            } else if (filter) {
                ++filtered_out_files;
            }
        } else if (
          entry.type && entry.type == ss::directory_entry_type::directory) {
            vlog(cst_log.debug, "Dir found {}", entry_path);
            dirlist.emplace_front(entry_path);
        }
    }

    bool empty() const { return dirlist.empty(); }

    ss::sstring pop() {
        auto r = dirlist.back();
        dirlist.pop_back();
        return r;
    }

    const access_time_tracker& tracker;
    std::deque<ss::sstring> dirlist;
    std::optional<recursive_directory_walker::filter_type> filter;
    fragmented_vector<file_list_item> files;
    uint64_t current_cache_size{0};
    size_t filtered_out_files{0};
};
} // namespace cloud_storage

namespace {
ss::future<> walker_process_directory(
  const ss::sstring& start_dir,
  ss::sstring target,
  cloud_storage::walk_accumulator& state,
  fragmented_vector<ss::sstring>& empty_dirs) {
    try {
        ss::file target_dir = co_await open_directory(target);

        bool seen_dentries = false;
        co_await target_dir
          .list_directory(
            [&state, &target, &seen_dentries](ss::directory_entry entry) {
                seen_dentries = true;
                return state.visit(target, std::move(entry));
            })
          .done()
          .finally([target_dir]() mutable { return target_dir.close(); });

        if (unlikely(!seen_dentries) && target != start_dir) {
            empty_dirs.push_back(target);
        }
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            // skip this directory, move to the ext one
        } else {
            throw;
        }
    }
}
} // namespace
namespace cloud_storage {

ss::future<walk_result> recursive_directory_walker::walk(
  ss::sstring start_dir,
  const access_time_tracker& tracker,
  std::optional<filter_type> collect_filter) {
    auto guard = _gate.hold();

    watchdog wd1m(std::chrono::seconds(60), [] {
        vlog(cst_log.info, "Directory walk is taking more than 1 min");
    });
    watchdog wd10m(std::chrono::seconds(600), [] {
        vlog(cst_log.warn, "Directory walk is taking more than 10 min");
    });

    // Object to accumulate data as we walk directories
    walk_accumulator state(start_dir, tracker, std::move(collect_filter));

    fragmented_vector<ss::sstring> empty_dirs;

    while (!state.empty()) {
        auto target = state.pop();
        vassert(
          std::string_view(target).starts_with(start_dir),
          "Looking at directory {}, which is outside of initial dir "
          "{}.",
          target,
          start_dir);

        co_await walker_process_directory(
          start_dir, std::move(target), state, empty_dirs);
    }

    co_return walk_result{
      .cache_size = state.current_cache_size,
      .filtered_out_files = state.filtered_out_files,
      .regular_files = std::move(state.files),
      .empty_dirs = std::move(empty_dirs)};
}

ss::future<> recursive_directory_walker::stop() {
    vlog(cst_log.debug, "Stopping recursive directory walker");
    return _gate.close();
}

} // namespace cloud_storage
