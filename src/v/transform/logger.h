/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "transform/log_manager.h"
#include "wasm/logger.h"

#include <seastar/util/log.hh>

namespace transform {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern ss::logger tlog;

class logger final : public wasm::logger {
    using meta = wasm::logger::meta;

public:
    logger() = delete;
    explicit logger(ss::sstring name, ss::logger* lg, log_manager* mgr)
      : wasm::logger(std::move(name), lg)
      , _mgr(mgr) {}
    // TODO(oren): destructor, rule of 5
    // TODO(oren): assert mgr non-null

    void log(ss::log_level lvl, std::string_view message) noexcept override {
        get_logger().log(lvl, message);
        _mgr->enqueue_log(
          meta{.level = lvl, .transform_name = name()}, message);
    }

private:
    // TODO(oren): this should maybe be a shard_ptr
    log_manager* _mgr{nullptr};
};

} // namespace transform
