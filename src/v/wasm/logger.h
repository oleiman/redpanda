// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"

#include <seastar/util/log.hh>

namespace wasm {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern ss::logger wasm_log;

class logger {
public:
    struct meta {
        ss::log_level level;
        std::string_view transform_name;
    };

    logger() = delete;
    explicit logger(ss::sstring name, ss::logger* log)
      : _name(std::move(name))
      , _log(log) {
        // TODO(oren): assert logger not null
    }
    virtual ~logger() = default;
    logger(const logger&) = delete;
    logger& operator=(const logger&) = delete;
    logger(logger&&) = delete;
    logger& operator=(logger&&) = delete;

    virtual void log(ss::log_level lvl, std::string_view message) noexcept {
        _log->log(lvl, message);
    }

    const ss::sstring& name() const { return _name; }
    ss::logger& get_logger() { return *_log; }

private:
    ss::sstring _name;
    ss::logger* _log;
};

} // namespace wasm
