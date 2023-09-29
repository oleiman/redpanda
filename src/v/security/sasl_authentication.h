// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "bytes/bytes.h"
#include "outcome.h"
#include "security/acl.h"
#include "vassert.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <memory>
#include <string_view>

namespace security {

/*
 * Generic SASL mechanism interface.
 */
class sasl_mechanism {
public:
    virtual ~sasl_mechanism() = default;
    virtual bool complete() const = 0;
    virtual bool failed() const = 0;
    virtual const acl_principal& principal() const = 0;
    virtual ss::future<result<bytes>> authenticate(bytes) = 0;
    virtual std::optional<std::chrono::milliseconds>
    credential_expires_in_ms() const {
        return std::nullopt;
    }
};

/*
 * SASL server protocol manager.
 */
class sasl_server final {
    using clock_type = ss::lowres_clock;

public:
    enum class sasl_state {
        initial,
        handshake,
        authenticate,
        complete,
        failed,
    };

    explicit sasl_server(
      sasl_state state,
      std::chrono::milliseconds conn_max_reauth = std::chrono::milliseconds{0})
      : _state(state)
      , _conn_max_reauth_ms(conn_max_reauth) {}

    sasl_state state() const { return _state; }
    void set_state(sasl_state state) { _state = state; }

    bool complete() const { return _state == sasl_state::complete; }
    bool expired() const {
        return _conn_max_reauth_ms > std::chrono::milliseconds{0}
               && clock_type::now() > _session_expiry;
    }

    void set_expiry(std::optional<std::chrono::milliseconds> cred_expiry_ms) {
        auto offset = cred_expiry_ms
                        ? (std::min(_conn_max_reauth_ms, *cred_expiry_ms))
                        : _conn_max_reauth_ms;
        _session_expiry = clock_type::now() + offset;
    }

    int64_t session_lifetime_ms() const {
        using namespace std::chrono_literals;
        if (_conn_max_reauth_ms == 0ms) {
            return 0;
        }
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 _session_expiry - clock_type::now())
          .count();
    }

    bool has_mechanism() const { return bool(_mechanism); }
    sasl_mechanism& mechanism() { return *_mechanism; }

    ss::future<result<bytes>> authenticate(bytes data) {
        return _mechanism->authenticate(std::move(data));
    }

    void reset() {
        set_state(sasl_state::initial);
        _mechanism = nullptr;
    }

    void set_mechanism(std::unique_ptr<sasl_mechanism> m) {
        vassert(!_mechanism, "Cannot change mechanism");
        _mechanism = std::move(m);
    }

    const acl_principal& principal() const { return _mechanism->principal(); }
    std::optional<std::chrono::milliseconds> credential_expires_in_ms() const {
        return _mechanism->credential_expires_in_ms();
    }

    bool handshake_v0() const { return _handshake_v0; }
    void set_handshake_v0() { _handshake_v0 = true; }

private:
    sasl_state _state;
    std::unique_ptr<sasl_mechanism> _mechanism;
    bool _handshake_v0{false};
    std::chrono::milliseconds _conn_max_reauth_ms{0};
    clock_type::time_point _session_expiry{};
};

// inline because the function is pretty small and clang complains about
// duplicate symbol since sasl_authentication.h is included in several
// locations.
inline std::string_view sasl_state_to_str(sasl_server::sasl_state state) {
    switch (state) {
    case sasl_server::sasl_state::initial:
        return "initial";
    case sasl_server::sasl_state::handshake:
        return "handshake";
    case sasl_server::sasl_state::authenticate:
        return "authenticate";
    case sasl_server::sasl_state::complete:
        return "complete";
    case sasl_server::sasl_state::failed:
        return "failed";
    }
}

}; // namespace security
