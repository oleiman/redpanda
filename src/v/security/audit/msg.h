#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "security/audit/schemas/schemas.h"
#include "ssx/semaphore.h"

#include <memory>

namespace security::audit {

class audit_msg {
public:
    /// Wrapper around an ocsf event pointer
    ///
    /// Main benefit is to tie the lifetime of semaphore units with the
    /// underlying ocsf event itself
    audit_msg(
      size_t hash_key,
      std::unique_ptr<ocsf_base_impl> msg,
      ssx::semaphore_units&& units)
      : _hash_key(hash_key)
      , _msg(std::move(msg))
      , _units(std::move(units)) {
        vassert(_msg != nullptr, "Audit record cannot be null");
    }

    size_t key() const { return _hash_key; }

    void increment(timestamp_t t) const { _msg->increment(t); }

    const std::unique_ptr<ocsf_base_impl>& ocsf_msg() const { return _msg; }

    std::unique_ptr<ocsf_base_impl> release() && {
        _units.return_all();
        return std::move(_msg);
    }

private:
    size_t _hash_key;
    std::unique_ptr<ocsf_base_impl> _msg;
    ssx::semaphore_units _units;
};
} // namespace security::audit
