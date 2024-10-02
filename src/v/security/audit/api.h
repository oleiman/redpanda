#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "container/fragmented_vector.h"
#include "security/audit/fwd.h"

#include <seastar/core/future.hh>

#include <vector>

namespace security::audit {
class sink {
public:
    sink() = default;
    virtual ~sink() = default;

    /// Starts a kafka::client if none is allocated, backgrounds the work
    virtual ss::future<> start() = 0;

    /// Closes all gates, deallocates client returns when all has completed
    virtual ss::future<> stop() = 0;

    // TODO(oren): these record essences or whatever. the API here shouldn't be
    // kafka types. we shouldn't leak any kafka crap at this interface. virtual
    virtual ss::future<> produce(chunked_vector<audit_msg>) = 0;

    /// Allocates and connects, or deallocates and shuts down the audit client
    virtual void toggle(bool enabled) = 0;

private:
};

} // namespace security::audit
