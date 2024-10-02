#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "container/fragmented_vector.h"
#include "security/audit/api.h"
#include "security/audit/fwd.h"
#include "security/audit/msg.h"
#include "ssx/future-util.h"

namespace security::audit {

class rpc_sink : public sink {
    ss::future<> start() override;
    ss::future<> stop() override;
    ss::future<> produce(chunked_vector<audit_msg>) override;
    void toggle(bool enabled) override;
};

} // namespace security::audit
