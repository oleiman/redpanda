#include "security/audit/rpc_sink.h"

namespace security::audit {

ss::future<> rpc_sink::start() { return ss::make_ready_future(); }
ss::future<> rpc_sink::stop() { return ss::make_ready_future(); }
ss::future<> rpc_sink::produce([[maybe_unused]] chunked_vector<audit_msg>) {
    return ss::make_ready_future();
}
void rpc_sink::toggle([[maybe_unused]] bool enabled) {}

} // namespace security::audit
