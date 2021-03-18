// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/sync_group.h"

#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/print.hh>

namespace kafka {

void sync_group_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

template<>
ss::future<response_ptr> sync_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sync_group_request request;
    request.decode(ctx.reader(), ctx.header().version);
    auto resp = co_await ctx.groups().sync_group(std::move(request));
    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
