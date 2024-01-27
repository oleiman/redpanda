/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/logging/tests/utils.h"

#include "bytes/streambuf.h"
#include "json/istreamwrapper.h"

#include <istream>
#include <random>

namespace transform::logging::testing {

json::Document parse_json(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

std::string get_message_body(iobuf msg) {
    auto doc = parse_json(std::move(msg));
    return {doc["body"].GetString()};
}

model::transform_name random_transform_name(size_t len) {
    static const std::string chrs{
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"};
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dist(
      0, static_cast<int>(chrs.size() - 1));

    std::string rand_str{};
    rand_str.reserve(len);

    for (size_t i = 0; i < len; ++i) {
        rand_str.push_back(chrs[dist(gen)]);
    }

    return model::transform_name{std::move(rand_str)};
}

} // namespace transform::logging::testing
