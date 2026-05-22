/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"

namespace cloud_io {

class min_share_policy;

class min_share_policy_probe {
public:
    explicit min_share_policy_probe(const min_share_policy& policy);

    min_share_policy_probe(const min_share_policy_probe&) = delete;
    min_share_policy_probe& operator=(const min_share_policy_probe&) = delete;
    min_share_policy_probe(min_share_policy_probe&&) = delete;
    min_share_policy_probe& operator=(min_share_policy_probe&&) = delete;
    ~min_share_policy_probe() = default;

private:
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_io
