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

#include "base/seastarx.h"
#include "cloud_io/group_id.h"
#include "cloud_io/policy_type.h"
#include "metrics/metrics.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>

#include <memory>
#include <optional>

namespace cloud_io {

class scheduler_policy;

/// \brief Admission scheduler for cloud_io operations.
///
/// Plain (non-sharded) class owned by cloud_storage_clients::
/// client_pool. Each pool shard owns one. Constructor picks a
/// policy by reading the cloud_io_scheduler_policy cluster property;
/// changing it requires a rolling restart.
class scheduler {
public:
    class permit;

    scheduler(policy_type, size_t capacity);
    scheduler(const scheduler&) = delete;
    scheduler& operator=(const scheduler&) = delete;
    scheduler(scheduler&&) = delete;
    scheduler& operator=(scheduler&&) = delete;
    ~scheduler() noexcept;

    /// Registers metrics. Must be called before first admit.
    ss::future<> start();

    /// Drains waiters, stops the policy.
    ss::future<> stop();

    /// Acquire admission for an op classified as g. Returns a RAII
    /// permit; release on destruction.
    /// \throws ss::abort_requested_exception if as fires while queued.
    [[nodiscard]] ss::future<permit> admit(group_id g, ss::abort_source& as);

    /// Non-blocking admit. nullopt if admit would queue.
    [[nodiscard]] std::optional<permit> try_admit(group_id g) noexcept;

    /// Observability — forwards to the active policy.
    size_t in_flight(group_id) const noexcept;
    size_t waiters(group_id) const noexcept;
    size_t available_slots() const noexcept;
    size_t total_capacity() const noexcept;
    bool has_waiters() const noexcept;

    /// Typed accessor to the underlying policy. nullptr if the
    /// active policy is not of type P. Used by tests and future
    /// admin RPC.
    template<typename P>
    P* policy_as() noexcept;

    /// Internal helper: policies call this to mint a local permit
    /// after a successful admit.
    [[nodiscard]] permit make_permit(group_id g) noexcept;

    /// Mint a remote-flavored permit for the borrower shard.
    /// The permit's destructor is a no-op; the lease deleter is
    /// responsible for calling release_remote(g) on the owning shard.
    [[nodiscard]] static permit
    make_remote_permit(group_id g, ss::shard_id remote_sid) noexcept;

    /// Internal helper: lease deleter (cross-shard borrow path)
    /// calls this on the peer shard to release a remote permit's
    /// slot. Public for accessibility from client_pool; not
    /// intended for direct external use.
    void release_remote(group_id g) noexcept;

private:
    void release_permit(group_id) noexcept;

    std::unique_ptr<scheduler_policy> _policy;
    bool _draining = false;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;

    friend class permit;
};

/// \brief RAII handle representing one admitted slot.
class scheduler::permit {
public:
    permit() = delete;
    permit(const permit&) = delete;
    permit& operator=(const permit&) = delete;
    permit(permit&&) noexcept;
    permit& operator=(permit&&) noexcept;
    ~permit() noexcept;

    /// Explicit early release (otherwise releases on destruction).
    void return_all() noexcept;

    /// Disown the local scheduler reference without releasing the
    /// reserved slot. Used by the cross-shard borrow path when handing
    /// off slot ownership to the lease deleter's release_remote call.
    void disown() noexcept { _shell = nullptr; }

    group_id group() const noexcept { return _group; }
    bool is_remote() const noexcept { return _remote_sid.has_value(); }
    ss::shard_id remote_sid() const noexcept { return *_remote_sid; }

private:
    friend class scheduler;

    permit(scheduler* shell, group_id g) noexcept
      : _shell(shell)
      , _group(g) {}
    permit(group_id g, ss::shard_id remote_sid) noexcept
      : _shell(nullptr)
      , _remote_sid(remote_sid)
      , _group(g) {}

    scheduler* _shell = nullptr;
    std::optional<ss::shard_id> _remote_sid;
    group_id _group = group_id::default_group;
};

/// Factory — defined in scheduler_factory.cc.
std::unique_ptr<scheduler_policy>
make_scheduler_policy(policy_type, scheduler& shell, size_t capacity);

} // namespace cloud_io

// scheduler_policy references scheduler::permit, which must be fully
// defined first. The template definition of policy_as<P> below also
// requires scheduler_policy to be complete for dynamic_cast.
#include "cloud_io/scheduler_policy.h"

namespace cloud_io {

template<typename P>
P* scheduler::policy_as() noexcept {
    return dynamic_cast<P*>(_policy.get());
}

} // namespace cloud_io
