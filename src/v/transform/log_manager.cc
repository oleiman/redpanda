#include "transform/log_manager.h"

#include "model/namespace.h"
#include "transform/io.h"

#include <seastar/core/smp.hh>

namespace transform {

// class log_sink : public sink {
// public:
//     log_sink(cluster::topic_table* topic_table, rpc::client* client)
//       : _topic_table(topic_table)
//       , _client(client) {}

//     // TODO(oren): this is all wrong. We'll want special methods on the rpc
//     // interface, which unfortunately doesn't really expose anything generic
//     for
//     // whatever reason. pretty tightly coupled to specific
//     ss::future<> write(ss::chunked_fifo<model::record_batch> batches)
//     override {
//         // [[maybe_unused]] model::partition_id partition
//         //   = compute_output_partition();
//         // auto ec = co_await _client->produce(
//         //   model::transform_log_internal_ntp.tp, std::move(batches));
//         // if (ec != cluster::errc::success) {
//         //     throw std::runtime_error(ss::format(
//         //       "failure to produce transform data: {}",
//         //       cluster::error_category().message(int(ec))));
//         // }
//         return ss::now();
//     }

//     // steps:
//     // 1. Find the transform log leader (ntp is fixed
//     // 2. If it's this node, use local_service in rpc::client to produce
//     // 3. Otherwise we need to send a produce request over the wire. there
//     are
//     //   specific methods to do this over in transform_rpc_client_protocol.

// private:
//     model::partition_id compute_output_partition() {
//         return model::partition_id{0};
//     }
//     [[maybe_unused]] cluster::topic_table* _topic_table;
//     [[maybe_unused]] rpc::client* _client;
// };

log_manager::log_manager(
  model::node_id self,
  rpc::client* rpc_client,
  cluster::topic_table* topic_table)
  : _self(self)
  , _rpc_client(rpc_client)
  , _topic_table(topic_table) {
    _flush_timer.set_callback(
      [this]() { ssx::spawn_with_gate(_gate, [this]() { return flush(); }); });
}

log_manager::~log_manager() = default;

ss::future<> log_manager::start() {
    _flush_timer.arm(std::chrono::milliseconds(500));
    return ss::now();
}
ss::future<> log_manager::stop() {
    _flush_timer.cancel();
    return ss::now();
}

ss::future<> log_manager::flush() {
    [[maybe_unused]] auto tot = 0ul;
    for (auto& [n, q] : _event_queue) {
        if (q.empty()) {
            continue;
        }
        ss::chunked_fifo<model::transform_log_event> ev{};
        while (!q.empty()) {
            ev.emplace_back(std::move(q.front()));
            q.pop_front();
        }
        auto num = ev.size();
        tot += num;
        auto res = co_await _rpc_client->append_transform_logs(
          model::transform_name{n}, std::move(ev), std::chrono::seconds{1});
        if (res.has_error() && res.error() != cluster::errc::success) {
            std::cerr << fmt::format("Failed to write {} log events\n", num);
        } else {
            std::cerr << fmt::format("Successfully wrote {} log events\n", num);
        }
    }
    // std::cerr << fmt::format("Processed {} log events\n", tot);
    _flush_timer.arm(std::chrono::milliseconds(5000));
    co_return;
}

void log_manager::enqueue_log(
  [[maybe_unused]] meta metadata, [[maybe_unused]] std::string_view message) {
    iobuf b;
    b.append(message.data(), message.size());
    // TODO(oren): better
    auto [it, res] = _event_queue.emplace(
      ss::sstring{
        metadata.transform_name.data(), metadata.transform_name.size()},
      ss::chunked_fifo<model::transform_log_event>{});
    it->second.emplace_back(
      _self,
      model::transform_log_event::clock_type::now(),
      metadata.level,
      std::move(b));
    // _event_queue[metadata.transform_name].emplace_back(
    //   _self, ss::lowres_system_clock::now(), metadata.level, std::move(b));
}

} // namespace transform
