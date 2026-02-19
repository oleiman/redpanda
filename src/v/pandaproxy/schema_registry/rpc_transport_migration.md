# Migrate Schema Registry to Internal RPC Transport

## Motivation

Schema registry uses `kafka::client::client` — a full Kafka protocol client with SASL, ephemeral credentials, and error mitigation — to produce and consume from the `_schemas` internal topic. This adds unnecessary complexity and auth overhead for what is purely internal communication between Redpanda nodes.

This change introduces an alternative transport using `kafka::data::rpc::client`, which routes internally via RPC. The approach mirrors the pattern established by the audit subsystem (`audit_use_rpc`): a runtime-selectable transport behind an abstract interface, controlled by a config flag and cluster version gate.

## What Changed

### Prerequisite: Extend Produce RPC to Return Base Offset

Schema registry's collision detection needs the offset where a produce landed. The existing `kafka::data::rpc::client::produce()` discarded this — the internal replication returned a `result<model::offset>`, but the RPC handler collapsed it into just an error code.

- Added `std::optional<model::offset> base_offset` to `kafka_topic_data_result` (serde v1, compat v0 for mixed-version safety)
- Propagated the offset through the RPC service handler instead of discarding it
- Added `produce_with_offset()` to the RPC client, sharing the same codepath as `produce()`

### Transport Abstraction

Created an abstract `transport` interface in `src/v/pandaproxy/schema_registry/transport.h` with three operations:

- `produce(batch) → produce_result` — write to `_schemas`, return base offset
- `get_high_watermark() → model::offset` — list offsets for `_schemas`
- `consume_range(start, end, consumer)` — fetch and process batches in a range

Two implementations:

- **`kafka_client_transport`** — wraps the existing `kafka::client::client` path. Extracts the produce/list-offsets/fetch logic that was previously inline in `seq_writer` and `service`.
- **`rpc_transport`** — wraps `kafka::data::rpc::client`. Includes retry with exponential backoff for transient errors (`not_leader`, `timeout`) since the RPC client's offset and consume APIs don't have built-in retries.

### Refactored seq_writer and service

Both `seq_writer` and `service` now take a `transport&` instead of `ss::sharded<kafka::client::client>&`. The three key callsites were migrated:

- `seq_writer::read_sync()` — `list_offsets` → `transport.get_high_watermark()`
- `seq_writer::wait_for()` — `make_client_fetch_batch_reader().consume()` → `transport.consume_range()`
- `seq_writer::produce_and_apply()` — `produce_record_batch()` → `transport.produce()`
- `service::fetch_internal_topic()` — same pattern as `wait_for`

### Feature Gate and Selection Logic

Added `schema_registry_use_rpc` config property (bool, default false, needs restart). In `api::start()`, the transport is selected based on:

1. Config flag enabled
2. RPC client available (passed from application wiring)
3. Cluster version ≥ v26.1.1 (ensures all nodes support the `base_offset` extension)

Per-shard transport instances are created and passed to the sequencer and service via `ss::sharded_parameter`.

### Integration Testing

Added `SchemaRegistryRpcTransportTest` — a ducktape test class that inherits all existing schema registry test methods and runs them with the RPC transport enabled.

## Bug Found During Development

The `consume_to_store` temporary object was used as a coroutine target:

```cpp
// BUG: temporary destroyed while coroutine still suspended
consume_to_store{_store, seq}(std::move(batch)).discard_result();
```

`consume_to_store::operator()` is a coroutine that captures `this`. The temporary is destroyed at the end of the expression, but the coroutine may still be suspended — classic use-after-free. Fixed by making the caller a coroutine with `consume_to_store` as a local variable in the coroutine frame:

```cpp
[&seq](model::record_batch batch) -> ss::future<> {
    consume_to_store c{seq._store, seq};
    co_await c(std::move(batch));
}
```

This was a latent bug in the original code that was never triggered because the old `reader.consume(consumer)` API took ownership of the consumer object.

## Files Changed

- **4 new files**: `transport.h`, `kafka_client_transport.h/.cc`, `rpc_transport.h/.cc`
- **18 modified**: across `kafka/data/rpc` (serde, service, client), `pandaproxy/schema_registry` (seq_writer, service, api, BUILD), `config`, application wiring, and tests

## Verification

- 24/24 schema registry unit tests pass
- 1/1 kafka data RPC unit tests pass
- Ducktape integration test passes with RPC transport enabled
- Full `redpanda` binary builds cleanly
