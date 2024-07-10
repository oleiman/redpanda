// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "js_vm.h"

#include <redpanda/transform_sdk.h>

#include <cstdlib>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <print>
#include <quickjs.h>
#include <utility>
#include <variant>
#include <vector>

namespace redpanda::js {

extern "C" {

// The following functions are how the user's code is injected into the Wasm
// binary. We leave this imported functions, and RPK will inject these symbols
// in `rpk transform build` after esbuild runs.

#ifdef __wasi__

#define WASM_IMPORT(mod, name)                                                 \
    __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(redpanda_js_provider, file_length)
uint32_t redpanda_js_source_file_length();

WASM_IMPORT(redpanda_js_provider, get_file)
void redpanda_js_source_get_file(char* dst);

#else

constexpr std::string_view test_source_file = R"(
import {onRecordWritten} from "@redpanda-data/transform-sdk";

onRecordWritten((event, writer) => {
  return writer.write(event.record);
});
)";

uint32_t redpanda_js_source_file_length() { return test_source_file.size(); }

void redpanda_js_source_get_file(char* dst) {
    std::memcpy(dst, test_source_file.data(), test_source_file.size());
}

#endif
}

/**
 * A custom JS class holding opaque bytes, easily convertable into common JS
 * types.
 */
class record_data {
public:
    explicit record_data(bytes_view data)
      : _data(data) {}
    record_data(const record_data&) = delete;
    record_data& operator=(const record_data&) = delete;
    record_data(record_data&&) = default;
    record_data& operator=(record_data&&) = default;

    std::expected<qjs::value, qjs::exception>
    text(JSContext* ctx, std::span<qjs::value> /*params*/) {
        return qjs::value::string(ctx, std::string_view{_data});
    }

    std::expected<qjs::value, qjs::exception>
    json(JSContext* ctx, std::span<qjs::value> /*params*/) {
        // TODO(rockwood): This is going to be the most common case, this needs
        // to be zero copy.
        std::string str;
        str.append_range(_data);
        return qjs::value::parse_json(ctx, str);
    }

    std::expected<qjs::value, qjs::exception>
    array(JSContext* ctx, std::span<qjs::value> /*params*/) {
        const std::span data_view = {// NOLINTNEXTLINE(*-const-cast)
                                     const_cast<uint8_t*>(_data.data()),
                                     _data.size()};
        auto array = qjs::value::uint8_array(ctx, data_view);
        // This memory isn't copied so we need to make sure we
        // invalid these arrays when the memory is gone.
        _arrays.push_back(array);
        return array;
    }

    ~record_data() {
        for (qjs::value array : _arrays) {
            std::ignore = array.detach_uint8_array();
        }
    }

    [[nodiscard]] bytes_view data() const { return _data; }

private:
    bytes_view _data;
    std::vector<qjs::value> _arrays;
};

/**
 * A JS class representation of redpanda::record_writer
 */
class record_writer {
public:
    explicit record_writer(
      redpanda::record_writer* writer,
      qjs::class_factory<record_data>* record_data)
      : _writer(writer)
      , _record_data(record_data) {}

    std::expected<qjs::value, qjs::exception>
    write(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to writer.write, got: {}, "
                "expected: 1",
                params.size())));
        }
        auto& param = params.front();
        if (!param.is_object()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx, "expected only object parameters to writer.write"));
        }
        auto key = extract_data(ctx, param.get_property("key"));
        if (!key.has_value()) [[unlikely]] {
            return std::unexpected(key.error());
        }
        auto value = extract_data(ctx, param.get_property("value"));
        if (!value.has_value()) [[unlikely]] {
            return std::unexpected(value.error());
        }
        auto headers = extract_headers(ctx, param.get_property("headers"));
        if (!headers.has_value()) [[unlikely]] {
            return std::unexpected(headers.error());
        }
        auto errc = _writer->write({
          .key = *key,
          .value = *value,
          .headers = *headers,
        });
        _strings.clear(); // free any allocated strings
        if (errc) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx, std::format("error writing record: {}", errc.message())));
        }
        return qjs::value::undefined(ctx);
    }

    std::expected<std::vector<redpanda::header_view>, qjs::exception>
    extract_headers(JSContext* ctx, const qjs::value& val) {
        std::vector<redpanda::header_view> headers;
        if (val.is_undefined() || val.is_null()) {
            return headers;
        }
        if (!val.is_array()) {
            return std::unexpected(
              qjs::exception::make(ctx, "unexpected type for headers"));
        }
        const size_t len = val.array_length();
        headers.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            auto elem = val.get_element(i);
            if (!elem.is_object()) [[unlikely]] {
                return std::unexpected(qjs::exception::make(
                  ctx, "expected only objects as headers"));
            }
            auto key = extract_data(ctx, elem.get_property("key"));
            if (!key.has_value()) [[unlikely]] {
                return std::unexpected(key.error());
            }
            auto value = extract_data(ctx, elem.get_property("value"));
            if (!value.has_value()) [[unlikely]] {
                return std::unexpected(value.error());
            }
            auto key_str = std::string_view{
              key->value_or(redpanda::bytes_view{})};
            headers.emplace_back(key_str, *value);
        }
        return headers;
    }

    std::expected<std::optional<redpanda::bytes_view>, qjs::exception>
    extract_data(JSContext* ctx, const qjs::value& val) {
        if (val.is_string()) {
            _strings.emplace_back(val.string_data());
            const auto& data = _strings.back();
            return redpanda::bytes_view(data.view());
        }
        if (val.is_uint8_array()) {
            auto data = val.uint8_array_data();
            if (data.data() == nullptr) [[unlikely]] {
                return std::unexpected(qjs::exception::current(ctx));
            }
            return redpanda::bytes_view(data.data(), data.size());
        }
        if (val.is_array_buffer()) {
            auto data = val.array_buffer_data();
            if (data.data() == nullptr) [[unlikely]] {
                return std::unexpected(qjs::exception::current(ctx));
            }
            return redpanda::bytes_view(data.data(), data.size());
        }
        if (val.is_null() || val.is_undefined()) {
            return std::nullopt;
        }
        record_data* record_data = _record_data->get_opaque(val);
        if (record_data == nullptr) [[unlikely]] {
            return std::unexpected(
              qjs::exception::make(ctx, "unexpected type for record data"));
        }
        return record_data->data();
    }

private:
    redpanda::record_writer* _writer;
    qjs::class_factory<record_data>* _record_data;
    std::vector<qjs::cstring> _strings; // a place to temporarily hold data.
};

qjs::class_factory<record_writer>
make_record_writer_class(qjs::runtime* runtime) {
    qjs::class_builder<record_writer> builder(
      runtime->context(), "RecordWriter");
    builder.method<&record_writer::write>("write");
    return builder.build();
}

qjs::class_factory<record_data> make_record_data_class(qjs::runtime* runtime) {
    qjs::class_builder<record_data> builder(runtime->context(), "RecordData");
    builder.method<&record_data::json>("json");
    builder.method<&record_data::text>("text");
    builder.method<&record_data::array>("array");
    return builder.build();
}

std::expected<qjs::value, qjs::exception> make_write_event(
  JSContext* ctx,
  const redpanda::write_event& evt,
  qjs::class_factory<record_data>* data_factory) {
    auto make_kv = [ctx, data_factory](
                     std::optional<redpanda::bytes_view> key,
                     std::optional<redpanda::bytes_view> val)
      -> std::expected<qjs::value, qjs::exception> {
        qjs::value obj = qjs::value::object(ctx);
        std::expected<std::monostate, qjs::exception> result;
        if (key) {
            result = obj.set_property(
              "key", data_factory->create(std::make_unique<record_data>(*key)));
        } else {
            result = obj.set_property("key", qjs::value::null(ctx));
        }
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
        if (val) {
            result = obj.set_property(
              "value",
              data_factory->create(std::make_unique<record_data>(*val)));
        } else {
            result = obj.set_property("value", qjs::value::null(ctx));
        }
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
        return obj;
    };
    auto maybe_record = make_kv(evt.record.key, evt.record.value);
    if (!maybe_record.has_value()) [[unlikely]] {
        return std::unexpected(maybe_record.error());
    }
    auto record = maybe_record.value();
    auto headers = qjs::value::array(ctx);
    for (const auto& header : evt.record.headers) {
        auto maybe_header = make_kv(
          redpanda::bytes_view(header.key), header.value);
        if (!maybe_header.has_value()) [[unlikely]] {
            return std::unexpected(maybe_header.error());
        }
        auto result = headers.push_back(*maybe_header);
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
    }
    auto result = record.set_property("headers", headers);
    if (!result.has_value()) [[unlikely]] {
        return std::unexpected(result.error());
    }
    qjs::value write_event = qjs::value::object(ctx);
    result = write_event.set_property("record", record);
    if (!result.has_value()) [[unlikely]] {
        return std::unexpected(result.error());
    }
    return write_event;
}

class schema {
public:
    explicit schema() = default;
    schema(const schema&) = delete;
    schema& operator=(const schema&) = delete;
    schema(schema&&) = default;
    schema& operator=(schema&&) = default;

    std::expected<qjs::value, qjs::exception>
    get_schema(JSContext* ctx, std::span<qjs::value> /*params*/) {
        return qjs::value::undefined(ctx);
    }

    std::expected<qjs::value, qjs::exception>
    get_format(JSContext* ctx, std::span<qjs::value> /*params*/) {
        return qjs::value::undefined(ctx);
    }

    std::expected<qjs::value, qjs::exception>
    get_references(JSContext* ctx, std::span<qjs::value> /*params*/) {
        return qjs::value::undefined(ctx);
    }
};

class schema_registry_client {
public:
    explicit schema_registry_client(
      std::unique_ptr<redpanda::sr::schema_registry_client> client)
      : _client(std::move(client)) {}
    schema_registry_client(const schema_registry_client&) = delete;
    schema_registry_client& operator=(const schema_registry_client&) = delete;
    schema_registry_client(schema_registry_client&&) = default;
    schema_registry_client& operator=(schema_registry_client&&) = default;

    std::expected<qjs::value, qjs::exception>
    lookup_schema_by_id(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookup_schema_by_id, got: {}, "
                "expected: 1",
                params.size())));
        }
        auto& param = params.front();
        if (!param.is_number()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected only integer parameters to "
              "SchemaRegistryClient.lookup_schema_by_id"));
        }
        redpanda::sr::schema_id id = param.as_number();

        auto result = _client->lookup_schema_by_id(id);
        if (!result.has_value()) {
            return std::unexpected(qjs::exception::make(
              ctx, std::format("Lookup failed: {}", result.error().message())));
        }

        return make_schema(ctx, result.value());
    }

    std::expected<qjs::value, qjs::exception>
    lookup_schema_by_version(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 2) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookup_schema_by_version, got: "
                "{}, expected: 2",
                params.size())));
        }
        auto& subject_param = params[0];
        auto& version_param = params[1];

        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for subject param in "
              "SchemaRegistryClient.lookup_schema_by_version"));
        }
        auto subject = subject_param.string_data();

        if (!version_param.is_number()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected integer for version param in "
              "SchemaRegistryClient.lookup_schema_by_version"));
        }

        auto version = static_cast<redpanda::sr::schema_version>(
          std::lround(version_param.as_number()));

        auto subj_schema = _client->lookup_schema_by_version(
          std::string{subject.view()}, version);

        if (!subj_schema.has_value()) {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "Error looking up schema: {}", subj_schema.error().message())));
        }

        return make_subject_schema(ctx, subj_schema.value());
    }

    std::expected<qjs::value, qjs::exception>
    lookup_latest_schema(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookup_latest_schema, got: {}, "
                "expected: 1",
                params.size())));
        }
        auto& subject_param = params[0];
        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for subject param in "
              "SchemaRegistryClient.lookup_latest_schema"));
        }
        auto subject = subject_param.string_data();

        auto subj_schema = _client->lookup_latest_schema(
          std::string{subject.view()});

        if (!subj_schema.has_value()) {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "Error looking up schema: {}", subj_schema.error().message())));
        }

        return make_subject_schema(ctx, subj_schema.value());
    }
    std::expected<qjs::value, qjs::exception>
    create_schema(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 2) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.create_schema, got: {}, expected: 2",
                params.size())));
        }
        auto& subject_param = params[0];
        auto& schema_param = params[1];

        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for subject param in "
              "SchemaRegistryClient.create_schema"));
        }
        auto subject = subject_param.string_data();

        if (!schema_param.is_object()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected an object for schema param in "
              "SchemaRegistryClient.create_schema"));
        }
        auto schema = extract_schema(ctx, schema_param);
        if (!schema.has_value()) {
            return std::unexpected(schema.error());
        }

        auto result = _client->create_schema(
          std::string{subject.view()}, std::move(schema).value());

        if (!result.has_value()) {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "Error creating schema: {}", result.error().message())));
        }

        return make_subject_schema(ctx, result.value());
    }

private:
    std::unique_ptr<redpanda::sr::schema_registry_client> _client;

    // TODO(oren): currently redpanda::schema takes strings rather than views...
    // it's possible that a zero copy interface where the string is staged here
    // in the wrapper would be more efficient. could be premature though.
    std::expected<redpanda::sr::schema, qjs::exception>
    extract_schema(JSContext* ctx, const qjs::value& val) {
        auto raw_schema = val.get_property("schema");
        if (!raw_schema.is_string()) {
            return std::unexpected(qjs::exception::make(
              ctx, "Malformed schema def: Expected string for 'schema'"));
        }
        auto format = val.get_property("format");
        // TODO(oren): needs to be some kinda enum or whatever?
        if (!format.is_number()) {
            return std::unexpected(qjs::exception::make(
              ctx, "Malformed schema def: Expected int for 'format'"));
        }
        std::vector<redpanda::sr::reference> native_refs;
        auto refs = val.get_property("references");
        if (!refs.is_null() && !refs.is_undefined()) {
            if (!refs.is_array()) {
                return std::unexpected(qjs::exception::make(
                  ctx,
                  "Malformed schema def: Expected array for 'references'"));
            }

            native_refs.reserve(refs.array_length());
            for (size_t i = 0; i < refs.array_length(); ++i) {
                auto ref = refs.get_element(i);
                if (!ref.is_object()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "Malformed schema def: Expected array "
                      "of objects for 'references'"));
                }
                auto name = ref.get_property("name");
                if (!name.is_string()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "Malformed schema def: Bad reference: "
                      "'name' should be a string"));
                }
                auto subj = ref.get_property("subject");
                if (!subj.is_string()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "Malformed schema def: Bad reference: "
                      "'subject' should be a string"));
                }
                auto vers = ref.get_property("version");
                if (!vers.is_number()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "Malformed schema def: Bad reference: "
                      "'version' should be a number"));
                }
                native_refs.emplace_back(
                  std::string{name.string_data().view()},
                  std::string{subj.string_data().view()},
                  static_cast<redpanda::sr::schema_version>(vers.as_number()));
            }
        }

        return redpanda::sr::schema{
          std::string{raw_schema.string_data().view()},
          static_cast<redpanda::sr::schema_format>(format.as_number()),
          std::move(native_refs)};
    }

    std::expected<qjs::value, qjs::exception> make_subject_schema(
      JSContext* ctx, const redpanda::sr::subject_schema subj_schema) {
        qjs::value obj = qjs::value::object(ctx);
        std::expected<std::monostate, qjs::exception> result;
        auto schema = make_schema(ctx, subj_schema.schema());
        if (!schema.has_value()) {
            return std::unexpected(schema.error());
        }
        result = obj.set_property("schema", schema.value());
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        auto subject = qjs::value::string(ctx, subj_schema.subject());
        if (!subject.has_value()) {
            return std::unexpected(subject.error());
        }
        result = obj.set_property("subject", subject.value());
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        result = obj.set_property(
          "version", qjs::value::integer(ctx, subj_schema.version()));
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        result = obj.set_property(
          "id", qjs::value::integer(ctx, subj_schema.id()));
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        return obj;
    }

    std::expected<qjs::value, qjs::exception>
    make_schema(JSContext* ctx, const redpanda::sr::schema& the_schema) {
        qjs::value obj = qjs::value::object(ctx);
        std::expected<std::monostate, qjs::exception> result;
        auto schema_v = qjs::value::string(ctx, the_schema.raw_schema());
        if (!schema_v.has_value()) {
            return std::unexpected(schema_v.error());
        }
        result = obj.set_property("schema", schema_v.value());
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
        // TODO(oren): need some type of enum or something for this
        result = obj.set_property(
          "format",
          qjs::value::integer(ctx, static_cast<int>(the_schema.format())));
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }

        auto references = qjs::value::array(ctx);
        for (const auto& ref : the_schema.references()) {
            qjs::value obj = qjs::value::object(ctx);
            auto name_v = qjs::value::string(ctx, ref.name);
            if (!name_v.has_value()) [[unlikely]] {
                return std::unexpected(name_v.error());
            }
            result = obj.set_property("name", name_v.value());
            if (!result.has_value()) [[unlikely]] {
                return std::unexpected(result.error());
            }
            auto subj_v = qjs::value::string(ctx, ref.subject);
            if (!subj_v.has_value()) [[unlikely]] {
                return std::unexpected(subj_v.error());
            }
            result = obj.set_property("subject", subj_v.value());
            if (!result.has_value()) [[unlikely]] {
                return std::unexpected(result.error());
            }
            result = obj.set_property(
              "version", qjs::value::integer(ctx, ref.version));
            if (!result.has_value()) [[unlikely]] {
                return std::unexpected(result.error());
            }
        }

        result = obj.set_property("references", references);
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }

        return obj;
    }
};

qjs::class_factory<schema_registry_client>
make_schema_registry_client_class(qjs::runtime* runtime) {
    qjs::class_builder<schema_registry_client> builder(
      runtime->context(), "SchemaRegistryClient");
    builder.method<&schema_registry_client::lookup_schema_by_id>(
      "lookupSchemaById");
    builder.method<&schema_registry_client::lookup_schema_by_version>(
      "lookupSchemaByVersion");
    builder.method<&schema_registry_client::lookup_latest_schema>(
      "lookupLatestSchema");
    builder.method<&schema_registry_client::create_schema>("createSchema");
    return builder.build();
}

std::expected<std::monostate, qjs::exception> initial_native_modules(
  qjs::runtime* runtime,
  qjs::value* user_callback,
  qjs::class_factory<schema_registry_client>* sr_client_factory) {
    auto mod = qjs::module_builder("@redpanda-data/transform-sdk");
    mod.add_function(
      "onRecordWritten",
      [user_callback](
        JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() != 1 || !args.front().is_function()) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                "invalid argument, onRecordWritten must take only a single "
                "function as an argument"));
          }
          return {std::exchange(*user_callback, std::move(args.front()))};
      });
    auto sr_mod = qjs::module_builder("@redpanda-data/transform-sdk/sr");
    sr_mod.add_function(
      "newClient",
      [&sr_client_factory](
        JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() > 0) [[unlikely]] {
              return std::unexpected(
                qjs::exception::make(ctx, "Unexpected arguments to newClient"));
          }
          return sr_client_factory->create(
            std::make_unique<schema_registry_client>(
              redpanda::sr::schema_registry_client::new_client()));
      });
    sr_mod.add_function(
      "decodeSchemaID",
      [](JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() != 1 && !args.front().is_uint8_array()) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                "invalid argument, decodeSchemaId expects a single Uint8Array "
                "argument"));
          }
          auto data = args.front().uint8_array_data();
          auto id = redpanda::sr::decode_schema_id(
            redpanda::bytes_view{data.data(), data.size()});
          if (!id.has_value()) {
              return std::unexpected(qjs::exception::make(
                ctx,
                std::format(
                  "Failed to decode schema ID: {}", id.error().message())));
          }
          return qjs::value::integer(ctx, id.value().first);
      });
    auto result = runtime->add_module(std::move(mod));
    if (!result.has_value()) {
        return result;
    }
    return runtime->add_module(std::move(sr_mod));
}

std::expected<std::monostate, qjs::exception>
compile_and_load(qjs::runtime* runtime) {
    std::string source;
    source.resize(redpanda_js_source_file_length(), 'a');
    redpanda_js_source_get_file(source.data());
    auto compile_result = runtime->compile(source);
    if (!compile_result.has_value()) [[unlikely]] {
        auto msg = qjs::value::string(
          runtime->context(),
          std::format(
            "unable to compile module: {}",
            compile_result.error().val.debug_string()));
        return std::unexpected(qjs::exception(msg.value()));
    }
    auto load_result = runtime->load(compile_result.value().raw());
    if (!load_result.has_value()) [[unlikely]] {
        auto msg = qjs::value::string(
          runtime->context(),
          std::format(
            "unable to load module: {}",
            load_result.error().val.debug_string()));
        if (!msg) {
            return std::unexpected(msg.error());
        }
        return std::unexpected(qjs::exception(msg.value()));
    }
    return {};
}

int run() {
    qjs::runtime runtime;
    auto result = runtime.create_builtins();
    if (!result) {
        std::println(
          stderr,
          "unable to install globals: {}",
          result.error().val.debug_string());
        return 1;
    }
    auto writer_factory = make_record_writer_class(&runtime);
    auto data_factory = make_record_data_class(&runtime);
    qjs::value record_callback = qjs::value::undefined(runtime.context());
    auto sr_client_factory = make_schema_registry_client_class(&runtime);
    result = initial_native_modules(
      &runtime, &record_callback, &sr_client_factory);
    if (!result) [[unlikely]] {
        std::println(
          stderr,
          "unable to install native modules: {}",
          result.error().val.debug_string());
        return 1;
    }
    result = compile_and_load(&runtime);
    if (!result) [[unlikely]] {
        std::println(
          stderr,
          "unable to load module: {}",
          result.error().val.debug_string());
        return 1;
    }
    if (!record_callback.is_function()) [[unlikely]] {
        std::println(stderr, "module did not call onRecordWritten");
        return 1;
    }
    redpanda::on_record_written(
      [&runtime, &writer_factory, &record_callback, &data_factory](
        const redpanda::write_event& evt, redpanda::record_writer* writer) {
          const qjs::value js_writer = writer_factory.create(
            std::make_unique<record_writer>(writer, &data_factory));
          auto event_result = make_write_event(
            runtime.context(), evt, &data_factory);
          if (!event_result) [[unlikely]] {
              std::println(
                stderr,
                "error creating event: {}",
                event_result.error().val.debug_string());
              return std::make_error_code(std::errc::bad_message);
          }
          auto args = std::to_array({event_result.value(), js_writer});
          auto js_result = record_callback.call(args);
          if (js_result.has_value()) {
              // TODO(rockwood): if this is a promise we should await it.
              return std::error_code();
          }
          std::println(
            stderr,
            "error processing record: {}",
            js_result.error().val.debug_string());
          return std::make_error_code(std::errc::interrupted);
      });
    return 0;
}

} // namespace redpanda::js

int main() { return redpanda::js::run(); }
