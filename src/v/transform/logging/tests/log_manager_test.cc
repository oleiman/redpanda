#include "base/units.h"
#include "config/mock_property.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "strings/utf8.h"
#include "test_utils/async.h"
#include "transform/logging/event.h"
#include "transform/logging/io.h"
#include "transform/logging/log_manager.h"
#include "transform/logging/tests/utils.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>

namespace transform::logging {

namespace {

using namespace std::chrono_literals;
using manager_t = transform::logging::manager<ss::manual_clock>;

class fake_client : public transform::logging::client {
public:
    fake_client() = default;

    ss::future<>
    write(model::transform_name_view, ss::chunked_fifo<iobuf> events) override {
        std::move(events.begin(), events.end(), std::back_inserter(_logs));
        return ss::now();
    }

    const ss::chunked_fifo<iobuf>& logs() const { return _logs; }

private:
    ss::chunked_fifo<iobuf> _logs;
};

} // namespace

class TransformLogManagerTest : public ::testing::Test {
public:
    void SetUp() override {
        auto sink = std::make_unique<fake_client>();
        _client = sink.get();
        _manager = std::make_unique<manager_t>(
          model::node_id{0},
          std::move(sink),
          _buffer_limit(),
          _line_limit.bind(),
          _flush_ms.bind());
        start_manager();
    }

    void SetUp(size_t buffer_capacity, size_t line_max) {
        set_buffer_limit(buffer_capacity);
        set_line_limit(line_max);
        SetUp();
    }

    void TearDown() override {
        _client = nullptr;
        _manager->stop().get();
        _manager.reset();
        set_buffer_limit(default_bl);
        set_line_limit(default_ll);
        set_flush_interval(default_flush);
    }

    void start_manager() { _manager->start().get(); }
    void stop_manager() { _manager->stop().get(); }

    void enqueue_log(
      ss::log_level lvl,
      model::transform_name_view name,
      std::string_view msg) {
        _manager->enqueue_log(lvl, name, msg);
    }

    void enqueue_log(std::string_view msg) {
        enqueue_log(
          ss::log_level::info, model::transform_name_view{"foo"}, msg);
    }

    const ss::chunked_fifo<iobuf>& logs() const { return _client->logs(); }

    std::chrono::milliseconds flush_interval() { return _flush_ms(); }

    void advance_clock(std::optional<ss::manual_clock::duration> dur = {}) {
        ss::manual_clock::advance(dur.value_or(flush_interval()));
        tests::drain_task_queue().get();
    }

    ss::sstring last_log_msg() {
        return testing::get_message_body(logs().back().copy());
    }

    void set_buffer_limit(size_t bl) { _buffer_limit.update(std::move(bl)); }
    void set_line_limit(size_t ll) { _line_limit.update(std::move(ll)); }
    void set_flush_interval(std::chrono::milliseconds fi) {
        _flush_ms.update(std::move(fi));
    }

private:
    fake_client* _client{};
    std::unique_ptr<manager_t> _manager{};

    static constexpr size_t default_bl{100_KiB};
    config::mock_property<size_t> _buffer_limit{default_bl};

    static constexpr size_t default_ll{1_KiB};
    config::mock_property<size_t> _line_limit{default_ll};

    static constexpr std::chrono::milliseconds default_flush{500ms};
    config::mock_property<std::chrono::milliseconds> _flush_ms{default_flush};
};

TEST_F(TransformLogManagerTest, EnqueueLogs) {
    enqueue_log("Hello from some test code!");

    advance_clock();
    EXPECT_EQ(logs().size(), 1);

    enqueue_log("Hello again from some test code!");

    advance_clock();
    EXPECT_EQ(logs().size(), 2);

    auto msg = last_log_msg();
    EXPECT_TRUE(msg.find("again") != ss::sstring::npos);
    EXPECT_THAT(msg, ::testing::HasSubstr("again"));
}

TEST_F(TransformLogManagerTest, LogsDroppedAtShutdown) {
    constexpr size_t n = 10;

    for (int i = 0; i < n; ++i) {
        enqueue_log("Hello, World!");
    }

    EXPECT_EQ(logs().size(), 0);
    stop_manager();
    EXPECT_EQ(logs().size(), 0);
}

TEST_F(TransformLogManagerTest, LargeBuffer) {
    // This will cause a reactor stall in Debug mode but NOT
    // in Release

    constexpr size_t buf_cap = 1_MiB;
    constexpr size_t line_max = 1_KiB;
    set_buffer_limit(buf_cap);
    set_line_limit(line_max);
    SetUp();

    static const std::vector<ss::sstring> names{
      "foo",
      "bar",
      "baz",
      "qux",
    };

    auto N = buf_cap / line_max;

    for (int i = 0; i < N; ++i) {
        enqueue_log(
          ss::log_level::info,
          model::transform_name_view{names.at(i % names.size())},
          ss::sstring(line_max, 'x'));
    }

    advance_clock();

    EXPECT_EQ(logs().size(), N);
}

TEST_F(TransformLogManagerTest, BufferLimits) {
    constexpr size_t buf_cap = 1_KiB;
    constexpr size_t line_max = 16;
    constexpr size_t line_cap = buf_cap / line_max;

    set_buffer_limit(buf_cap);
    set_line_limit(line_max);
    SetUp();

    static const std::vector<ss::sstring> names{
      "foo",
      "bar",
      "baz",
      "qux",
    };

    // some logs will get dropped due to buffer limit semaphore
    // irrespective of transform name
    for (int i = 0; i < line_cap * 2; ++i) {
        enqueue_log(
          ss::log_level::info,
          model::transform_name_view{names.at(i % 3)},
          ss::sstring(line_max * 2, 'x'));
    }

    advance_clock();
    EXPECT_EQ(logs().size(), line_cap);

    // we should have full capacity now
    for (int i = 0; i < line_cap; ++i) {
        enqueue_log(
          ss::log_level::info,
          model::transform_name_view{names.at(i % names.size())},
          ss::sstring(line_max * 2, 'x'));
    }

    advance_clock();
    EXPECT_EQ(logs().size(), line_cap * 2);
}

TEST_F(TransformLogManagerTest, LwmTriggerFlush) {
    constexpr size_t buf_cap = 1000;
    constexpr size_t line_max = buf_cap * 8 / 10;
    set_buffer_limit(buf_cap);
    set_line_limit(line_max);
    SetUp();

    constexpr size_t big_line = line_max;
    constexpr size_t small_line = line_max / 7;

    // this shouldn't trigger lwm
    enqueue_log(ss::sstring(big_line, 'x'));
    EXPECT_TRUE(logs().empty());
    advance_clock(1ms);
    EXPECT_TRUE(logs().empty());

    // this one _should_ trigger lwm
    enqueue_log(ss::sstring(small_line, 'x'));
    EXPECT_TRUE(logs().empty());
    // any duration really. we should flush immediately.
    advance_clock(1ms);
    EXPECT_EQ(logs().size(), 2);

    // we should have full capacity now
    enqueue_log(ss::sstring(line_max + small_line, 'x'));
    advance_clock();
    EXPECT_EQ(logs().size(), 3);
}

TEST_F(TransformLogManagerTest, MessageTruncation) {
    constexpr size_t line_max = 16;
    // arbitrary buffer cap, don't care, but set the line max to something
    // convenient and small
    set_buffer_limit(1_KiB);
    set_line_limit(line_max);
    SetUp();

    enqueue_log(ss::sstring(line_max * 2, 'x'));
    advance_clock();
    EXPECT_EQ(last_log_msg().length(), line_max);

    // test that truncation occurs _after_ control char escaping
    ss::sstring in_msg(line_max, char{0x7f});
    auto escaped = replace_control_chars_in_string(in_msg);
    EXPECT_GT(escaped.length(), line_max);

    enqueue_log(in_msg);
    advance_clock();
    auto msg = last_log_msg();
    EXPECT_EQ(msg.length(), line_max);
    EXPECT_EQ(msg, escaped.substr(0, line_max));
}

TEST_F(TransformLogManagerTest, IllegalMessages) {
    std::string bad_utf8_msg = "FOO\xc3\x28";
    const std::array<char, 8> control_char_msg{
      'f', 'o', 'o', 0x01, 0x02, 0x03, 0x04, 0x00};

    enqueue_log(
      ss::log_level::info, model::transform_name_view{"foo"}, bad_utf8_msg);

    // invalid UTF-8 message is dropped
    advance_clock();
    EXPECT_EQ(logs().size(), 0);

    enqueue_log(
      ss::log_level::info,
      model::transform_name_view{"foo"},
      control_char_msg.data());

    // control char message is properly escaped
    advance_clock();
    EXPECT_EQ(logs().size(), 1);

    auto msg = testing::get_message_body(logs().front().copy());
    EXPECT_EQ(msg, replace_control_chars_in_string(control_char_msg.data()));
}

} // namespace transform::logging
