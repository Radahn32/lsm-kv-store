#include "kvstore/cli.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

namespace kvstore {
namespace {

Status ParseArgs(std::initializer_list<std::string> arguments, CliConfig* config) {
    std::vector<std::string> owned(arguments);
    std::vector<char*> argv;
    argv.reserve(owned.size());
    for (auto& argument : owned) argv.push_back(argument.data());
    return ParseCliArgs(static_cast<int>(argv.size()), argv.data(), config);
}

class CliTest : public ::testing::Test {
protected:
    void SetUp() override {
        static std::atomic<uint64_t> counter{0};
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto instance = counter.fetch_add(1, std::memory_order_relaxed);
        data_dir_ = std::filesystem::temp_directory_path() /
                    ("kvstore_cli_test_" + std::to_string(::getpid()) + "_" +
                     std::to_string(now) + "_" + std::to_string(instance));
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(data_dir_, ec);
    }

    CliConfig Config() const {
        CliConfig config;
        config.data_dir = data_dir_.string();
        return config;
    }

    std::filesystem::path data_dir_;
};

TEST(CliArgsTest, DefaultsAndValidFlags) {
    CliConfig config;
    ASSERT_TRUE(ParseArgs({"kvstore_cli"}, &config).ok());
    EXPECT_EQ(config.data_dir, "./kvstore_data");
    EXPECT_EQ(config.memtable_limit, 4U * 1024U * 1024U);
    EXPECT_EQ(config.block_cache_bytes, 16U * 1024U * 1024U);
    EXPECT_EQ(config.vlog_threshold, 1024U);
    EXPECT_EQ(config.vlog_max_segment_size, 16U * 1024U * 1024U);
    EXPECT_TRUE(config.enable_vlog);
    EXPECT_FALSE(config.show_help);

    ASSERT_TRUE(ParseArgs({"kvstore_cli", "--dir", "/tmp/cli data",
                           "--memtable-bytes", "4096", "--cache-bytes", "8192",
                           "--vlog-threshold", "128", "--vlog-segment-bytes", "16384",
                           "--no-vlog"}, &config).ok());
    EXPECT_EQ(config.data_dir, "/tmp/cli data");
    EXPECT_EQ(config.memtable_limit, 4096U);
    EXPECT_EQ(config.block_cache_bytes, 8192U);
    EXPECT_EQ(config.vlog_threshold, 128U);
    EXPECT_EQ(config.vlog_max_segment_size, 16384U);
    EXPECT_FALSE(config.enable_vlog);
}

TEST(CliArgsTest, HelpUnknownFlagsAndInvalidNumbers) {
    CliConfig config;
    ASSERT_TRUE(ParseArgs({"kvstore_cli", "--help"}, &config).ok());
    EXPECT_TRUE(config.show_help);
    ASSERT_TRUE(ParseArgs({"kvstore_cli", "-h"}, &config).ok());
    EXPECT_TRUE(config.show_help);

    Status status = ParseArgs({"kvstore_cli", "--unknown"}, &config);
    EXPECT_EQ(status.code, Status::Code::kInvalidArgument);
    EXPECT_NE(status.message.find("unknown option"), std::string::npos);

    for (const auto& args : std::vector<std::vector<std::string>>{
             {"kvstore_cli", "--memtable-bytes", "0"},
             {"kvstore_cli", "--cache-bytes", "nope"},
             {"kvstore_cli", "--vlog-threshold", "-1"},
             {"kvstore_cli", "--vlog-segment-bytes", "999999999999999999999999999999"},
             {"kvstore_cli", "--cache-bytes"}}) {
        std::vector<char*> argv;
        for (const auto& argument : args) argv.push_back(const_cast<char*>(argument.c_str()));
        status = ParseCliArgs(static_cast<int>(argv.size()), argv.data(), &config);
        EXPECT_EQ(status.code, Status::Code::kInvalidArgument) << args.back();
    }
}

TEST_F(CliTest, TokenizesQuotedEscapedAndReportsCommandErrors) {
    CliSession session(Config());
    ASSERT_TRUE(session.Open().ok());
    std::ostringstream out;
    std::ostringstream err;

    EXPECT_TRUE(session.ExecuteLine("  \t  ", out, err));
    EXPECT_TRUE(session.ExecuteLine("  # comment with an unterminated \" quote", out, err));
    EXPECT_TRUE(out.str().empty());
    EXPECT_TRUE(err.str().empty());

    ASSERT_TRUE(session.ExecuteLine("PuT \"key with space\" \"value with spaces\"", out, err));
    ASSERT_TRUE(session.ExecuteLine("get \"key with space\"", out, err));
    EXPECT_NE(out.str().find("value with spaces\n"), std::string::npos);

    out.str("");
    out.clear();
    ASSERT_TRUE(session.ExecuteLine(R"(put escaped "line1\nline2\t\\\"")", out, err));
    ASSERT_TRUE(session.ExecuteLine("get escaped", out, err));
    EXPECT_NE(out.str().find("line1\nline2\t\\\"\n"), std::string::npos);

    EXPECT_TRUE(session.ExecuteLine("put broken \"unterminated", out, err));
    EXPECT_NE(err.str().find("unterminated double quote"), std::string::npos);
    EXPECT_TRUE(session.ExecuteLine("unknown-command", out, err));
    EXPECT_NE(err.str().find("unknown command"), std::string::npos);
    EXPECT_TRUE(session.ExecuteLine("put only-one-argument", out, err));
    EXPECT_NE(err.str().find("usage: put <key> <value>"), std::string::npos);

    out.str("");
    out.clear();
    std::istringstream script("help\nexit\nget never-read\n");
    EXPECT_EQ(session.Run(script, out, err, false), 0);
    EXPECT_EQ(out.str().find("kv> "), std::string::npos);
    EXPECT_NE(out.str().find("Commands:"), std::string::npos);
}

TEST_F(CliTest, FullCommandFlowIncludesVLogGcAndTelemetry) {
    CliConfig config = Config();
    config.memtable_limit = 256;
    config.vlog_threshold = 32;
    config.vlog_max_segment_size = 180;
    CliSession session(config);
    ASSERT_TRUE(session.Open().ok());

    std::ostringstream out;
    std::ostringstream err;
    const auto run = [&](const std::string& line) {
        EXPECT_TRUE(session.ExecuteLine(line, out, err)) << line;
    };

    run("put history first");
    run("put history second");
    run("get history");
    run("getasof history 1");
    run("diff history 1 2");
    run("scan hist z");
    run("del history");
    run("get history");

    const std::string large_value(120, 'v');
    for (int i = 0; i < 5; ++i) {
        run("put blob \"" + large_value + "\"");
    }
    run("get blob");
    run("gc");
    run("get blob");
    run("flush");
    run("compact");
    run("stats");

    EXPECT_TRUE(err.str().empty()) << err.str();
    EXPECT_NE(out.str().find("second\n"), std::string::npos);
    EXPECT_NE(out.str().find("first\n"), std::string::npos);
    EXPECT_NE(out.str().find("changed: true\nold: first\nnew: second"), std::string::npos);
    EXPECT_NE(out.str().find("history = second"), std::string::npos);
    EXPECT_NE(out.str().find("(not found)"), std::string::npos);
    const std::string output = out.str();
    const std::string gc_marker = "reclaimed_segments=";
    const std::size_t gc_stats = output.find(gc_marker);
    ASSERT_NE(gc_stats, std::string::npos);
    const std::size_t reclaimed_start = gc_stats + gc_marker.size();
    const std::size_t reclaimed_end = output.find(' ', reclaimed_start);
    uint64_t reclaimed_segments = 0;
    const auto parsed_reclaimed = std::from_chars(
        output.data() + reclaimed_start,
        output.data() + (reclaimed_end == std::string::npos ? output.size() : reclaimed_end),
        reclaimed_segments);
    ASSERT_EQ(parsed_reclaimed.ec, std::errc{});
    EXPECT_GT(reclaimed_segments, 0U);
    EXPECT_NE(out.str().find("OK\n"), std::string::npos);
    EXPECT_NE(out.str().find("L0 sstables="), std::string::npos);
    EXPECT_NE(out.str().find("cache hits="), std::string::npos);
    EXPECT_NE(out.str().find("load requests_per_sec="), std::string::npos);
    EXPECT_NE(out.str().find("scheduler mode="), std::string::npos);
    EXPECT_NE(out.str().find("vlog enabled=true"), std::string::npos);
    EXPECT_NE(out.str().find(large_value), std::string::npos);
}

TEST_F(CliTest, PersistsWalAndTimeTravelVersionsAcrossSessions) {
    CliConfig config = Config();
    config.vlog_threshold = 8;
    {
        CliSession session(config);
        ASSERT_TRUE(session.Open().ok());
        std::ostringstream out;
        std::ostringstream err;
        ASSERT_TRUE(session.ExecuteLine("put temporal \"large first value\"", out, err));
        ASSERT_TRUE(session.ExecuteLine("put temporal \"large second value\"", out, err));
        EXPECT_TRUE(err.str().empty()) << err.str();
    }

    CliSession reopened(config);
    ASSERT_TRUE(reopened.Open().ok());
    std::ostringstream out;
    std::ostringstream err;
    ASSERT_TRUE(reopened.ExecuteLine("get temporal", out, err));
    ASSERT_TRUE(reopened.ExecuteLine("getasof temporal 1", out, err));
    EXPECT_TRUE(err.str().empty()) << err.str();
    EXPECT_NE(out.str().find("large second value\n"), std::string::npos);
    EXPECT_NE(out.str().find("large first value\n"), std::string::npos);
}

TEST_F(CliTest, RunShowsPromptOnlyInInteractiveModeAndExitStopsScript) {
    CliSession session(Config());
    std::istringstream piped("help\nexit\nhelp\n");
    std::ostringstream output;
    std::ostringstream error;
    EXPECT_EQ(session.Run(piped, output, error, false), 0);
    EXPECT_EQ(output.str().find("kv> "), std::string::npos);
    EXPECT_EQ(output.str().find("Commands:"), output.str().rfind("Commands:"));

    std::istringstream interactive("exit\n");
    output.str("");
    output.clear();
    EXPECT_EQ(session.Run(interactive, output, error, true), 0);
    EXPECT_EQ(output.str(), "kv> ");
}

} // namespace
} // namespace kvstore
