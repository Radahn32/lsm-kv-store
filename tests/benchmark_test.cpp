#include "kvstore/benchmark.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

namespace kvstore {
namespace {

Status ParseArgs(std::initializer_list<std::string> arguments, BenchmarkConfig* config) {
    std::vector<std::string> owned(arguments);
    std::vector<char*> argv;
    argv.reserve(owned.size());
    for (auto& argument : owned) argv.push_back(argument.data());
    return ParseBenchmarkArgs(static_cast<int>(argv.size()), argv.data(), config);
}

class BenchmarkTest : public ::testing::Test {
protected:
    void SetUp() override {
        static std::atomic<uint64_t> counter{0};
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto instance = counter.fetch_add(1, std::memory_order_relaxed);
        root_ = std::filesystem::temp_directory_path() /
                ("kvstore_benchmark_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(now) + "_" + std::to_string(instance));
        csv_path_ = std::filesystem::temp_directory_path() /
                    ("kvstore_benchmark_csv_" + std::to_string(::getpid()) + "_" +
                     std::to_string(instance) + ".csv");
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
        std::filesystem::remove(csv_path_, ec);
    }

    BenchmarkConfig FastConfig() const {
        BenchmarkConfig config;
        config.work_dir = root_.string();
        config.ops = 40;
        config.large_ops = 20;
        config.memtable_limit = 4096;
        config.large_value_bytes = 2048;
        config.vlog_max_segment_size = 8192;
        return config;
    }

    std::filesystem::path root_;
    std::filesystem::path csv_path_;
};

TEST(BenchmarkArgsTest, DefaultsAndValidOptions) {
    BenchmarkConfig config;
    ASSERT_TRUE(ParseArgs({"kvstore_bench"}, &config).ok());
    EXPECT_TRUE(config.work_dir.empty());
    EXPECT_TRUE(config.csv_path.empty());
    EXPECT_TRUE(config.filter.empty());
    EXPECT_EQ(config.ops, 500U);
    EXPECT_EQ(config.large_ops, 150U);
    EXPECT_EQ(config.value_bytes, 128U);
    EXPECT_EQ(config.large_value_bytes, 4096U);
    EXPECT_EQ(config.memtable_limit, 64U * 1024U);
    EXPECT_EQ(config.block_cache_bytes, 4U * 1024U * 1024U);
    EXPECT_EQ(config.vlog_threshold, 1024U);
    EXPECT_EQ(config.vlog_max_segment_size, 256U * 1024U);
    EXPECT_EQ(config.seed, 42U);
    EXPECT_FALSE(config.keep_dir);
    EXPECT_FALSE(config.show_help);

    ASSERT_TRUE(ParseArgs({"kvstore_bench", "--dir", "/tmp/bench data", "--csv", "out.csv",
                           "--filter", "write", "--ops", "120", "--large-ops", "30",
                           "--value-bytes", "64", "--large-value-bytes", "2048",
                           "--memtable-bytes", "8192", "--cache-bytes", "16384",
                           "--vlog-threshold", "256", "--vlog-segment-bytes", "32768",
                           "--seed", "7", "--keep-dir"}, &config).ok());
    EXPECT_EQ(config.work_dir, "/tmp/bench data");
    EXPECT_EQ(config.csv_path, "out.csv");
    EXPECT_EQ(config.filter, "write");
    EXPECT_EQ(config.ops, 120U);
    EXPECT_EQ(config.large_ops, 30U);
    EXPECT_EQ(config.value_bytes, 64U);
    EXPECT_EQ(config.large_value_bytes, 2048U);
    EXPECT_EQ(config.memtable_limit, 8192U);
    EXPECT_EQ(config.block_cache_bytes, 16384U);
    EXPECT_EQ(config.vlog_threshold, 256U);
    EXPECT_EQ(config.vlog_max_segment_size, 32768U);
    EXPECT_EQ(config.seed, 7U);
    EXPECT_TRUE(config.keep_dir);
}

TEST(BenchmarkArgsTest, HelpUnknownFlagsAndInvalidNumbers) {
    BenchmarkConfig config;
    ASSERT_TRUE(ParseArgs({"kvstore_bench", "--help"}, &config).ok());
    EXPECT_TRUE(config.show_help);
    ASSERT_TRUE(ParseArgs({"kvstore_bench", "-h"}, &config).ok());
    EXPECT_TRUE(config.show_help);

    Status status = ParseArgs({"kvstore_bench", "--bogus"}, &config);
    EXPECT_EQ(status.code, Status::Code::kInvalidArgument);
    EXPECT_NE(status.message.find("unknown option"), std::string::npos);

    for (const auto& args : std::vector<std::vector<std::string>>{
             {"kvstore_bench", "--ops", "words"},
             {"kvstore_bench", "--large-ops", "-1"},
             {"kvstore_bench", "--value-bytes", "18446744073709551616"},
             {"kvstore_bench", "--memtable-bytes"}}) {
        std::vector<char*> argv;
        for (const auto& argument : args) argv.push_back(const_cast<char*>(argument.c_str()));
        status = ParseBenchmarkArgs(static_cast<int>(argv.size()), argv.data(), &config);
        EXPECT_EQ(status.code, Status::Code::kInvalidArgument) << args.back();
    }
}

TEST(BenchmarkMetricsTest, ComputesNearestRankPercentilesForSortedAndUnsortedInputs) {
    const BenchmarkResult unsorted = ComputeLatencyStats("unsorted", {9.0, 1.0, 5.0, 3.0}, 2.0);
    EXPECT_EQ(unsorted.operations, 4U);
    EXPECT_DOUBLE_EQ(unsorted.ops_per_sec, 2000.0);
    EXPECT_DOUBLE_EQ(unsorted.p50_us, 3.0);
    EXPECT_DOUBLE_EQ(unsorted.p95_us, 9.0);
    EXPECT_DOUBLE_EQ(unsorted.p99_us, 9.0);
    EXPECT_DOUBLE_EQ(unsorted.max_us, 9.0);

    std::vector<double> sorted;
    for (int value = 1; value <= 20; ++value) sorted.push_back(static_cast<double>(value));
    const BenchmarkResult known = ComputeLatencyStats("known", std::move(sorted), 10.0);
    EXPECT_DOUBLE_EQ(known.p50_us, 10.0);
    EXPECT_DOUBLE_EQ(known.p95_us, 19.0);
    EXPECT_DOUBLE_EQ(known.p99_us, 20.0);
    EXPECT_DOUBLE_EQ(known.max_us, 20.0);

    const BenchmarkResult empty = ComputeLatencyStats("empty", {}, 0.0);
    EXPECT_EQ(empty.operations, 0U);
    EXPECT_DOUBLE_EQ(empty.ops_per_sec, 0.0);
    EXPECT_DOUBLE_EQ(empty.p50_us, 0.0);
    EXPECT_DOUBLE_EQ(empty.p95_us, 0.0);
    EXPECT_DOUBLE_EQ(empty.p99_us, 0.0);
    EXPECT_DOUBLE_EQ(empty.max_us, 0.0);
}

TEST_F(BenchmarkTest, PrintsTableAndWritesEscapedCsv) {
    BenchmarkResult first;
    first.name = "bench,\"one\"";
    first.operations = 3;
    first.elapsed_ms = 1.5;
    first.ops_per_sec = 2000.0;
    first.p50_us = 1.0;
    first.p95_us = 3.0;
    first.p99_us = 3.0;
    first.max_us = 3.0;
    first.sst_bytes = 2048;
    first.vlog_bytes = 4096;
    first.cache_hits = 3;
    first.cache_misses = 1;
    first.notes = "contains,comma and \"quote\"\nsecond line";
    BenchmarkResult second;
    second.name = "plain";
    second.operations = 1;
    second.notes = "second row";

    std::ostringstream table;
    PrintBenchmarkTable({first, second}, table);
    EXPECT_NE(table.str().find("Benchmark"), std::string::npos);
    EXPECT_NE(table.str().find("Elapsed(ms)"), std::string::npos);
    EXPECT_NE(table.str().find("CacheHit%"), std::string::npos);
    EXPECT_NE(table.str().find("bench,\"one\""), std::string::npos);

    ASSERT_TRUE(WriteBenchmarkCsv({first, second}, csv_path_.string()).ok());
    std::ifstream csv_file(csv_path_);
    ASSERT_TRUE(csv_file.is_open());
    const std::string csv((std::istreambuf_iterator<char>(csv_file)),
                          std::istreambuf_iterator<char>());
    EXPECT_NE(csv.find("name,operations,elapsed_ms,ops_per_sec,p50_us,p95_us,p99_us,max_us,"
                       "sst_bytes,vlog_bytes,cache_hits,cache_misses,notes\n"),
              std::string::npos);
    EXPECT_NE(csv.find("\"bench,\"\"one\"\"\""), std::string::npos);
    EXPECT_NE(csv.find("\"contains,comma and \"\"quote\"\"\nsecond line\""), std::string::npos);
    EXPECT_NE(csv.find("\n\"plain\","), std::string::npos);
    EXPECT_NE(csv.find("\n\"bench,\"\"one\"\"\","), std::string::npos);
}

TEST_F(BenchmarkTest, RunsAllWorkloadsAndSupportsFiltering) {
    BenchmarkConfig config = FastConfig();
    std::vector<BenchmarkResult> results;
    ASSERT_TRUE(RunBenchmarks(config, &results).ok());
    ASSERT_EQ(results.size(), 8U);

    for (const auto& result : results) {
        EXPECT_GT(result.ops_per_sec, 0.0) << result.name;
        EXPECT_GT(result.sst_bytes, 0U) << result.name;
        EXPECT_GE(result.p50_us, 0.0) << result.name;
        EXPECT_GE(result.p95_us, 0.0) << result.name;
        EXPECT_GE(result.p99_us, 0.0) << result.name;
        EXPECT_GE(result.max_us, 0.0) << result.name;
        EXPECT_LE(result.p50_us, result.p95_us) << result.name;
        EXPECT_LE(result.p95_us, result.p99_us) << result.name;
        EXPECT_LE(result.p99_us, result.max_us) << result.name;
    }

    const auto inline_result = std::find_if(results.begin(), results.end(), [](const auto& result) {
        return result.name == "large_val_inline";
    });
    const auto vlog_result = std::find_if(results.begin(), results.end(), [](const auto& result) {
        return result.name == "large_val_vlog";
    });
    ASSERT_NE(inline_result, results.end());
    ASSERT_NE(vlog_result, results.end());
    EXPECT_LT(vlog_result->sst_bytes, inline_result->sst_bytes);
    EXPECT_GT(vlog_result->vlog_bytes, 0U);
    EXPECT_NE(vlog_result->notes.find("reclaimed="), std::string::npos);
    EXPECT_NE(vlog_result->notes.find("relocated="), std::string::npos);
    EXPECT_NE(vlog_result->notes.find("dropped="), std::string::npos);

    config.filter = "point_read";
    ASSERT_TRUE(RunBenchmarks(config, &results).ok());
    ASSERT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0].name, "point_read_hit");
    EXPECT_EQ(results[1].name, "point_read_miss");

    config.filter = "no_such_benchmark";
    const Status no_matches = RunBenchmarks(config, &results);
    EXPECT_EQ(no_matches.code, Status::Code::kInvalidArgument);
    EXPECT_TRUE(results.empty());
}

} // namespace
} // namespace kvstore
