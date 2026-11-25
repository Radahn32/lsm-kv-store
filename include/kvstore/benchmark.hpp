#pragma once

#include "kvstore/types.hpp"

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace kvstore {

struct BenchmarkConfig {
    std::string work_dir;
    std::string csv_path;
    std::string filter;
    uint64_t ops = 500;
    uint64_t large_ops = 150;
    std::size_t value_bytes = 128;
    std::size_t large_value_bytes = 4096;
    std::size_t memtable_limit = 64U * 1024U;
    std::size_t block_cache_bytes = 4U * 1024U * 1024U;
    std::size_t vlog_threshold = 1024;
    std::size_t vlog_max_segment_size = 256U * 1024U;
    uint64_t seed = 42;
    bool keep_dir = false;
    bool show_help = false;
};

struct BenchmarkResult {
    std::string name;
    uint64_t operations = 0;
    double elapsed_ms = 0.0;
    double ops_per_sec = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double max_us = 0.0;
    uint64_t sst_bytes = 0;
    uint64_t vlog_bytes = 0;
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
    std::string notes;
};

Status ParseBenchmarkArgs(int argc, char** argv, BenchmarkConfig* config);
void PrintBenchmarkUsage(std::ostream& out);

BenchmarkResult ComputeLatencyStats(std::string name,
                                    std::vector<double> latencies_us,
                                    double elapsed_ms);
void PrintBenchmarkTable(const std::vector<BenchmarkResult>& results, std::ostream& out);
Status WriteBenchmarkCsv(const std::vector<BenchmarkResult>& results, const std::string& path);
Status RunBenchmarks(const BenchmarkConfig& config, std::vector<BenchmarkResult>* results);

} // namespace kvstore
