#include "kvstore/benchmark.hpp"

#include <iostream>
#include <utility>

int main(int argc, char** argv) {
    kvstore::BenchmarkConfig config;
    const kvstore::Status parse_status = kvstore::ParseBenchmarkArgs(argc, argv, &config);
    if (!parse_status.ok()) {
        std::cerr << "Error: " << parse_status.message << '\n';
        kvstore::PrintBenchmarkUsage(std::cerr);
        return 1;
    }
    if (config.show_help) {
        kvstore::PrintBenchmarkUsage(std::cout);
        return 0;
    }

    std::vector<kvstore::BenchmarkResult> results;
    const kvstore::Status run_status = kvstore::RunBenchmarks(config, &results);
    if (!run_status.ok()) {
        std::cerr << "Error: " << run_status.message << '\n';
        return 1;
    }
    kvstore::PrintBenchmarkTable(results, std::cout);
    if (!config.csv_path.empty()) {
        const kvstore::Status csv_status = kvstore::WriteBenchmarkCsv(results, config.csv_path);
        if (!csv_status.ok()) {
            std::cerr << "Error: " << csv_status.message << '\n';
            return 1;
        }
        std::cout << "CSV written to " << config.csv_path << '\n';
    }
    return 0;
}
