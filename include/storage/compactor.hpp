#pragma once

#include "kvstore/types.hpp"
#include "storage/sstable.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace kvstore {

class LevelManager;
struct TableMeta;

struct CompactionOptions {
    uint64_t retention_window{7ULL * 86400}; // timestamp delta (e.g. 7 days or configurable version delta)
    uint64_t target_file_size{2 * 1024 * 1024}; // 2 MB
    size_t l0_trigger{4};
    std::vector<uint64_t> level_size_budget{
        0,                 // L0 (file count based)
        10 * 1024 * 1024,  // L1: 10 MB
        100 * 1024 * 1024, // L2: 100 MB
        1000 * 1024 * 1024 // L3: 1 GB
    };
    size_t bits_per_key{10};
    bool drop_all_historical{false};
};

struct CompactionJob {
    size_t from_level{0};
    size_t to_level{1};
    std::vector<std::shared_ptr<TableMeta>> inputs_from;
    std::vector<std::shared_ptr<TableMeta>> inputs_to;
    uint64_t retention_cutoff{0};
    bool is_bottom_level{false};
};

struct CompactionStats {
    uint64_t bytes_read{0};
    uint64_t bytes_written{0};
    uint64_t keys_read{0};
    uint64_t keys_dropped{0};
    uint64_t keys_retained{0};
};

class Compactor {
public:
    static std::unique_ptr<CompactionJob> PickCompaction(const LevelManager& lm,
                                                         const CompactionOptions& options,
                                                         Timestamp current_time = 0);

    static std::unique_ptr<CompactionJob> PickCompactionForLevel(const LevelManager& lm,
                                                                size_t level,
                                                                const CompactionOptions& options,
                                                                Timestamp current_time = 0);

    static Status RunCompaction(LevelManager& lm,
                                const CompactionOptions& options,
                                const CompactionJob& job,
                                CompactionStats* stats = nullptr);
};

} // namespace kvstore

namespace lsm {
    using CompactionOptions = kvstore::CompactionOptions;
    using CompactionJob = kvstore::CompactionJob;
    using CompactionStats = kvstore::CompactionStats;
    using Compactor = kvstore::Compactor;
} // namespace lsm
