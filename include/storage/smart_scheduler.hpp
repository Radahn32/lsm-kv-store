#pragma once

#include "storage/load_monitor.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace kvstore {

struct SchedulerOptions {
    double high_load_rps{100.0};
    double low_load_rps{20.0};
    size_t high_queue_depth{8};
    uint64_t target_p99_us{2000}; // 2,000 us (2 ms) target
    size_t max_l0_debt_threshold{8}; // >= 8 files in L0 triggers emergency debt override
    uint64_t busy_throttle_bytes_per_sec{64 * 1024}; // 64 KB/s trickle
    uint64_t emergency_min_bytes_per_sec{10 * 1024 * 1024}; // 10 MB/s
    bool pause_on_busy{true}; // pause compaction when busy, unless debt override
};

struct CompactionPolicy {
    bool should_run{true};
    uint64_t max_bytes_per_sec{UINT64_MAX}; // UINT64_MAX = unthrottled
    bool is_debt_override{false};
    std::string mode{"QUIET"}; // "QUIET", "BUSY", "EMERGENCY_DEBT"
};

class SmartScheduler {
public:
    explicit SmartScheduler(std::shared_ptr<LoadMonitor> load_monitor = nullptr,
                            SchedulerOptions options = {});
    ~SmartScheduler() = default;

    SmartScheduler(const SmartScheduler&) = delete;
    SmartScheduler& operator=(const SmartScheduler&) = delete;

    CompactionPolicy EvaluatePolicy(size_t l0_file_count, uint64_t overflow_bytes = 0) const;

    // Rate-limiting throttle called during compaction write loop
    void Throttle(size_t bytes_written, size_t l0_file_count = 0);
    void ResetThrottle();

    const SchedulerOptions& options() const noexcept { return options_; }
    void SetOptions(const SchedulerOptions& options) { options_ = options; }

    std::shared_ptr<LoadMonitor> load_monitor() const noexcept { return load_monitor_; }
    void SetLoadMonitor(std::shared_ptr<LoadMonitor> monitor) { load_monitor_ = monitor; }

private:
    std::shared_ptr<LoadMonitor> load_monitor_;
    SchedulerOptions options_;

    mutable std::mutex rate_limiter_mutex_;
    std::chrono::steady_clock::time_point last_token_time_{std::chrono::steady_clock::now()};
    double available_tokens_{0.0};
};

} // namespace kvstore

namespace lsm {
    using SchedulerOptions = kvstore::SchedulerOptions;
    using CompactionPolicy = kvstore::CompactionPolicy;
    using SmartScheduler = kvstore::SmartScheduler;
} // namespace lsm
