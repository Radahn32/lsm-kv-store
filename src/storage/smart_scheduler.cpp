#include "storage/smart_scheduler.hpp"

#include <algorithm>
#include <thread>

namespace kvstore {

SmartScheduler::SmartScheduler(std::shared_ptr<LoadMonitor> load_monitor,
                               SchedulerOptions options)
    : load_monitor_(std::move(load_monitor)),
      options_(options),
      last_token_time_(std::chrono::steady_clock::now()) {
}

CompactionPolicy SmartScheduler::EvaluatePolicy(size_t l0_file_count, uint64_t overflow_bytes) const {
    (void)overflow_bytes;

    const bool debt_override = (l0_file_count >= options_.max_l0_debt_threshold);

    double rps = load_monitor_ ? load_monitor_->GetRequestsPerSec() : 0.0;
    size_t queue_depth = load_monitor_ ? load_monitor_->GetQueueDepth() : 0;
    uint64_t p99_us = load_monitor_ ? load_monitor_->GetP99LatencyUs() : 0;

    const bool has_traffic = (rps > 0.0) || (queue_depth > 0);
    const bool is_busy = has_traffic && ((rps > options_.high_load_rps) ||
                                         (queue_depth > options_.high_queue_depth) ||
                                         (p99_us > options_.target_p99_us));

    // 1. Debt Emergency Override: If compaction debt reaches critical threshold,
    // force compaction regardless of busy foreground traffic.
    if (debt_override) {
        CompactionPolicy policy;
        policy.should_run = true;
        policy.is_debt_override = true;
        policy.mode = "EMERGENCY_DEBT";
        policy.max_bytes_per_sec = is_busy ? options_.emergency_min_bytes_per_sec : UINT64_MAX;
        return policy;
    }

    // 2. Busy Traffic Mode: Pause or throttle to prevent foreground query stalls.
    if (is_busy) {
        CompactionPolicy policy;
        policy.should_run = !options_.pause_on_busy;
        policy.max_bytes_per_sec = options_.busy_throttle_bytes_per_sec;
        policy.is_debt_override = false;
        policy.mode = "BUSY";
        return policy;
    }

    // 3. Quiet Traffic Mode: Run full unthrottled compaction speed.
    CompactionPolicy policy;
    policy.should_run = true;
    policy.max_bytes_per_sec = UINT64_MAX;
    policy.is_debt_override = false;
    policy.mode = "QUIET";
    return policy;
}

void SmartScheduler::Throttle(size_t bytes_written, size_t l0_file_count) {
    if (bytes_written == 0) return;

    CompactionPolicy policy = EvaluatePolicy(l0_file_count);
    if (policy.max_bytes_per_sec == UINT64_MAX) {
        return; // Unthrottled full speed
    }

    std::lock_guard lock(rate_limiter_mutex_);
    auto now = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(now - last_token_time_).count();
    last_token_time_ = now;

    double refill_rate = static_cast<double>(policy.max_bytes_per_sec);
    available_tokens_ += elapsed_sec * refill_rate;
    if (available_tokens_ > refill_rate) {
        available_tokens_ = refill_rate; // Cap burst bucket at 1s rate
    }

    if (!policy.should_run) {
        // Paused in busy mode: sleep briefly to yield I/O
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return;
    }

    if (available_tokens_ < static_cast<double>(bytes_written)) {
        double deficit = static_cast<double>(bytes_written) - available_tokens_;
        double sleep_sec = deficit / refill_rate;
        if (sleep_sec > 0.0) {
            auto sleep_duration = std::chrono::duration<double>(std::min(sleep_sec, 0.05));
            std::this_thread::sleep_for(std::chrono::duration_cast<std::chrono::microseconds>(sleep_duration));
        }
        available_tokens_ = 0.0;
    } else {
        available_tokens_ -= static_cast<double>(bytes_written);
    }
}

void SmartScheduler::ResetThrottle() {
    std::lock_guard lock(rate_limiter_mutex_);
    last_token_time_ = std::chrono::steady_clock::now();
    available_tokens_ = 0.0;
}

} // namespace kvstore
