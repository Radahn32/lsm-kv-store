#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace kvstore {

class LoadMonitor;

class ScopedRequestTimer {
public:
    explicit ScopedRequestTimer(LoadMonitor* monitor);
    ~ScopedRequestTimer();

    ScopedRequestTimer(const ScopedRequestTimer&) = delete;
    ScopedRequestTimer& operator=(const ScopedRequestTimer&) = delete;

    ScopedRequestTimer(ScopedRequestTimer&& other) noexcept;
    ScopedRequestTimer& operator=(ScopedRequestTimer&& other) noexcept;

private:
    LoadMonitor* monitor_{nullptr};
    std::chrono::steady_clock::time_point start_;
};

class LoadMonitor {
public:
    static constexpr size_t kRingBufferSize = 1024;

    LoadMonitor() = default;
    ~LoadMonitor() = default;

    LoadMonitor(const LoadMonitor&) = delete;
    LoadMonitor& operator=(const LoadMonitor&) = delete;

    void OnRequestStart() noexcept;
    void OnRequestEnd(uint64_t latency_us) noexcept;

    // Helper to directly record a completed request latency
    void RecordLatency(uint64_t latency_us) noexcept;

    // RAII timer helper
    ScopedRequestTimer RecordRequest() noexcept;

    // Rolling 1-second request rate
    double GetRequestsPerSec() const;

    // Current in-flight requests (queue depth)
    size_t GetQueueDepth() const noexcept;

    // 99th percentile latency of the last 1,024 recorded latencies
    uint64_t GetP99LatencyUs() const;

    void Reset();

private:
    std::atomic<size_t> in_flight_requests_{0};

    // Latency ring buffer
    mutable std::mutex latency_mutex_;
    std::array<uint64_t, kRingBufferSize> latency_ring_{};
    size_t ring_head_{0};
    size_t ring_count_{0};

    // Rolling 1-second window request timestamps
    mutable std::mutex rate_mutex_;
    mutable std::deque<std::chrono::steady_clock::time_point> request_timestamps_;
};

} // namespace kvstore

namespace lsm {
    using LoadMonitor = kvstore::LoadMonitor;
    using ScopedRequestTimer = kvstore::ScopedRequestTimer;
} // namespace lsm
