#include "storage/load_monitor.hpp"

#include <algorithm>
#include <cmath>

namespace kvstore {

ScopedRequestTimer::ScopedRequestTimer(LoadMonitor* monitor)
    : monitor_(monitor), start_(std::chrono::steady_clock::now()) {
    if (monitor_) {
        monitor_->OnRequestStart();
    }
}

ScopedRequestTimer::~ScopedRequestTimer() {
    if (monitor_) {
        auto end = std::chrono::steady_clock::now();
        uint64_t elapsed_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count());
        monitor_->OnRequestEnd(elapsed_us);
    }
}

ScopedRequestTimer::ScopedRequestTimer(ScopedRequestTimer&& other) noexcept
    : monitor_(other.monitor_), start_(other.start_) {
    other.monitor_ = nullptr;
}

ScopedRequestTimer& ScopedRequestTimer::operator=(ScopedRequestTimer&& other) noexcept {
    if (this != &other) {
        if (monitor_) {
            auto end = std::chrono::steady_clock::now();
            uint64_t elapsed_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count());
            monitor_->OnRequestEnd(elapsed_us);
        }
        monitor_ = other.monitor_;
        start_ = other.start_;
        other.monitor_ = nullptr;
    }
    return *this;
}

void LoadMonitor::OnRequestStart() noexcept {
    in_flight_requests_.fetch_add(1, std::memory_order_relaxed);
}

void LoadMonitor::OnRequestEnd(uint64_t latency_us) noexcept {
    size_t current = in_flight_requests_.load(std::memory_order_relaxed);
    while (current > 0 && !in_flight_requests_.compare_exchange_weak(current, current - 1,
                                                                    std::memory_order_relaxed)) {
        // Retry atomic decrement
    }

    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(rate_mutex_);
        request_timestamps_.push_back(now);
        auto cutoff = now - std::chrono::seconds(1);
        while (!request_timestamps_.empty() && request_timestamps_.front() < cutoff) {
            request_timestamps_.pop_front();
        }
    }

    {
        std::lock_guard lock(latency_mutex_);
        latency_ring_[ring_head_] = latency_us;
        ring_head_ = (ring_head_ + 1) % kRingBufferSize;
        if (ring_count_ < kRingBufferSize) {
            ring_count_++;
        }
    }
}

void LoadMonitor::RecordLatency(uint64_t latency_us) noexcept {
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(rate_mutex_);
        request_timestamps_.push_back(now);
        auto cutoff = now - std::chrono::seconds(1);
        while (!request_timestamps_.empty() && request_timestamps_.front() < cutoff) {
            request_timestamps_.pop_front();
        }
    }

    {
        std::lock_guard lock(latency_mutex_);
        latency_ring_[ring_head_] = latency_us;
        ring_head_ = (ring_head_ + 1) % kRingBufferSize;
        if (ring_count_ < kRingBufferSize) {
            ring_count_++;
        }
    }
}

ScopedRequestTimer LoadMonitor::RecordRequest() noexcept {
    return ScopedRequestTimer(this);
}

size_t LoadMonitor::GetQueueDepth() const noexcept {
    return in_flight_requests_.load(std::memory_order_relaxed);
}

double LoadMonitor::GetRequestsPerSec() const {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(rate_mutex_);
    auto cutoff = now - std::chrono::seconds(1);
    while (!request_timestamps_.empty() && request_timestamps_.front() < cutoff) {
        request_timestamps_.pop_front();
    }
    return static_cast<double>(request_timestamps_.size());
}

uint64_t LoadMonitor::GetP99LatencyUs() const {
    std::vector<uint64_t> samples;
    {
        std::lock_guard lock(latency_mutex_);
        if (ring_count_ == 0) return 0;
        samples.assign(latency_ring_.begin(), latency_ring_.begin() + ring_count_);
    }
    std::sort(samples.begin(), samples.end());
    size_t N = samples.size();
    size_t p99_idx = (N == 0) ? 0 : (N - static_cast<size_t>(std::ceil(0.01 * N)));
    if (p99_idx >= N) p99_idx = N - 1;
    return samples[p99_idx];
}

void LoadMonitor::Reset() {
    in_flight_requests_.store(0, std::memory_order_relaxed);
    {
        std::lock_guard lock(latency_mutex_);
        ring_head_ = 0;
        ring_count_ = 0;
        latency_ring_.fill(0);
    }
    {
        std::lock_guard lock(rate_mutex_);
        request_timestamps_.clear();
    }
}

} // namespace kvstore
