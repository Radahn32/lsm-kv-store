#include "storage/load_monitor.hpp"
#include "storage/smart_scheduler.hpp"
#include "storage/level_manager.hpp"
#include "kvstore/memtable.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace kvstore {

class SmartSchedulerTest : public ::testing::Test {
protected:
    void SetUp() override {
        static std::atomic<uint64_t> counter{0};
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto instance = counter.fetch_add(1, std::memory_order_relaxed);
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("smart_scheduler_test_" + std::to_string(::getpid()) + "_" +
                     std::to_string(now) + "_" + std::to_string(instance));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    std::string DbDir() const {
        return test_dir_.string();
    }

    std::filesystem::path test_dir_;
};

// 1. Test LoadMonitor: record 100 synthetic latencies (99 at 100 us, 1 at 5,000 us),
// verify GetP99LatencyUs() returns 5,000 us and ScopedRequestTimer accurately tracks GetQueueDepth().
TEST_F(SmartSchedulerTest, LoadMonitorP99AndQueueDepthTracking) {
    LoadMonitor monitor;

    EXPECT_EQ(monitor.GetQueueDepth(), 0U);
    EXPECT_EQ(monitor.GetP99LatencyUs(), 0U);

    // Verify ScopedRequestTimer tracks in-flight queue depth
    {
        auto timer1 = monitor.RecordRequest();
        EXPECT_EQ(monitor.GetQueueDepth(), 1U);
        {
            auto timer2 = monitor.RecordRequest();
            EXPECT_EQ(monitor.GetQueueDepth(), 2U);
            {
                auto timer3 = monitor.RecordRequest();
                EXPECT_EQ(monitor.GetQueueDepth(), 3U);
            }
            EXPECT_EQ(monitor.GetQueueDepth(), 2U);
        }
        EXPECT_EQ(monitor.GetQueueDepth(), 1U);
    }
    EXPECT_EQ(monitor.GetQueueDepth(), 0U);

    // Record 99 latencies of 100 us and 1 latency of 5,000 us
    monitor.Reset();
    for (int i = 0; i < 99; ++i) {
        monitor.RecordLatency(100);
    }
    monitor.RecordLatency(5000);

    // The 99th percentile across 100 samples must be 5,000 us
    EXPECT_EQ(monitor.GetP99LatencyUs(), 5000U);

    // Request rate should be positive within the 1-second window
    EXPECT_GT(monitor.GetRequestsPerSec(), 0.0);
}

// 2. Test Busy Throttling: simulate high RPS / high p99 latency with moderate L0 count (4 files)
// and verify SmartScheduler pauses/throttles compaction.
TEST_F(SmartSchedulerTest, BusyTrafficModePausesOrThrottlesCompaction) {
    auto monitor = std::make_shared<LoadMonitor>();

    SchedulerOptions opts;
    opts.target_p99_us = 2000;
    opts.max_l0_debt_threshold = 8;
    opts.pause_on_busy = true;
    opts.busy_throttle_bytes_per_sec = 64 * 1024;

    SmartScheduler scheduler(monitor, opts);

    // Simulate high latency / busy traffic exceeding target_p99_us (e.g. 5,000 us > 2,000 us)
    for (int i = 0; i < 99; ++i) {
        monitor->RecordLatency(100);
    }
    monitor->RecordLatency(5000);
    EXPECT_GT(monitor->GetP99LatencyUs(), opts.target_p99_us);

    // Moderate L0 count: 4 files (< max_l0_debt_threshold)
    CompactionPolicy policy = scheduler.EvaluatePolicy(/*l0_file_count=*/ 4);

    EXPECT_FALSE(policy.should_run);
    EXPECT_FALSE(policy.is_debt_override);
    EXPECT_EQ(policy.mode, "BUSY");
    EXPECT_EQ(policy.max_bytes_per_sec, opts.busy_throttle_bytes_per_sec);
}

// 3. Test Quiet Full-Speed: drop simulated traffic to idle and verify SmartScheduler switches
// to unthrottled compaction.
TEST_F(SmartSchedulerTest, QuietTrafficModeRunsUnthrottled) {
    auto monitor = std::make_shared<LoadMonitor>();

    SchedulerOptions opts;
    opts.target_p99_us = 2000;
    opts.max_l0_debt_threshold = 8;
    opts.pause_on_busy = true;

    SmartScheduler scheduler(monitor, opts);

    // Idle/quiet traffic: all latencies well under target_p99_us
    for (int i = 0; i < 50; ++i) {
        monitor->RecordLatency(150); // 150 us < 2,000 us
    }
    EXPECT_LE(monitor->GetP99LatencyUs(), opts.target_p99_us);

    CompactionPolicy policy = scheduler.EvaluatePolicy(/*l0_file_count=*/ 4);

    EXPECT_TRUE(policy.should_run);
    EXPECT_FALSE(policy.is_debt_override);
    EXPECT_EQ(policy.mode, "QUIET");
    EXPECT_EQ(policy.max_bytes_per_sec, UINT64_MAX); // Unthrottled
}

// 4. Test Debt Emergency Override: simulate heavy traffic and high L0 debt (>= 8 files),
// verifying SmartScheduler overrides the pause and allows compaction to drain the backlog.
TEST_F(SmartSchedulerTest, DebtEmergencyOverrideForcesCompactionUnderHeavyLoad) {
    auto monitor = std::make_shared<LoadMonitor>();

    SchedulerOptions opts;
    opts.target_p99_us = 2000;
    opts.max_l0_debt_threshold = 8;
    opts.pause_on_busy = true;
    opts.emergency_min_bytes_per_sec = 10 * 1024 * 1024;

    SmartScheduler scheduler(monitor, opts);

    // Heavy traffic: high latency
    for (int i = 0; i < 99; ++i) {
        monitor->RecordLatency(100);
    }
    monitor->RecordLatency(8000); // 8,000 us > 2,000 us

    // High L0 debt (>= 8 files)
    CompactionPolicy policy = scheduler.EvaluatePolicy(/*l0_file_count=*/ 8);

    // Must override busy pause and force compaction
    EXPECT_TRUE(policy.should_run);
    EXPECT_TRUE(policy.is_debt_override);
    EXPECT_EQ(policy.mode, "EMERGENCY_DEBT");
    EXPECT_GE(policy.max_bytes_per_sec, opts.emergency_min_bytes_per_sec);

    // Also verify with higher debt (e.g. 12 files)
    CompactionPolicy policy12 = scheduler.EvaluatePolicy(/*l0_file_count=*/ 12);
    EXPECT_TRUE(policy12.should_run);
    EXPECT_TRUE(policy12.is_debt_override);
    EXPECT_EQ(policy12.mode, "EMERGENCY_DEBT");
}

// 5. Test Integration with LevelManager: Verify foreground queries update LoadMonitor
// and background compaction cooperates with SmartScheduler.
TEST_F(SmartSchedulerTest, LevelManagerIntegrationWithLoadMonitorAndScheduler) {
    auto monitor = std::make_shared<LoadMonitor>();

    SchedulerOptions sched_opts;
    sched_opts.target_p99_us = 5000;
    sched_opts.max_l0_debt_threshold = 6;
    auto scheduler = std::make_shared<SmartScheduler>(monitor, sched_opts);

    CompactionOptions comp_opts;
    comp_opts.l0_trigger = 3;

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, comp_opts, monitor, scheduler);

    // Verify LevelManager exposes the same LoadMonitor and SmartScheduler instances
    EXPECT_EQ(&lm.GetLoadMonitor(), monitor.get());
    EXPECT_EQ(&lm.GetSmartScheduler(), scheduler.get());

    // Flush a MemTable to disk
    MemTable mem;
    for (int i = 0; i < 10; ++i) {
        mem.Put({"key_" + std::to_string(i), static_cast<Timestamp>(i + 1), ValueType::kValue}, "val");
    }
    auto it = mem.NewIterator();
    ASSERT_TRUE(lm.Flush(*it).ok());

    // Run reads via GetAsOf — this must automatically feed telemetry to LoadMonitor
    for (int i = 0; i < 10; ++i) {
        InternalKey out_key;
        std::string out_val;
        ASSERT_TRUE(lm.GetAsOf("key_" + std::to_string(i), 100, &out_key, &out_val).ok());
        EXPECT_EQ(out_val, "val");
    }

    // Telemetry must have recorded the requests
    EXPECT_GT(monitor->GetRequestsPerSec(), 0.0);
    EXPECT_GT(monitor->GetP99LatencyUs(), 0U);

    // Verify NewIterator also records telemetry
    {
        auto disk_iter = lm.NewIterator();
        ASSERT_NE(disk_iter, nullptr);
    }
    EXPECT_GT(monitor->GetRequestsPerSec(), 0.0);
}

} // namespace kvstore
