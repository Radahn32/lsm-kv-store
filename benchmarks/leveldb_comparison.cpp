#include <benchmark/benchmark.h>

#include "kvstore/db_impl.hpp"
#include "storage/level_manager.hpp"
#include "storage/load_monitor.hpp"
#include "storage/smart_scheduler.hpp"

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

// Helper to compute nearest-rank percentile
uint64_t ComputePercentile(std::vector<uint64_t>& latencies_us, double rank) {
    if (latencies_us.empty()) return 0;
    std::sort(latencies_us.begin(), latencies_us.end());
    size_t index = static_cast<size_t>(std::ceil(rank / 100.0 * latencies_us.size())) - 1;
    if (index >= latencies_us.size()) index = latencies_us.size() - 1;
    return latencies_us[index];
}

std::string PadKey(size_t id) {
    std::ostringstream oss;
    oss << "key:" << std::setw(8) << std::setfill('0') << id;
    return oss.str();
}

// ============================================================================
// LSM-KV Setup Helpers
// ============================================================================

struct LSMKVContext {
    std::shared_ptr<kvstore::LoadMonitor> monitor;
    std::shared_ptr<kvstore::SmartScheduler> scheduler;
    std::shared_ptr<kvstore::LevelManager> level_manager;
    std::unique_ptr<kvstore::DBImpl> db;
    std::filesystem::path dir;
};

LSMKVContext OpenLSMKV(const std::string& path,
                       bool enable_vlog = true,
                       size_t memtable_limit = 4 * 1024 * 1024,
                       size_t vlog_threshold = 1024,
                       size_t l0_trigger = 4) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    std::filesystem::create_directories(path, ec);

    LSMKVContext ctx;
    ctx.dir = path;
    ctx.monitor = std::make_shared<kvstore::LoadMonitor>();

    kvstore::SchedulerOptions s_opts;
    s_opts.high_load_rps = 100.0;
    s_opts.low_load_rps = 20.0;
    s_opts.high_queue_depth = 8;
    s_opts.target_p99_us = 2000;
    s_opts.max_l0_debt_threshold = 8;
    ctx.scheduler = std::make_shared<kvstore::SmartScheduler>(ctx.monitor, s_opts);

    kvstore::CompactionOptions comp_opts;
    comp_opts.l0_trigger = l0_trigger;
    ctx.level_manager = std::make_shared<kvstore::LevelManager>(
        (ctx.dir / "sst").string(),
        8 * 1024 * 1024, // 8 MB BlockCache
        7, 10,
        comp_opts,
        ctx.monitor,
        ctx.scheduler);

    kvstore::DBOptions db_opts;
    db_opts.wal_path = (ctx.dir / "wal").string();
    db_opts.memtable_limit = memtable_limit;
    db_opts.disk = ctx.level_manager;
    if (enable_vlog) {
        db_opts.vlog_dir = (ctx.dir / "vlog").string();
    }
    db_opts.vlog_threshold = vlog_threshold;
    db_opts.vlog_max_segment_size = 16 * 1024 * 1024;

    ctx.db = std::make_unique<kvstore::DBImpl>(std::move(db_opts));
    auto s = ctx.db->Open();
    if (!s.ok()) {
        throw std::runtime_error("Failed to open LSMKV at " + path + ": " + s.message);
    }
    return ctx;
}

void CloseLSMKV(LSMKVContext& ctx) {
    if (ctx.db) {
        ctx.db->WaitForFlush();
    }
    if (ctx.level_manager) {
        ctx.level_manager->WaitForCompaction();
    }
    ctx.db.reset();
    ctx.level_manager.reset();
    ctx.scheduler.reset();
    ctx.monitor.reset();
    std::error_code ec;
    std::filesystem::remove_all(ctx.dir, ec);
}

// ============================================================================
// LevelDB Setup Helpers
// ============================================================================

struct LevelDBContext {
    leveldb::DB* db = nullptr;
    const leveldb::FilterPolicy* filter_policy = nullptr;
    leveldb::Cache* block_cache = nullptr;
    std::filesystem::path dir;
};

LevelDBContext OpenLevelDB(const std::string& path,
                           size_t write_buffer_size = 4 * 1024 * 1024) {
    std::error_code ec;
    leveldb::DestroyDB(path, leveldb::Options());
    std::filesystem::remove_all(path, ec);
    std::filesystem::create_directories(path, ec);

    LevelDBContext ctx;
    ctx.dir = path;
    ctx.filter_policy = leveldb::NewBloomFilterPolicy(10);
    ctx.block_cache = leveldb::NewLRUCache(8 * 1024 * 1024); // 8 MB cache

    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = write_buffer_size;
    options.filter_policy = ctx.filter_policy;
    options.block_cache = ctx.block_cache;

    leveldb::Status s = leveldb::DB::Open(options, path, &ctx.db);
    if (!s.ok()) {
        delete ctx.block_cache;
        delete ctx.filter_policy;
        throw std::runtime_error("Failed to open LevelDB at " + path + ": " + s.ToString());
    }
    return ctx;
}

void CloseLevelDB(LevelDBContext& ctx) {
    if (ctx.db) {
        delete ctx.db;
        ctx.db = nullptr;
    }
    if (ctx.block_cache) {
        delete ctx.block_cache;
        ctx.block_cache = nullptr;
    }
    if (ctx.filter_policy) {
        delete ctx.filter_policy;
        ctx.filter_policy = nullptr;
    }
    leveldb::DestroyDB(ctx.dir.string(), leveldb::Options());
    std::error_code ec;
    std::filesystem::remove_all(ctx.dir, ec);
}

// ============================================================================
// 1. Large-Value Write Throughput: LSM-KV (vLog) vs LevelDB
// ============================================================================

static void BM_LargeValueWrite_LSMKV(benchmark::State& state) {
    const size_t kNumKeys = 10000;
    const size_t kValueSize = 4096; // 4 KB values -> triggers vLog separation
    const std::string value(kValueSize, 'X');
    const std::string dir = "/tmp/lsm_bench_lsmkv_large";

    for (auto _ : state) {
        state.PauseTiming();
        auto ctx = OpenLSMKV(dir, /*enable_vlog=*/true, 4 * 1024 * 1024, 1024);
        state.ResumeTiming();

        for (size_t i = 0; i < kNumKeys; ++i) {
            ctx.db->Put(PadKey(i), value);
        }
        ctx.db->WaitForFlush();
        ctx.level_manager->WaitForCompaction();

        state.PauseTiming();
        CloseLSMKV(ctx);
        state.ResumeTiming();
    }

    const int64_t total_items = state.iterations() * kNumKeys;
    const int64_t total_bytes = state.iterations() * kNumKeys * (kValueSize + 12);
    state.SetItemsProcessed(total_items);
    state.SetBytesProcessed(total_bytes);
}
BENCHMARK(BM_LargeValueWrite_LSMKV)->Unit(benchmark::kMillisecond)->Iterations(1);

static void BM_LargeValueWrite_LevelDB(benchmark::State& state) {
    const size_t kNumKeys = 10000;
    const size_t kValueSize = 4096;
    const std::string value(kValueSize, 'X');
    const std::string dir = "/tmp/lsm_bench_leveldb_large";

    for (auto _ : state) {
        state.PauseTiming();
        auto ctx = OpenLevelDB(dir, 4 * 1024 * 1024);
        leveldb::WriteOptions write_opts;
        state.ResumeTiming();

        for (size_t i = 0; i < kNumKeys; ++i) {
            ctx.db->Put(write_opts, PadKey(i), value);
        }

        state.PauseTiming();
        CloseLevelDB(ctx);
        state.ResumeTiming();
    }

    const int64_t total_items = state.iterations() * kNumKeys;
    const int64_t total_bytes = state.iterations() * kNumKeys * (kValueSize + 12);
    state.SetItemsProcessed(total_items);
    state.SetBytesProcessed(total_bytes);
}
BENCHMARK(BM_LargeValueWrite_LevelDB)->Unit(benchmark::kMillisecond)->Iterations(1);

// ============================================================================
// 2. Tail Latency Under Traffic Bursts (SmartScheduler vs Standard Compaction)
// ============================================================================

static void BM_BurstP99Latency_LSMKV(benchmark::State& state) {
    const std::string dir = "/tmp/lsm_bench_lsmkv_burst";
    const size_t kInitialKeys = 4000;
    const size_t kBurstOps = 4000;
    const std::string small_val(256, 'B');

    for (auto _ : state) {
        state.PauseTiming();
        // Use a smaller memtable to induce frequent flushes and background compaction
        auto ctx = OpenLSMKV(dir, /*enable_vlog=*/false, 64 * 1024, 1024, /*l0_trigger=*/4);
        for (size_t i = 0; i < kInitialKeys; ++i) {
            ctx.db->Put(PadKey(i), small_val);
        }
        ctx.db->WaitForFlush();

        std::vector<uint64_t> latencies_us;
        latencies_us.reserve(kBurstOps);
        std::mt19937_64 rng(42);

        state.ResumeTiming();

        for (size_t i = 0; i < kBurstOps; ++i) {
            const bool is_write = (rng() % 100) < 30; // 30% write burst, 70% read
            const size_t key_id = rng() % (kInitialKeys + i + 1);
            const std::string key = PadKey(key_id);

            const auto start = Clock::now();
            if (is_write) {
                ctx.db->Put(key, small_val);
            } else {
                std::string val;
                ctx.db->Get(key, &val);
            }
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
            latencies_us.push_back(elapsed);
        }

        state.PauseTiming();

        uint64_t p50 = ComputePercentile(latencies_us, 50.0);
        uint64_t p95 = ComputePercentile(latencies_us, 95.0);
        uint64_t p99 = ComputePercentile(latencies_us, 99.0);

        state.counters["p50_us"] = p50;
        state.counters["p95_us"] = p95;
        state.counters["p99_us"] = p99;

        CloseLSMKV(ctx);
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations() * kBurstOps);
}
BENCHMARK(BM_BurstP99Latency_LSMKV)->Unit(benchmark::kMillisecond)->Iterations(1);

static void BM_BurstP99Latency_LevelDB(benchmark::State& state) {
    const std::string dir = "/tmp/lsm_bench_leveldb_burst";
    const size_t kInitialKeys = 4000;
    const size_t kBurstOps = 4000;
    const std::string small_val(256, 'B');

    for (auto _ : state) {
        state.PauseTiming();
        auto ctx = OpenLevelDB(dir, 64 * 1024); // 64 KB write buffer to trigger compaction
        leveldb::WriteOptions write_opts;
        leveldb::ReadOptions read_opts;

        for (size_t i = 0; i < kInitialKeys; ++i) {
            ctx.db->Put(write_opts, PadKey(i), small_val);
        }

        std::vector<uint64_t> latencies_us;
        latencies_us.reserve(kBurstOps);
        std::mt19937_64 rng(42);

        state.ResumeTiming();

        for (size_t i = 0; i < kBurstOps; ++i) {
            const bool is_write = (rng() % 100) < 30; // 30% write burst, 70% read
            const size_t key_id = rng() % (kInitialKeys + i + 1);
            const std::string key = PadKey(key_id);

            const auto start = Clock::now();
            if (is_write) {
                ctx.db->Put(write_opts, key, small_val);
            } else {
                std::string val;
                ctx.db->Get(read_opts, key, &val);
            }
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
            latencies_us.push_back(elapsed);
        }

        state.PauseTiming();

        uint64_t p50 = ComputePercentile(latencies_us, 50.0);
        uint64_t p95 = ComputePercentile(latencies_us, 95.0);
        uint64_t p99 = ComputePercentile(latencies_us, 99.0);

        state.counters["p50_us"] = p50;
        state.counters["p95_us"] = p95;
        state.counters["p99_us"] = p99;

        CloseLevelDB(ctx);
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations() * kBurstOps);
}
BENCHMARK(BM_BurstP99Latency_LevelDB)->Unit(benchmark::kMillisecond)->Iterations(1);

// ============================================================================
// 3. Point Read & Time-Travel Overhead: LevelDB Get vs LSM-KV Get & GetAsOf
// ============================================================================

class MultiVersionFixture : public benchmark::Fixture {
public:
    static constexpr size_t kNumKeys = 10000;
    static constexpr size_t kVersionsPerKey = 5;

    void SetUp(const ::benchmark::State&) override {
        if (initialized_) return;

        leveldb_ctx_ = std::make_unique<LevelDBContext>(OpenLevelDB("/tmp/lsm_bench_leveldb_mvcc", 1024 * 1024));
        lsmkv_ctx_ = std::make_unique<LSMKVContext>(OpenLSMKV("/tmp/lsm_bench_lsmkv_mvcc", /*enable_vlog=*/false, 1024 * 1024));

        leveldb::WriteOptions write_opts;
        // Populate 10,000 keys with 5 versions each
        for (size_t v = 1; v <= kVersionsPerKey; ++v) {
            for (size_t i = 0; i < kNumKeys; ++i) {
                std::string key = PadKey(i);
                std::string val = "val_v" + std::to_string(v) + "_" + std::to_string(i);
                leveldb_ctx_->db->Put(write_opts, key, val);
                lsmkv_ctx_->db->Put(key, val);
            }
        }

        lsmkv_ctx_->db->WaitForFlush();
        lsmkv_ctx_->level_manager->CompactNow();
        lsmkv_ctx_->level_manager->WaitForCompaction();

        keys_.reserve(kNumKeys);
        for (size_t i = 0; i < kNumKeys; ++i) {
            keys_.push_back(PadKey(i));
        }

        initialized_ = true;
    }

    void TearDown(const ::benchmark::State&) override {
        // Kept alive across runs; clean up at exit or destructor
    }

    static void GlobalCleanup() {
        if (leveldb_ctx_) {
            CloseLevelDB(*leveldb_ctx_);
            leveldb_ctx_.reset();
        }
        if (lsmkv_ctx_) {
            CloseLSMKV(*lsmkv_ctx_);
            lsmkv_ctx_.reset();
        }
        initialized_ = false;
    }

protected:
    static inline std::unique_ptr<LevelDBContext> leveldb_ctx_;
    static inline std::unique_ptr<LSMKVContext> lsmkv_ctx_;
    static inline std::vector<std::string> keys_;
    static inline bool initialized_ = false;
};

BENCHMARK_F(MultiVersionFixture, BM_PointGet_LevelDB)(benchmark::State& state) {
    leveldb::ReadOptions read_opts;
    size_t idx = 0;
    for (auto _ : state) {
        std::string val;
        leveldb_ctx_->db->Get(read_opts, keys_[idx % keys_.size()], &val);
        benchmark::DoNotOptimize(val);
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(MultiVersionFixture, BM_PointGet_LSMKV)(benchmark::State& state) {
    size_t idx = 0;
    for (auto _ : state) {
        std::string val;
        lsmkv_ctx_->db->Get(keys_[idx % keys_.size()], &val);
        benchmark::DoNotOptimize(val);
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(MultiVersionFixture, BM_GetAsOf_LSMKV)(benchmark::State& state) {
    // Query historical version 2 (timestamp ~ 15,000 in sequence)
    const kvstore::Timestamp historical_ts = static_cast<kvstore::Timestamp>(kNumKeys * 2);
    size_t idx = 0;
    for (auto _ : state) {
        std::string val;
        lsmkv_ctx_->db->GetAsOf(keys_[idx % keys_.size()], historical_ts, &val);
        benchmark::DoNotOptimize(val);
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}

} // namespace

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    MultiVersionFixture::GlobalCleanup();
    return 0;
}
