#include "storage/block_cache.hpp"
#include "storage/sstable.hpp"
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

TEST(BlockCacheTest, BasicPutGetAndHitMissCounters) {
    BlockCache cache(1024);

    EXPECT_EQ(cache.Hits(), 0U);
    EXPECT_EQ(cache.Misses(), 0U);
    EXPECT_EQ(cache.CurrentBytes(), 0U);
    EXPECT_EQ(cache.CapacityBytes(), 1024U);

    // Initial lookup is a miss
    auto miss_val = cache.Get(1, 0);
    EXPECT_EQ(miss_val, nullptr);
    EXPECT_EQ(cache.Misses(), 1U);
    EXPECT_EQ(cache.Hits(), 0U);

    // Insert block
    auto block = std::make_shared<const std::string>("block_data_at_offset_0");
    cache.Put(1, 0, block);
    EXPECT_EQ(cache.CurrentBytes(), block->size());

    // Hit lookup
    auto hit_val = cache.Get(1, 0);
    ASSERT_NE(hit_val, nullptr);
    EXPECT_EQ(*hit_val, "block_data_at_offset_0");
    EXPECT_EQ(cache.Hits(), 1U);
    EXPECT_EQ(cache.Misses(), 1U);

    // Updating existing block
    auto updated_block = std::make_shared<const std::string>("updated_block_data");
    cache.Put(1, 0, updated_block);
    EXPECT_EQ(cache.CurrentBytes(), updated_block->size());
    EXPECT_EQ(*cache.Get(1, 0), "updated_block_data");
    EXPECT_EQ(cache.Hits(), 2U);

    // Reset telemetry
    cache.ResetTelemetry();
    EXPECT_EQ(cache.Hits(), 0U);
    EXPECT_EQ(cache.Misses(), 0U);
}

TEST(BlockCacheTest, LruEvictionOrder) {
    // Capacity for exactly 3 blocks of 100 bytes each
    constexpr size_t kBlockSize = 100;
    BlockCache cache(3 * kBlockSize);

    auto b0 = std::make_shared<const std::string>(kBlockSize, '0');
    auto b1 = std::make_shared<const std::string>(kBlockSize, '1');
    auto b2 = std::make_shared<const std::string>(kBlockSize, '2');
    auto b3 = std::make_shared<const std::string>(kBlockSize, '3');

    cache.Put(1, 0, b0);
    cache.Put(1, 100, b1);
    cache.Put(1, 200, b2);

    EXPECT_EQ(cache.CurrentBytes(), 300U);

    // Access b0 so it becomes Most Recently Used:
    // LRU order is now: b1 (oldest/LRU), b2, b0 (MRU)
    ASSERT_NE(cache.Get(1, 0), nullptr);

    // Insert b3 -> must evict b1
    cache.Put(1, 300, b3);
    EXPECT_EQ(cache.CurrentBytes(), 300U);

    // b1 was evicted
    EXPECT_EQ(cache.Get(1, 100), nullptr);

    // b0, b2, and b3 should still be in cache
    EXPECT_NE(cache.Get(1, 0), nullptr);
    EXPECT_NE(cache.Get(1, 200), nullptr);
    EXPECT_NE(cache.Get(1, 300), nullptr);
}

TEST(BlockCacheTest, OverCapacitySingleBlockSkippedCleanly) {
    BlockCache cache(256);

    auto big_block = std::make_shared<const std::string>(512, 'X');
    cache.Put(1, 0, big_block);

    EXPECT_EQ(cache.CurrentBytes(), 0U);
    EXPECT_EQ(cache.Get(1, 0), nullptr);
}

TEST(BlockCacheTest, EvictFilePurgesOnlyMatchingFileBlocks) {
    BlockCache cache(4096);

    cache.Put(10, 0, std::make_shared<const std::string>("f10_b0"));
    cache.Put(10, 100, std::make_shared<const std::string>("f10_b1"));
    cache.Put(20, 0, std::make_shared<const std::string>("f20_b0"));
    cache.Put(20, 100, std::make_shared<const std::string>("f20_b1"));

    const size_t total_before = cache.CurrentBytes();
    EXPECT_GT(total_before, 0U);

    // Evict file 10 only
    cache.EvictFile(10);

    EXPECT_EQ(cache.Get(10, 0), nullptr);
    EXPECT_EQ(cache.Get(10, 100), nullptr);

    // File 20 blocks must remain intact
    auto b20_0 = cache.Get(20, 0);
    ASSERT_NE(b20_0, nullptr);
    EXPECT_EQ(*b20_0, "f20_b0");

    auto b20_1 = cache.Get(20, 100);
    ASSERT_NE(b20_1, nullptr);
    EXPECT_EQ(*b20_1, "f20_b1");

    EXPECT_LT(cache.CurrentBytes(), total_before);
}

TEST(BlockCacheTest, ConcurrentReadsAndWritesMultiThreaded) {
    constexpr size_t kCapacity = 32 * 1024;
    BlockCache cache(kCapacity);

    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 1000;
    std::atomic<bool> stop_flag{false};

    std::vector<std::thread> workers;
    workers.reserve(kNumThreads);

    for (int t = 0; t < kNumThreads; ++t) {
        workers.emplace_back([&cache, &stop_flag, t]() {
            for (int i = 0; i < kOpsPerThread && !stop_flag.load(); ++i) {
                const uint64_t file_id = static_cast<uint64_t>((t % 4) + 1);
                const uint64_t block_offset = static_cast<uint64_t>((i % 16) * 256);

                if (i % 3 == 0) {
                    // Put
                    auto data = std::make_shared<const std::string>(256, static_cast<char>('A' + (i % 26)));
                    cache.Put(file_id, block_offset, data);
                } else if (i % 3 == 1) {
                    // Get
                    auto res = cache.Get(file_id, block_offset);
                    if (res) {
                        EXPECT_EQ(res->size(), 256U);
                    }
                } else {
                    // Occasional EvictFile
                    if (i % 100 == 2) {
                        cache.EvictFile(file_id);
                    }
                }
            }
        });
    }

    for (auto& w : workers) {
        w.join();
    }

    EXPECT_LE(cache.CurrentBytes(), kCapacity);
    EXPECT_GT(cache.Hits() + cache.Misses(), 0U);
}

TEST(BlockCacheTest, SSTableReaderIntegrationWithBlockCache) {
    static std::atomic<uint64_t> counter{0};
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto instance = counter.fetch_add(1, std::memory_order_relaxed);
    const auto temp_dir = std::filesystem::temp_directory_path() /
                          ("block_cache_sst_" + std::to_string(::getpid()) + "_" +
                           std::to_string(now) + "_" + std::to_string(instance));
    std::filesystem::create_directories(temp_dir);
    const std::string sst_path = (temp_dir / "table.sst").string();

    // 1. Build an SSTable spanning multiple blocks
    {
        SSTableBuilder builder(sst_path, 10);
        for (size_t i = 0; i < 500; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "key_%04zu", i);
            InternalKey ikey{buf, 1, ValueType::kValue};
            std::string val = "value_" + std::string(buf) + "_" + std::string(60, 'z');
            ASSERT_TRUE(builder.Add(ikey, val).ok());
        }
        ASSERT_TRUE(builder.Finish().ok());
    }

    // 2. Open SSTableReader with BlockCache
    BlockCache cache(64 * 1024);
    constexpr uint64_t kFileId = 42;

    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open(sst_path, &reader, kFileId, &cache);
    ASSERT_TRUE(s.ok());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->file_id(), kFileId);
    EXPECT_EQ(reader->cache(), &cache);

    // Initial Point Lookup -> Miss (reads block from disk into cache)
    std::string val1;
    ASSERT_TRUE(reader->Get("key_0100", &val1).ok());
    EXPECT_EQ(cache.Misses(), 1U);
    EXPECT_EQ(cache.Hits(), 0U);
    EXPECT_GT(cache.CurrentBytes(), 0U);

    // Second Point Lookup on same block -> Hit!
    std::string val2;
    ASSERT_TRUE(reader->Get("key_0101", &val2).ok());
    EXPECT_GE(cache.Hits(), 1U);

    // Range scan through all blocks -> loads blocks into cache
    auto iter = reader->NewIterator();
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        count++;
    }
    EXPECT_EQ(count, 500U);
    EXPECT_GT(cache.CurrentBytes(), 0U);

    // Evict file from cache
    cache.EvictFile(kFileId);
    EXPECT_EQ(cache.CurrentBytes(), 0U);

    // Cleanup
    reader.reset();
    std::error_code ec;
    std::filesystem::remove_all(temp_dir, ec);
}

} // namespace kvstore
