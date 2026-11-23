#include "storage/compactor.hpp"
#include "storage/level_manager.hpp"
#include "kvstore/db_impl.hpp"
#include "kvstore/memtable.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace kvstore {

class CompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        test_dir_ = std::filesystem::temp_directory_path() / ("compaction_test_" + std::to_string(now));
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

TEST_F(CompactionTest, L0ToL1MergeAndCleanup) {
    CompactionOptions opts;
    opts.l0_trigger = 4;
    opts.retention_window = 1000;

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
    lm.StopBackgroundThread();

    std::vector<std::string> old_l0_paths;

    // Flush 5 overlapping L0 SSTables
    for (int table_idx = 0; table_idx < 5; ++table_idx) {
        MemTable mem;
        for (int k = 1; k <= 10; ++k) {
            std::string key = "key_" + std::to_string(k);
            std::string val = "v" + std::to_string(table_idx) + "_" + std::to_string(k);
            Timestamp ts = static_cast<Timestamp>(table_idx * 10 + k);
            mem.Put({key, ts, ValueType::kValue}, val);
        }
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    EXPECT_GE(lm.NumFiles(0), 4U);

    // Save old L0 paths to verify physical deletion later
    for (const auto& meta : lm.GetLevelFiles(0)) {
        old_l0_paths.push_back(meta->file_path);
        EXPECT_TRUE(std::filesystem::exists(meta->file_path));
    }

    // Run compaction synchronously
    ASSERT_TRUE(lm.CompactNow().ok());

    // Verify L0 has 0 files and L1 has >= 1 file
    EXPECT_EQ(lm.NumFiles(0), 0U);
    EXPECT_GE(lm.NumFiles(1), 1U);

    // Verify all old L0 files were unlinked from disk
    for (const auto& path : old_l0_paths) {
        EXPECT_FALSE(std::filesystem::exists(path));
    }

    // Verify L1 files are non-overlapping in user_key range
    auto l1_files = lm.GetLevelFiles(1);
    for (size_t i = 0; i + 1 < l1_files.size(); ++i) {
        EXPECT_LT(l1_files[i]->max_key.user_key, l1_files[i + 1]->min_key.user_key);
    }

    // Verify newest versions can be read via GetAsOf
    for (int k = 1; k <= 10; ++k) {
        std::string key = "key_" + std::to_string(k);
        InternalKey out_key;
        std::string out_val;
        ASSERT_TRUE(lm.GetAsOf(key, 100, &out_key, &out_val).ok());
        EXPECT_EQ(out_key.version, static_cast<Timestamp>(4 * 10 + k));
        EXPECT_EQ(out_val, "v4_" + std::to_string(k));
    }
}

TEST_F(CompactionTest, TimeTravelRetentionWindow) {
    CompactionOptions opts;
    opts.l0_trigger = 10; // high so it won't auto-compact prematurely
    opts.retention_window = 20; // 20 units retention window

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
    lm.StopBackgroundThread();

    // Table 1: oldest versions
    {
        MemTable mem;
        mem.Put({"key1", 10, ValueType::kValue}, "val10");
        mem.Put({"key2", 10, ValueType::kValue}, "val2_10");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Table 2: key1 at ts 25
    {
        MemTable mem;
        mem.Put({"key1", 25, ValueType::kValue}, "val25");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Table 3: key1 at ts 40
    {
        MemTable mem;
        mem.Put({"key1", 40, ValueType::kValue}, "val40");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Table 4: key1 at ts 50
    {
        MemTable mem;
        mem.Put({"key1", 50, ValueType::kValue}, "val50");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // With current_time = 50 and retention_window = 20:
    // retention_cutoff = 50 - 20 = 30.
    // Inside retention window: v50 (newest) and v40 (>= 30) -> kept!
    // Older than retention window: v25 (< 30) and v10 (< 30) -> purged!
    // For key2: only v10 exists (newest version) -> kept!
    ASSERT_TRUE(lm.CompactLevel(0, /*current_time=*/ 50).ok());

    EXPECT_EQ(lm.NumFiles(0), 0U);
    EXPECT_GE(lm.NumFiles(1), 1U);

    InternalKey out_key;
    std::string out_val;

    // key1 @ 50 -> v50 survives
    ASSERT_TRUE(lm.GetAsOf("key1", 50, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 50U);
    EXPECT_EQ(out_val, "val50");

    // key1 @ 45 -> v40 survives
    ASSERT_TRUE(lm.GetAsOf("key1", 45, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 40U);
    EXPECT_EQ(out_val, "val40");

    // key1 @ 40 -> v40 survives
    ASSERT_TRUE(lm.GetAsOf("key1", 40, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 40U);
    EXPECT_EQ(out_val, "val40");

    // key1 @ 25 -> v25 was purged!
    EXPECT_TRUE(lm.GetAsOf("key1", 25, &out_key, &out_val).is_not_found());

    // key1 @ 10 -> v10 was purged!
    EXPECT_TRUE(lm.GetAsOf("key1", 10, &out_key, &out_val).is_not_found());

    // key2 @ 50 -> v10 survives because it is the latest/only version of key2
    ASSERT_TRUE(lm.GetAsOf("key2", 50, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 10U);
    EXPECT_EQ(out_val, "val2_10");
}

TEST_F(CompactionTest, TombstoneCleanupAtBottomLevel) {
    CompactionOptions opts;
    opts.l0_trigger = 10;
    opts.retention_window = 20;

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
    lm.StopBackgroundThread();

    // Table 1: keyX inserted at ts 10
    {
        MemTable mem;
        mem.Put({"keyX", 10, ValueType::kValue}, "original_value");
        mem.Put({"other", 10, ValueType::kValue}, "keep_me");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Table 2: keyX deleted at ts 20 (tombstone)
    {
        MemTable mem;
        mem.Put({"keyX", 20, ValueType::kTombstone}, "");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Compact at time 100 past the retention window (cutoff = 100 - 20 = 80).
    // keyX was deleted at ts 20 (<= 80) and no older versions exist below L1.
    // Both the tombstone and the shadowed value must be completely purged.
    ASSERT_TRUE(lm.CompactLevel(0, /*current_time=*/ 100).ok());

    InternalKey out_key;
    std::string out_val;

    // Point lookup for keyX must return NotFound (not a tombstone Ok)
    EXPECT_TRUE(lm.GetAsOf("keyX", 100, &out_key, &out_val).is_not_found());

    // "other" key must still exist
    ASSERT_TRUE(lm.GetAsOf("other", 100, &out_key, &out_val).ok());
    EXPECT_EQ(out_val, "keep_me");

    // Scan through all entries on disk: verify keyX does not exist in any form
    auto iter = lm.NewIterator();
    ASSERT_NE(iter, nullptr);
    iter->SeekToFirst();
    size_t count = 0;
    while (iter->Valid()) {
        EXPECT_NE(iter->Key().user_key, "keyX");
        count++;
        iter->Next();
    }
    EXPECT_EQ(count, 1U); // only "other" exists
}

TEST_F(CompactionTest, RestartRecoveryFromManifestAfterCompaction) {
    CompactionOptions opts;
    opts.l0_trigger = 10;
    opts.retention_window = 1000;

    std::vector<uint64_t> post_compaction_l1_ids;

    // Phase 1: Flush, compact, and record active files
    {
        LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
        lm.StopBackgroundThread();

        for (int i = 0; i < 4; ++i) {
            MemTable mem;
            mem.Put({"user_" + std::to_string(i), static_cast<Timestamp>(i + 1), ValueType::kValue},
                    "data_" + std::to_string(i));
            auto it = mem.NewIterator();
            ASSERT_TRUE(lm.Flush(*it).ok());
        }

        EXPECT_EQ(lm.NumFiles(0), 4U);
        ASSERT_TRUE(lm.CompactLevel(0).ok());

        EXPECT_EQ(lm.NumFiles(0), 0U);
        EXPECT_GE(lm.NumFiles(1), 1U);

        for (const auto& meta : lm.GetLevelFiles(1)) {
            post_compaction_l1_ids.push_back(meta->file_id);
        }
    }

    // Phase 2: Recover from MANIFEST in a new LevelManager instance
    {
        LevelManager lm_recovered(DbDir(), 1024 * 1024, 7, 10, opts);
        lm_recovered.StopBackgroundThread();

        EXPECT_EQ(lm_recovered.NumFiles(0), 0U);
        ASSERT_EQ(lm_recovered.NumFiles(1), post_compaction_l1_ids.size());

        auto recovered_l1 = lm_recovered.GetLevelFiles(1);
        for (size_t i = 0; i < post_compaction_l1_ids.size(); ++i) {
            EXPECT_EQ(recovered_l1[i]->file_id, post_compaction_l1_ids[i]);
        }

        // All keys must be readable
        for (int i = 0; i < 4; ++i) {
            InternalKey out_key;
            std::string out_val;
            ASSERT_TRUE(lm_recovered.GetAsOf("user_" + std::to_string(i), 100, &out_key, &out_val).ok());
            EXPECT_EQ(out_val, "data_" + std::to_string(i));
        }
    }
}

TEST_F(CompactionTest, AutomaticBackgroundCompaction) {
    CompactionOptions opts;
    opts.l0_trigger = 4;
    opts.retention_window = 1000;

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);

    // Flush 4 MemTables to trigger background compaction
    for (int i = 0; i < 4; ++i) {
        MemTable mem;
        mem.Put({"bg_key_" + std::to_string(i), static_cast<Timestamp>(i + 1), ValueType::kValue}, "bg_val");
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // Wait for the background thread to finish compaction
    lm.WaitForCompaction();

    // L0 should have been compacted into L1
    EXPECT_LT(lm.NumFiles(0), 4U);
    EXPECT_GE(lm.NumFiles(1), 1U);

    // Verify all keys remain accessible
    for (int i = 0; i < 4; ++i) {
        InternalKey out_key;
        std::string out_val;
        ASSERT_TRUE(lm.GetAsOf("bg_key_" + std::to_string(i), 100, &out_key, &out_val).ok());
        EXPECT_EQ(out_val, "bg_val");
    }
}

TEST_F(CompactionTest, TargetFileSizeSplitsOutputIntoMultipleL1Files) {
    CompactionOptions opts;
    opts.l0_trigger = 10;
    opts.retention_window = 1000;
    // Set a small target file size of 512 bytes so output splits into multiple SSTables
    opts.target_file_size = 512;

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
    lm.StopBackgroundThread();

    // Write enough data to cross 512 bytes across keys
    {
        MemTable mem;
        for (int i = 0; i < 30; ++i) {
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "k%04d", i);
            mem.Put({key_buf, static_cast<Timestamp>(i + 1), ValueType::kValue}, std::string(50, 'x'));
        }
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    ASSERT_TRUE(lm.CompactLevel(0).ok());

    EXPECT_EQ(lm.NumFiles(0), 0U);
    EXPECT_GT(lm.NumFiles(1), 1U); // Must have split into multiple L1 SSTables

    // Verify non-overlapping invariant in L1
    auto l1_files = lm.GetLevelFiles(1);
    for (size_t i = 0; i + 1 < l1_files.size(); ++i) {
        EXPECT_LT(l1_files[i]->max_key.user_key, l1_files[i + 1]->min_key.user_key);
    }

    // Verify all keys can be read
    for (int i = 0; i < 30; ++i) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "k%04d", i);
        InternalKey out_key;
        std::string out_val;
        ASSERT_TRUE(lm.GetAsOf(key_buf, 100, &out_key, &out_val).ok());
        EXPECT_EQ(out_val, std::string(50, 'x'));
    }
}

TEST_F(CompactionTest, MultiLevelLiToLiPlusOneCompaction) {
    CompactionOptions opts;
    opts.l0_trigger = 10;
    opts.retention_window = 1000;
    opts.level_size_budget = {
        0,     // L0
        100,   // L1 budget is only 100 bytes (will trigger L1 -> L2 compaction)
        10000, // L2
        100000 // L3
    };

    LevelManager lm(DbDir(), 1024 * 1024, 7, 10, opts);
    lm.StopBackgroundThread();

    // 1. Create data and compact to L1
    {
        MemTable mem;
        for (int i = 0; i < 20; ++i) {
            mem.Put({"lev_key_" + std::to_string(i), static_cast<Timestamp>(i + 1), ValueType::kValue},
                    std::string(40, 'a'));
        }
        auto it = mem.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    ASSERT_TRUE(lm.CompactLevel(0).ok());
    EXPECT_EQ(lm.NumFiles(0), 0U);
    EXPECT_GE(lm.NumFiles(1), 1U);
    EXPECT_EQ(lm.NumFiles(2), 0U);

    // L1 size should exceed 100 bytes
    EXPECT_GT(lm.TotalLevelSize(1), 100U);

    // 2. Compact L1 -> L2
    ASSERT_TRUE(lm.CompactLevel(1).ok());

    // L2 should now contain the compacted files
    EXPECT_GE(lm.NumFiles(2), 1U);

    // All keys must still be retrievable
    for (int i = 0; i < 20; ++i) {
        InternalKey out_key;
        std::string out_val;
        ASSERT_TRUE(lm.GetAsOf("lev_key_" + std::to_string(i), 100, &out_key, &out_val).ok());
        EXPECT_EQ(out_val, std::string(40, 'a'));
    }
}

TEST_F(CompactionTest, DBImplWithCompactionEndToEnd) {
    const std::string db_dir = DbDir();
    const std::string wal_path = db_dir + "/wal.log";

    CompactionOptions comp_opts;
    comp_opts.l0_trigger = 3;
    comp_opts.retention_window = 1000;

    auto lm = std::make_shared<LevelManager>(db_dir, 1024 * 1024, 7, 10, comp_opts);

    DBOptions options;
    options.wal_path = wal_path;
    options.memtable_limit = 200; // Small limit to trigger frequent flushes from MemTable
    options.disk = lm;

    // 1. Initial run: writes through Person A's DBImpl API, flushes to L0, compactor moves to L1
    {
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());

        for (int i = 0; i < 60; ++i) {
            std::string key = "e2e_key_" + std::to_string(i);
            std::string val = "e2e_val_" + std::to_string(i);
            ASSERT_TRUE(db.Put(key, val).ok());
        }

        // Wait for Person A's background flush thread
        ASSERT_TRUE(db.WaitForFlush().ok());

        // Wait for Person B's background compaction worker
        lm->WaitForCompaction();

        // Verify compaction occurred: L1 now contains files
        EXPECT_GE(lm->NumFiles(1), 1U);

        // Read through Person A's Get() API (reads through MemTable -> DiskStorage / LevelManager -> SSTables)
        for (int i = 0; i < 60; ++i) {
            std::string key = "e2e_key_" + std::to_string(i);
            std::string expected_val = "e2e_val_" + std::to_string(i);
            std::string actual_val;
            ASSERT_TRUE(db.Get(key, &actual_val).ok());
            EXPECT_EQ(actual_val, expected_val);
        }

        // Test range scan through Person A's Scan() API across the compacted SSTables
        std::vector<std::pair<std::string, std::string>> scan_results;
        ASSERT_TRUE(db.Scan("e2e_key_0", "e2e_key_999", &scan_results).ok());
        EXPECT_EQ(scan_results.size(), 60U);
    }

    // 2. Restart verification: reopen DBImpl with a fresh LevelManager on the same folder
    {
        auto lm_reopened = std::make_shared<LevelManager>(db_dir, 1024 * 1024, 7, 10, comp_opts);
        EXPECT_GE(lm_reopened->NumFiles(1), 1U);

        DBOptions options_reopened;
        options_reopened.wal_path = wal_path;
        options_reopened.disk = lm_reopened;

        DBImpl db_reopened(options_reopened);
        ASSERT_TRUE(db_reopened.Open().ok());

        // Verify points and range scans continue working after restart
        std::string val;
        EXPECT_TRUE(db_reopened.Get("e2e_key_42", &val).ok());
        EXPECT_EQ(val, "e2e_val_42");

        std::vector<std::pair<std::string, std::string>> scan_results;
        ASSERT_TRUE(db_reopened.Scan("e2e_key_0", "e2e_key_999", &scan_results).ok());
        EXPECT_EQ(scan_results.size(), 60U);
    }
}

} // namespace kvstore
