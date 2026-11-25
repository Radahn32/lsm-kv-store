#include "storage/level_manager.hpp"
#include "kvstore/db_impl.hpp"
#include "kvstore/memtable.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

namespace kvstore {

class LevelManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        static std::atomic<uint64_t> counter{0};
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto instance = counter.fetch_add(1, std::memory_order_relaxed);
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("level_mgr_test_" + std::to_string(::getpid()) + "_" +
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

TEST_F(LevelManagerTest, FlushMultipleL0SSTablesAndGetAsOf) {
    LevelManager lm(DbDir(), 1024 * 1024);

    // 1. Flush MemTable 1 (older)
    {
        MemTable mem1;
        mem1.Put({"keyA", 10, ValueType::kValue}, "A_v10");
        mem1.Put({"keyB", 10, ValueType::kValue}, "B_v10");
        auto it = mem1.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // 2. Flush MemTable 2 (middle)
    {
        MemTable mem2;
        mem2.Put({"keyA", 20, ValueType::kValue}, "A_v20");
        mem2.Put({"keyC", 20, ValueType::kValue}, "C_v20");
        auto it = mem2.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    // 3. Flush MemTable 3 (newest, with tombstone)
    {
        MemTable mem3;
        mem3.Put({"keyB", 30, ValueType::kTombstone}, "");
        mem3.Put({"keyC", 30, ValueType::kValue}, "C_v30");
        auto it = mem3.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    EXPECT_EQ(lm.NumFiles(0), 3U);
    EXPECT_EQ(lm.MaxVersion(), 30U);

    InternalKey out_key;
    std::string out_val;

    // Test keyA lookups:
    // At ts 25 -> v20
    ASSERT_TRUE(lm.GetAsOf("keyA", 25, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 20U);
    EXPECT_EQ(out_val, "A_v20");

    // At ts 15 -> v10
    ASSERT_TRUE(lm.GetAsOf("keyA", 15, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 10U);
    EXPECT_EQ(out_val, "A_v10");

    // At ts 5 -> not found
    EXPECT_TRUE(lm.GetAsOf("keyA", 5, &out_key, &out_val).is_not_found());

    // Test keyB lookups:
    // At ts 35 -> tombstone at 30 (must return Ok with kTombstone)
    ASSERT_TRUE(lm.GetAsOf("keyB", 35, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 30U);
    EXPECT_EQ(out_key.type, ValueType::kTombstone);

    // At ts 20 -> v10
    ASSERT_TRUE(lm.GetAsOf("keyB", 20, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 10U);
    EXPECT_EQ(out_key.type, ValueType::kValue);
    EXPECT_EQ(out_val, "B_v10");

    // Test keyC lookups:
    // At ts 35 -> v30
    ASSERT_TRUE(lm.GetAsOf("keyC", 35, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 30U);
    EXPECT_EQ(out_val, "C_v30");

    // At ts 25 -> v20
    ASSERT_TRUE(lm.GetAsOf("keyC", 25, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 20U);
    EXPECT_EQ(out_val, "C_v20");

    // Non-existent key
    EXPECT_TRUE(lm.GetAsOf("non_existent", 100, &out_key, &out_val).is_not_found());
}

TEST_F(LevelManagerTest, NewIteratorGlobalSortedOrderAcrossL0) {
    LevelManager lm(DbDir(), 1024 * 1024);

    {
        MemTable mem1;
        mem1.Put({"a", 1, ValueType::kValue}, "a1");
        mem1.Put({"c", 1, ValueType::kValue}, "c1");
        auto it = mem1.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }
    {
        MemTable mem2;
        mem2.Put({"b", 2, ValueType::kValue}, "b2");
        mem2.Put({"d", 2, ValueType::kValue}, "d2");
        auto it = mem2.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }
    {
        MemTable mem3;
        mem3.Put({"a", 3, ValueType::kValue}, "a3");
        mem3.Put({"b", 3, ValueType::kValue}, "b3");
        auto it = mem3.NewIterator();
        ASSERT_TRUE(lm.Flush(*it).ok());
    }

    auto iter = lm.NewIterator();
    ASSERT_NE(iter, nullptr);

    // 1. SeekToFirst: should yield in exact (user_key ASC, version DESC) order
    iter->SeekToFirst();
    std::vector<std::pair<InternalKey, std::string>> actual;
    while (iter->Valid()) {
        actual.emplace_back(iter->Key(), std::string(iter->Value()));
        iter->Next();
    }

    std::vector<std::pair<InternalKey, std::string>> expected = {
        {{"a", 3, ValueType::kValue}, "a3"},
        {{"a", 1, ValueType::kValue}, "a1"},
        {{"b", 3, ValueType::kValue}, "b3"},
        {{"b", 2, ValueType::kValue}, "b2"},
        {{"c", 1, ValueType::kValue}, "c1"},
        {{"d", 2, ValueType::kValue}, "d2"}
    };

    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].first, expected[i].first);
        EXPECT_EQ(actual[i].second, expected[i].second);
    }

    // 2. Seek to exact key
    iter->Seek({"b", 2, ValueType::kValue});
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->Key(), (InternalKey{"b", 2, ValueType::kValue}));
    EXPECT_EQ(iter->Value(), "b2");

    // 3. Seek between keys
    iter->Seek({"bb", 1, ValueType::kValue});
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->Key().user_key, "c");

    // 4. Seek past end
    iter->Seek({"z", 1, ValueType::kValue});
    EXPECT_FALSE(iter->Valid());
}

TEST_F(LevelManagerTest, CrashRestartRecoveryFromManifest) {
    // Phase 1: Populate LevelManager with 2 SSTables
    {
        LevelManager lm(DbDir(), 1024 * 1024);
        MemTable mem1;
        mem1.Put({"user_1", 20, ValueType::kValue}, "alice");
        auto it1 = mem1.NewIterator();
        ASSERT_TRUE(lm.Flush(*it1).ok());

        MemTable mem2;
        mem2.Put({"user_2", 42, ValueType::kValue}, "bob");
        auto it2 = mem2.NewIterator();
        ASSERT_TRUE(lm.Flush(*it2).ok());

        EXPECT_EQ(lm.NumFiles(0), 2U);
        EXPECT_EQ(lm.MaxVersion(), 42U);
    }

    // Phase 2: Simulate crash and restart by opening a new LevelManager on the same directory
    {
        LevelManager lm_recovered(DbDir(), 1024 * 1024);
        EXPECT_EQ(lm_recovered.NumFiles(0), 2U);
        EXPECT_EQ(lm_recovered.MaxVersion(), 42U);

        InternalKey key;
        std::string val;
        ASSERT_TRUE(lm_recovered.GetAsOf("user_1", 50, &key, &val).ok());
        EXPECT_EQ(val, "alice");

        ASSERT_TRUE(lm_recovered.GetAsOf("user_2", 50, &key, &val).ok());
        EXPECT_EQ(val, "bob");

        // Flush a 3rd table and verify monotonic file_id continued
        MemTable mem3;
        mem3.Put({"user_3", 99, ValueType::kValue}, "charlie");
        auto it3 = mem3.NewIterator();
        ASSERT_TRUE(lm_recovered.Flush(*it3).ok());
        EXPECT_EQ(lm_recovered.NumFiles(0), 3U);
        EXPECT_EQ(lm_recovered.MaxVersion(), 99U);
    }

    // Phase 3: Reopen again, all 3 tables must survive
    {
        LevelManager lm_final(DbDir(), 1024 * 1024);
        EXPECT_EQ(lm_final.NumFiles(0), 3U);
        EXPECT_EQ(lm_final.MaxVersion(), 99U);

        InternalKey key;
        std::string val;
        ASSERT_TRUE(lm_final.GetAsOf("user_3", 100, &key, &val).ok());
        EXPECT_EQ(val, "charlie");
    }
}

TEST_F(LevelManagerTest, DBImplEndToEndIntegrationWithLevelManager) {
    const std::string db_dir = DbDir();
    const std::string wal_path = db_dir + "/wal.log";

    // 1. Initial DB instance: insert, update, delete, flush
    {
        auto lm = std::make_shared<LevelManager>(db_dir, 1024 * 1024);

        DBOptions options;
        options.wal_path = wal_path;
        options.memtable_limit = 256; // small limit to trigger automatic flushes
        options.disk = lm;

        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());

        ASSERT_TRUE(db.Put("user_100", "v1").ok());
        ASSERT_TRUE(db.Put("user_200", "v1").ok());

        // Insert enough items with 60-byte values to trigger multiple flushes to LevelManager
        for (int i = 0; i < 40; ++i) {
            ASSERT_TRUE(db.Put("bulk_" + std::to_string(i), std::string(60, 'x')).ok());
        }
        ASSERT_TRUE(db.WaitForFlush().ok());

        // Update user_100, delete user_200
        ASSERT_TRUE(db.Put("user_100", "v2").ok());
        ASSERT_TRUE(db.Delete("user_200").ok());

        for (int i = 40; i < 80; ++i) {
            ASSERT_TRUE(db.Put("bulk_" + std::to_string(i), std::string(60, 'y')).ok());
        }
        ASSERT_TRUE(db.WaitForFlush().ok());

        std::string val;
        EXPECT_TRUE(db.Get("user_100", &val).ok());
        EXPECT_EQ(val, "v2");

        EXPECT_TRUE(db.Get("user_200", &val).is_not_found());

        // LevelManager must have flushed files
        EXPECT_GT(lm->NumFiles(0) + lm->NumFiles(1), 0U);
    }

    // 2. Restart simulation: reopen DBImpl with a fresh LevelManager on the same directory
    auto lm_reopened = std::make_shared<LevelManager>(db_dir, 1024 * 1024);
    EXPECT_GT(lm_reopened->NumFiles(0) + lm_reopened->NumFiles(1), 0U);
    EXPECT_GT(lm_reopened->MaxVersion(), 0U);

    DBOptions options_reopened;
    options_reopened.wal_path = wal_path;
    options_reopened.disk = lm_reopened;

    DBImpl db_reopened(options_reopened);
    ASSERT_TRUE(db_reopened.Open().ok());

    std::string val;
    EXPECT_TRUE(db_reopened.Get("user_100", &val).ok());
    EXPECT_EQ(val, "v2");
    EXPECT_TRUE(db_reopened.Get("user_200", &val).is_not_found());

    // Scan across disk files
    std::vector<std::pair<std::string, std::string>> results;
    ASSERT_TRUE(db_reopened.Scan("user_000", "user_999", &results).ok());
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0].first, "user_100");
    EXPECT_EQ(results[0].second, "v2");
}

} // namespace kvstore
