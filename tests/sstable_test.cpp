#include "storage/sstable.hpp"
#include "kvstore/memtable.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace kvstore {

class SSTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        test_dir_ = std::filesystem::temp_directory_path() / ("sstable_test_" + std::to_string(now));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    std::string TempFilePath(const std::string& filename) {
        return (test_dir_ / filename).string();
    }

    std::filesystem::path test_dir_;
};

TEST_F(SSTableTest, MultiBlockBuildAndPointLookupsFromMemTable) {
    const std::string sst_path = TempFilePath("multi_block.sst");
    MemTable memtable;

    constexpr size_t kNumEntries = 2500;
    for (size_t i = 0; i < kNumEntries; ++i) {
        // Form padded keys: key_0000, key_0001, ...
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key_%04zu", i);
        std::string key_str(buf);
        // ~120 byte values to ensure multiple 4 KB data blocks (~70+ blocks)
        std::string val_str = "value_for_" + key_str + "_" + std::string(100, 'x');

        memtable.Put(InternalKey{key_str, 100, ValueType::kValue}, val_str);
    }

    auto iter = memtable.NewIterator();
    Status build_status = SSTableBuilder::BuildFromIterator(sst_path, iter.get(), 10);
    ASSERT_TRUE(build_status.ok()) << build_status.message;

    std::unique_ptr<SSTableReader> reader;
    Status open_status = SSTableReader::Open(sst_path, &reader);
    ASSERT_TRUE(open_status.ok()) << open_status.message;
    ASSERT_NE(reader, nullptr);

    // Verify multiple data blocks exist
    EXPECT_GT(reader->block_count(), 50U);
    EXPECT_GT(reader->file_size(), 300000U);
    EXPECT_EQ(reader->MaxVersion(), 100U);

    // Verify Bloom filter presence for all inserted keys
    for (size_t i = 0; i < kNumEntries; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key_%04zu", i);
        EXPECT_TRUE(reader->MayContain(buf)) << "Bloom filter missing key: " << buf;
    }

    // Verify Bloom filter returns false for non-existent keys (with < 2% FP rate)
    size_t false_positives = 0;
    for (size_t i = 0; i < kNumEntries; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "non_existent_%04zu", i);
        if (reader->MayContain(buf)) {
            false_positives++;
        }
    }
    double fp_rate = static_cast<double>(false_positives) / static_cast<double>(kNumEntries);
    EXPECT_LT(fp_rate, 0.02);

    // Verify Point Lookups (Get) for all 2,500 entries across blocks
    for (size_t i = 0; i < kNumEntries; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key_%04zu", i);
        std::string expected_val = "value_for_" + std::string(buf) + "_" + std::string(100, 'x');

        std::string actual_val;
        Status s = reader->Get(buf, &actual_val, 150);
        ASSERT_TRUE(s.ok()) << "Failed to Get key " << buf << ": " << s.message;
        EXPECT_EQ(actual_val, expected_val);
    }

    // Verify Get for non-existent key returns NotFound
    std::string val;
    Status s_not_found = reader->Get("key_9999_absent", &val);
    EXPECT_TRUE(s_not_found.is_not_found());
}

TEST_F(SSTableTest, DirectAddAndMetadataTracking) {
    const std::string sst_path = TempFilePath("direct_add.sst");
    SSTableBuilder builder(sst_path, 10);

    EXPECT_TRUE(builder.Add({"alpha", 10, ValueType::kValue}, "val_alpha").ok());
    EXPECT_TRUE(builder.Add({"beta", 25, ValueType::kValue}, "val_beta").ok());
    EXPECT_TRUE(builder.Add({"gamma", 30, ValueType::kValue}, "val_gamma").ok());

    EXPECT_EQ(builder.min_key().user_key, "alpha");
    EXPECT_EQ(builder.max_key().user_key, "gamma");
    EXPECT_EQ(builder.MaxVersion(), 30U);
    EXPECT_EQ(builder.entry_count(), 3U);

    // Out-of-order Add should fail
    Status out_of_order = builder.Add({"alpha", 5, ValueType::kValue}, "err");
    EXPECT_EQ(out_of_order.code, Status::Code::kInvalidArgument);

    ASSERT_TRUE(builder.Finish().ok());
    EXPECT_GT(builder.file_size(), 48U);

    std::unique_ptr<SSTableReader> reader;
    ASSERT_TRUE(SSTableReader::Open(sst_path, &reader).ok());
    EXPECT_EQ(reader->MaxVersion(), 30U);

    std::string val;
    EXPECT_TRUE(reader->Get("alpha", &val).ok());
    EXPECT_EQ(val, "val_alpha");
    EXPECT_TRUE(reader->Get("beta", &val).ok());
    EXPECT_EQ(val, "val_beta");
    EXPECT_TRUE(reader->Get("gamma", &val).ok());
    EXPECT_EQ(val, "val_gamma");
}

TEST_F(SSTableTest, DiskStorageGetAsOfSemanticsWithTombstones) {
    const std::string sst_path = TempFilePath("get_as_of.sst");
    SSTableBuilder builder(sst_path, 10);

    // "doc" versions: 100, 50, 20
    EXPECT_TRUE(builder.Add({"doc", 100, ValueType::kValue}, "v100").ok());
    EXPECT_TRUE(builder.Add({"doc", 50, ValueType::kValue}, "v50").ok());
    EXPECT_TRUE(builder.Add({"doc", 20, ValueType::kValue}, "v20").ok());

    // "user_profile" created at 10, deleted at 40 (tombstone), recreated at 80
    EXPECT_TRUE(builder.Add({"user_profile", 80, ValueType::kValue}, "recreated").ok());
    EXPECT_TRUE(builder.Add({"user_profile", 40, ValueType::kTombstone}, "").ok());
    EXPECT_TRUE(builder.Add({"user_profile", 10, ValueType::kValue}, "initial").ok());

    ASSERT_TRUE(builder.Finish().ok());
    EXPECT_EQ(builder.MaxVersion(), 100U);

    std::unique_ptr<SSTableReader> reader;
    ASSERT_TRUE(SSTableReader::Open(sst_path, &reader).ok());
    EXPECT_EQ(reader->MaxVersion(), 100U);

    InternalKey out_key;
    std::string out_val;

    // 1. GetAsOf on live key at various timestamps
    ASSERT_TRUE(reader->GetAsOf("doc", 200, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 100U);
    EXPECT_EQ(out_key.type, ValueType::kValue);
    EXPECT_EQ(out_val, "v100");

    ASSERT_TRUE(reader->GetAsOf("doc", 75, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 50U);
    EXPECT_EQ(out_key.type, ValueType::kValue);
    EXPECT_EQ(out_val, "v50");

    ASSERT_TRUE(reader->GetAsOf("doc", 25, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 20U);
    EXPECT_EQ(out_key.type, ValueType::kValue);
    EXPECT_EQ(out_val, "v20");

    EXPECT_TRUE(reader->GetAsOf("doc", 10, &out_key, &out_val).is_not_found());

    // 2. GetAsOf on tombstone key:
    // At ts 50: newest version is 40 (kTombstone). DiskStorage requires GetAsOf to return Ok()
    // with out_key populated so the caller knows the exact version and that it is a tombstone!
    ASSERT_TRUE(reader->GetAsOf("user_profile", 50, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 40U);
    EXPECT_EQ(out_key.type, ValueType::kTombstone);

    // At ts 40: exact tombstone
    ASSERT_TRUE(reader->GetAsOf("user_profile", 40, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 40U);
    EXPECT_EQ(out_key.type, ValueType::kTombstone);

    // At ts 20: alive
    ASSERT_TRUE(reader->GetAsOf("user_profile", 20, &out_key, &out_val).ok());
    EXPECT_EQ(out_key.version, 10U);
    EXPECT_EQ(out_key.type, ValueType::kValue);
    EXPECT_EQ(out_val, "initial");

    // At ts 5: not found
    EXPECT_TRUE(reader->GetAsOf("user_profile", 5, &out_key, &out_val).is_not_found());

    // 3. Contrast with Get() which hides tombstones by returning NotFound
    std::string val;
    EXPECT_TRUE(reader->Get("user_profile", &val, 50).is_not_found());
    EXPECT_TRUE(reader->Get("user_profile", &val, 20).ok());
    EXPECT_EQ(val, "initial");

    // 4. Null argument check
    EXPECT_EQ(reader->GetAsOf("doc", 100, nullptr, &out_val).code, Status::Code::kInvalidArgument);
    EXPECT_EQ(reader->GetAsOf("doc", 100, &out_key, nullptr).code, Status::Code::kInvalidArgument);
}

TEST_F(SSTableTest, RangeIteratorOperations) {
    const std::string sst_path = TempFilePath("iterator.sst");
    SSTableBuilder builder(sst_path, 10);

    constexpr size_t kTotal = 1000;
    std::vector<InternalKey> expected_keys;
    std::vector<std::string> expected_values;
    expected_keys.reserve(kTotal);
    expected_values.reserve(kTotal);

    for (size_t i = 0; i < kTotal; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "item_%04zu", i);
        InternalKey ikey{buf, i + 1, ValueType::kValue};
        std::string val = "val_" + std::string(buf);
        expected_keys.push_back(ikey);
        expected_values.push_back(val);
        EXPECT_TRUE(builder.Add(ikey, val).ok());
    }
    ASSERT_TRUE(builder.Finish().ok());
    EXPECT_EQ(builder.MaxVersion(), kTotal);

    std::unique_ptr<SSTableReader> reader;
    ASSERT_TRUE(SSTableReader::Open(sst_path, &reader).ok());
    EXPECT_EQ(reader->MaxVersion(), kTotal);

    auto it = reader->NewIterator();

    // 1. SeekToFirst and full iteration
    it->SeekToFirst();
    size_t count = 0;
    while (it->Valid()) {
        ASSERT_LT(count, kTotal);
        EXPECT_EQ(it->Key(), expected_keys[count]);
        EXPECT_EQ(it->Value(), expected_values[count]);
        it->Next();
        count++;
    }
    EXPECT_EQ(count, kTotal);
    EXPECT_FALSE(it->Valid());

    // 2. Seek to exact key
    InternalKey target_exact{"item_0450", 451, ValueType::kValue};
    it->Seek(target_exact);
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), target_exact);
    EXPECT_EQ(it->Value(), "val_item_0450");

    // 3. Seek between keys (target "item_0450a" between "item_0450" and "item_0451")
    InternalKey target_between{"item_0450a", 1, ValueType::kValue};
    it->Seek(target_between);
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key().user_key, "item_0451");

    // 4. Seek past the end of the file
    InternalKey target_past_end{"zzzz", 1, ValueType::kValue};
    it->Seek(target_past_end);
    EXPECT_FALSE(it->Valid());

    // 5. Seek before first key
    InternalKey target_before_start{"aaaa", 1, ValueType::kValue};
    it->Seek(target_before_start);
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key().user_key, "item_0000");
}

TEST_F(SSTableTest, ErrorHandlingAndCorruptFiles) {
    // Non-existent file
    std::unique_ptr<SSTableReader> reader;
    Status s_missing = SSTableReader::Open(TempFilePath("missing.sst"), &reader);
    EXPECT_EQ(s_missing.code, Status::Code::kIOError);

    // File smaller than footer
    const std::string small_path = TempFilePath("small.sst");
    {
        std::ofstream out(small_path, std::ios::binary);
        out << "short content";
    }
    Status s_small = SSTableReader::Open(small_path, &reader);
    EXPECT_EQ(s_small.code, Status::Code::kCorruption);

    // File with bad magic number
    const std::string bad_magic_path = TempFilePath("bad_magic.sst");
    {
        std::string bad_data(kFooterSize, '\0');
        // Bad magic number at the end (bytes 40..47)
        bad_data[kFooterSize - 8] = 'B';
        bad_data[kFooterSize - 7] = 'A';
        bad_data[kFooterSize - 6] = 'D';
        std::ofstream out(bad_magic_path, std::ios::binary);
        out.write(bad_data.data(), bad_data.size());
    }
    Status s_bad_magic = SSTableReader::Open(bad_magic_path, &reader);
    EXPECT_EQ(s_bad_magic.code, Status::Code::kCorruption);
}

} // namespace kvstore
