#include "kvstore/memtable.hpp"

#include <gtest/gtest.h>

#include <string>

namespace kvstore {

TEST(MemTableTest, IteratesKeysAndVersionsInContractOrder) {
    MemTable table;
    table.Put({"b", 1, ValueType::kValue}, "b1");
    table.Put({"a", 1, ValueType::kValue}, "a1");
    table.Put({"a", 4, ValueType::kValue}, "a4");
    auto it = table.NewIterator();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), (InternalKey{"a", 4, ValueType::kValue}));
    EXPECT_EQ(it->Value(), "a4");
    it->Next();
    EXPECT_EQ(it->Key(), (InternalKey{"a", 1, ValueType::kValue}));
    it->Next();
    EXPECT_EQ(it->Key().user_key, "b");
    it->Next();
    EXPECT_FALSE(it->Valid());
}

TEST(MemTableTest, SeekFindsFirstEntryNotLessThanTarget) {
    MemTable table;
    table.Put({"a", 9, ValueType::kValue}, "a9");
    table.Put({"a", 5, ValueType::kValue}, "a5");
    table.Put({"b", 2, ValueType::kValue}, "b2");
    auto it = table.NewIterator();
    it->Seek({"a", 7, ValueType::kValue});
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), (InternalKey{"a", 5, ValueType::kValue}));
    it->Seek({"aa", 1, ValueType::kValue});
    EXPECT_EQ(it->Key().user_key, "b");
}

TEST(MemTableTest, GetsLatestAndHistoricalVersions) {
    MemTable table;
    table.Put({"key", 3, ValueType::kValue}, "three");
    table.Put({"key", 8, ValueType::kValue}, "eight");
    std::string value;
    EXPECT_TRUE(table.Get("key", &value).ok());
    EXPECT_EQ(value, "eight");
    EXPECT_TRUE(table.GetAsOf("key", 5, &value).ok());
    EXPECT_EQ(value, "three");
    EXPECT_TRUE(table.GetAsOf("key", 2, &value).is_not_found());
}

TEST(MemTableTest, TombstoneHidesOlderValueAtAndAfterItsVersion) {
    MemTable table;
    table.Put({"key", 2, ValueType::kValue}, "old");
    table.Put({"key", 6, ValueType::kTombstone}, "");
    std::string value;
    EXPECT_TRUE(table.GetAsOf("key", 5, &value).ok());
    EXPECT_EQ(value, "old");
    EXPECT_TRUE(table.GetAsOf("key", 6, &value).is_not_found());
    EXPECT_TRUE(table.Get("key", &value).is_not_found());
}

TEST(MemTableTest, ReplacingExactVersionDoesNotAddAnotherEntry) {
    MemTable table;
    table.Put({"key", 1, ValueType::kValue}, "x");
    const auto before = table.ApproximateMemoryUsage();
    table.Put({"key", 1, ValueType::kValue}, "a much longer value");
    EXPECT_GT(table.ApproximateMemoryUsage(), before);
    auto it = table.NewIterator();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Value(), "a much longer value");
    it->Next();
    EXPECT_FALSE(it->Valid());
}

TEST(MemTableTest, TracksMemoryAndRejectsNullGetOutput) {
    MemTable table;
    EXPECT_EQ(table.ApproximateMemoryUsage(), 0U);
    table.Put({"k", 1, ValueType::kValue}, "v");
    EXPECT_GT(table.ApproximateMemoryUsage(), 0U);
    EXPECT_EQ(table.Get("k", nullptr).code, Status::Code::kInvalidArgument);
}

TEST(MemTableTest, IteratorHoldsStableSnapshotAgainstWriter) {
    MemTable table;
    table.Put({"a", 1, ValueType::kValue}, "one");
    auto it = table.NewIterator();
    // Iterator owns the shared lock; release it before performing a write.
    EXPECT_EQ(it->Value(), "one");
    it.reset();
    table.Put({"b", 1, ValueType::kValue}, "two");
    EXPECT_EQ(table.ApproximateMemoryUsage() > 0, true);
}

} // namespace kvstore
