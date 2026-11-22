#include "kvstore/wal.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>

namespace kvstore {
namespace {
class TempWal {
public:
    TempWal() : path(std::filesystem::temp_directory_path()/
        ("kvstore-wal-"+std::to_string(::getpid())+"-"+std::to_string(next++))) {}
    ~TempWal(){ std::error_code ec; std::filesystem::remove(path,ec); }
    std::filesystem::path path;
    static inline int next=0;
};
}

TEST(WALTest, AppendsAndReplaysVersionedRecords) {
    TempWal file;
    { WAL wal(file.path.string()); ASSERT_TRUE(wal.Open().ok());
      ASSERT_TRUE(wal.Append({"alpha",4,ValueType::kValue},"four").ok());
      ASSERT_TRUE(wal.Append({"alpha",9,ValueType::kValue},"nine").ok());
      ASSERT_TRUE(wal.Append({"gone",10,ValueType::kTombstone},"").ok()); }
    WAL wal(file.path.string()); MemTable table;
    ASSERT_TRUE(wal.Replay(&table).ok());
    std::string value;
    EXPECT_TRUE(table.GetAsOf("alpha",5,&value).ok()); EXPECT_EQ(value,"four");
    EXPECT_TRUE(table.Get("alpha",&value).ok()); EXPECT_EQ(value,"nine");
    EXPECT_TRUE(table.Get("gone",&value).is_not_found());
}

TEST(WALTest, TruncatesIncompleteTailAndKeepsValidPrefix) {
    TempWal file;
    { WAL wal(file.path.string()); ASSERT_TRUE(wal.Append({"k",1,ValueType::kValue},"v").ok()); }
    { std::ofstream out(file.path,std::ios::binary|std::ios::app); out << "partial"; }
    WAL wal(file.path.string()); MemTable table;
    ASSERT_TRUE(wal.Replay(&table).ok());
    std::string value; ASSERT_TRUE(table.Get("k",&value).ok()); EXPECT_EQ(value,"v");
    auto repaired_size=std::filesystem::file_size(file.path);
    EXPECT_TRUE(wal.Append({"next",2,ValueType::kValue},"ok").ok());
    EXPECT_GT(std::filesystem::file_size(file.path),repaired_size);
}

TEST(WALTest, DetectsCorruptRecordAndDropsItAndFollowingTail) {
    TempWal file;
    { WAL wal(file.path.string()); ASSERT_TRUE(wal.Append({"first",1,ValueType::kValue},"a").ok());
      ASSERT_TRUE(wal.Append({"second",2,ValueType::kValue},"b").ok()); }
    { std::fstream io(file.path,std::ios::binary|std::ios::in|std::ios::out);
      io.seekp(24+34); char byte='X'; io.write(&byte,1); }
    WAL wal(file.path.string()); MemTable table;
    ASSERT_TRUE(wal.Replay(&table).ok());
    std::string value; EXPECT_TRUE(table.Get("first",&value).ok());
    EXPECT_TRUE(table.Get("second",&value).is_not_found());
}

TEST(WALTest, RejectsNullReplayTarget) {
    TempWal file; WAL wal(file.path.string());
    EXPECT_EQ(wal.Replay(nullptr).code,Status::Code::kInvalidArgument);
}
}
