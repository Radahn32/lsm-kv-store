#include "kvstore/vlog.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace kvstore {
namespace {

class VLogFixture : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() /
               ("vlog-test-" + std::to_string(::getpid()) + "-" +
                std::to_string(++counter_));
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    std::filesystem::path dir_;
    static inline int counter_ = 0;
};

TEST(VLogTest, ValuePointerRoundTripAndInvalidLengths) {
    const ValuePointer original{0x12345678U, 0x0123456789abcdefULL, 0xfedcba98U};
    const std::string encoded = EncodeValuePointer(original);
    ASSERT_EQ(encoded.size(), 16U);
    ValuePointer decoded;
    ASSERT_TRUE(DecodeValuePointer(encoded, &decoded).ok());
    EXPECT_EQ(decoded.file_id, original.file_id);
    EXPECT_EQ(decoded.offset, original.offset);
    EXPECT_EQ(decoded.size, original.size);
    EXPECT_EQ(DecodeValuePointer("short", &decoded).code, Status::Code::kCorruption);
}

TEST_F(VLogFixture, AppendsReadsAndRotatesSegments) {
    VLog vlog(dir_.string(), 40);
    ASSERT_TRUE(vlog.Open().ok());
    ValuePointer first;
    ValuePointer second;
    ASSERT_TRUE(vlog.Append("k", 1, "value-one", &first).ok());
    ASSERT_TRUE(vlog.Append("k", 2, "value-two", &second).ok());
    EXPECT_EQ(first.file_id, 1U);
    EXPECT_EQ(second.file_id, 2U);
    EXPECT_EQ(vlog.SealedSegmentIds(), (std::vector<uint32_t>{1U}));

    std::string value;
    ASSERT_TRUE(vlog.Read(first, &value).ok());
    EXPECT_EQ(value, "value-one");
    ASSERT_TRUE(vlog.Read(second, &value).ok());
    EXPECT_EQ(value, "value-two");
}

TEST_F(VLogFixture, AcceptsNumericStemsOfAnyWidth) {
    ValuePointer original;
    {
        VLog vlog(dir_.string(), 1024);
        ASSERT_TRUE(vlog.Open().ok());
        ASSERT_TRUE(vlog.Append("key", 1, "original", &original).ok());
    }
    std::filesystem::rename(dir_ / "000001.vlog", dir_ / "1.vlog");

    VLog reopened(dir_.string(), 1024);
    ASSERT_TRUE(reopened.Open().ok());
    std::string value;
    ASSERT_TRUE(reopened.Read(original, &value).ok());
    EXPECT_EQ(value, "original");
    ValuePointer appended;
    ASSERT_TRUE(reopened.Append("key", 2, "appended", &appended).ok());
    EXPECT_EQ(appended.file_id, 1U);
    ASSERT_TRUE(reopened.Read(appended, &value).ok());
    EXPECT_EQ(value, "appended");
}

TEST_F(VLogFixture, DetectsChecksumCorruptionAndRepairsTornTail) {
    ValuePointer pointer;
    {
        VLog vlog(dir_.string(), 1024);
        ASSERT_TRUE(vlog.Open().ok());
        ASSERT_TRUE(vlog.Append("key", 7, "payload", &pointer).ok());
        const auto path = dir_ / "000001.vlog";
        std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.is_open());
        file.seekg(20);
        char byte = 0;
        file.read(&byte, 1);
        byte ^= 0x01;
        file.seekp(20);
        file.write(&byte, 1);
        file.flush();
        std::string value;
        EXPECT_EQ(vlog.Read(pointer, &value).code, Status::Code::kCorruption);
    }
    const auto path = dir_ / "000001.vlog";
    {
        VLog vlog(dir_.string(), 1024);
        ASSERT_TRUE(vlog.Open().ok());
    }

    std::filesystem::remove(path);
    {
        VLog vlog(dir_.string(), 1024);
        ASSERT_TRUE(vlog.Open().ok());
        ValuePointer valid;
        ASSERT_TRUE(vlog.Append("key", 1, "valid", &valid).ok());
    }
    const auto clean_size = std::filesystem::file_size(path);
    {
        std::ofstream file(path, std::ios::binary | std::ios::app);
        file.write("torn-tail", 9);
    }
    {
        VLog vlog(dir_.string(), 1024);
        ASSERT_TRUE(vlog.Open().ok());
    }
    EXPECT_EQ(std::filesystem::file_size(path), clean_size);
}

TEST_F(VLogFixture, ScansSegmentsRemovesSealedAndRejectsActiveRemoval) {
    VLog vlog(dir_.string(), 40);
    ASSERT_TRUE(vlog.Open().ok());
    ValuePointer first;
    ValuePointer second;
    ASSERT_TRUE(vlog.Append("first", 1, "value-one", &first).ok());
    ASSERT_TRUE(vlog.Append("second", 2, "value-two", &second).ok());

    std::vector<VLogRecord> records;
    ASSERT_TRUE(vlog.ScanSegment(first.file_id, [&](const VLogRecord& record) {
        records.push_back(record);
        return Status::Ok();
    }).ok());
    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0].user_key, "first");
    EXPECT_EQ(records[0].version, 1U);
    EXPECT_EQ(records[0].value, "value-one");
    EXPECT_EQ(records[0].pointer.file_id, first.file_id);
    EXPECT_EQ(vlog.RemoveSegment(second.file_id).code, Status::Code::kInvalidArgument);
    ASSERT_TRUE(vlog.RemoveSegment(first.file_id).ok());
    EXPECT_FALSE(std::filesystem::exists(dir_ / "000001.vlog"));
}

TEST_F(VLogFixture, SupportsConcurrentReadersAndWriter) {
    VLog vlog(dir_.string(), 1024 * 1024);
    ASSERT_TRUE(vlog.Open().ok());
    ValuePointer initial;
    ASSERT_TRUE(vlog.Append("key", 0, "initial", &initial).ok());
    std::atomic<bool> failed = false;
    std::thread reader([&] {
        for (int i = 0; i < 200; ++i) {
            std::string value;
            if (!vlog.Read(initial, &value).ok() || value != "initial") failed = true;
        }
    });
    std::thread writer([&] {
        for (int i = 1; i <= 100; ++i) {
            ValuePointer pointer;
            if (!vlog.Append("key", static_cast<Timestamp>(i), "value", &pointer).ok()) {
                failed = true;
            }
        }
    });
    reader.join();
    writer.join();
    EXPECT_FALSE(failed.load());
}

} // namespace
} // namespace kvstore
