#include "kvstore/db_impl.hpp"
#include "storage/level_manager.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
#include <limits>
#include <filesystem>
#include <map>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace kvstore {
namespace {

class VectorIterator final : public Iterator {
public:
    explicit VectorIterator(std::vector<std::pair<InternalKey, std::string>> entries)
        : entries_(std::move(entries)) {}
    bool Valid() const override { return index_ < entries_.size(); }
    void SeekToFirst() override { index_ = 0; }
    void Seek(const InternalKey& target) override {
        index_ = 0;
        while (Valid() && entries_[index_].first < target) ++index_;
    }
    void Next() override { if (Valid()) ++index_; }
    InternalKey Key() const override { return Valid() ? entries_[index_].first : InternalKey{}; }
    std::string_view Value() const override { return Valid() ? entries_[index_].second : std::string_view{}; }
private:
    std::vector<std::pair<InternalKey, std::string>> entries_;
    std::size_t index_ = 0;
};

class DiskMock : public DiskStorage {
public:
    Timestamp MaxVersion() const override {
        std::lock_guard lock(mutex_);
        Timestamp maximum = 0;
        for (const auto& [key, record] : records_) {
            (void)key;
            maximum = std::max(maximum, record.first.version);
        }
        return maximum;
    }
    Status GetAsOf(const std::string& key, Timestamp timestamp, InternalKey* output_key,
                   std::string* output_value) const override {
        std::lock_guard lock(mutex_);
        auto it = records_.find(key);
        if (it == records_.end() || it->second.first.version > timestamp) return Status::NotFound();
        *output_key = it->second.first;
        *output_value = it->second.second;
        return Status::Ok();
    }
    std::unique_ptr<Iterator> NewIterator() const override {
        std::lock_guard lock(mutex_);
        std::vector<std::pair<InternalKey, std::string>> entries;
        for (const auto& [key, record] : records_) {
            (void)key;
            entries.push_back(record);
        }
        return std::make_unique<VectorIterator>(std::move(entries));
    }
    Status Flush(Iterator& iterator) override {
        std::lock_guard lock(mutex_);
        if (fail_flush_) return Status::IOError("injected flush failure");
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next()) {
            InternalKey key = iterator.Key();
            records_[key.user_key] = {key, std::string(iterator.Value())};
        }
        return Status::Ok();
    }
    void Seed(std::string key, Timestamp version, ValueType type, std::string value) {
        std::lock_guard lock(mutex_);
        records_[key] = {{std::move(key), version, type}, std::move(value)};
    }
    void FailFlush(bool fail) { fail_flush_ = fail; }
private:
    mutable std::mutex mutex_;
    std::map<std::string, std::pair<InternalKey, std::string>> records_;
    bool fail_flush_ = false;
};

class Fixture {
public:
    Fixture() : path_("/tmp/db-test-" + std::to_string(::getpid()) + "-" +
                      std::to_string(++counter_) + ".wal") {}
    ~Fixture() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
        std::filesystem::remove_all(path_ + ".vlog", ec);
        std::filesystem::remove_all(path_ + ".disk", ec);
        const std::filesystem::path base(path_);
        const auto parent = base.has_parent_path() ? base.parent_path() : std::filesystem::path(".");
        const std::string prefix = base.filename().string() + ".";
        for (std::filesystem::directory_iterator it(parent, ec), end; !ec && it != end; it.increment(ec)) {
            const std::string filename = it->path().filename().string();
            if (filename.size() > prefix.size() &&
                filename.compare(0, prefix.size(), prefix) == 0) {
                std::filesystem::remove(it->path(), ec);
                ec.clear();
            }
        }
    }
    DBOptions Options(std::shared_ptr<DiskStorage> disk = {}, std::size_t limit = 100000) const {
        return {path_, limit, std::move(disk), {}, 1024, 16U * 1024U * 1024U};
    }
    const std::string& Path() const { return path_; }
private:
    std::string path_;
    static inline int counter_ = 0;
};

TEST(DBTest, CRUDTimeTravelDiffAndTombstones) {
    Fixture fixture;
    DBImpl db(fixture.Options());
    ASSERT_TRUE(db.Open().ok());
    ASSERT_TRUE(db.Put("k", "one").ok());
    ASSERT_TRUE(db.Put("k", "two").ok());
    std::string value;
    ASSERT_TRUE(db.GetAsOf("k", 1, &value).ok());
    EXPECT_EQ(value, "one");
    DiffResult diff;
    ASSERT_TRUE(db.Diff("k", 1, 2, &diff).ok());
    EXPECT_TRUE(diff.changed);
    EXPECT_EQ(*diff.old_value, "one");
    ASSERT_TRUE(db.Delete("k").ok());
    EXPECT_TRUE(db.Get("k", &value).is_not_found());
    EXPECT_TRUE(db.GetAsOf("k", 2, &value).ok());
}

TEST(DBTest, ReplaysAndContinuesMonotonicVersions) {
    Fixture fixture;
    {
        DBImpl db(fixture.Options());
        ASSERT_TRUE(db.Put("k", "one").ok());
        ASSERT_TRUE(db.Put("k", "two").ok());
    }
    DBImpl db(fixture.Options());
    ASSERT_TRUE(db.Open().ok());
    std::string value;
    ASSERT_TRUE(db.GetAsOf("k", 1, &value).ok());
    EXPECT_EQ(value, "one");
    ASSERT_TRUE(db.Put("k", "three").ok());
    EXPECT_TRUE(db.GetAsOf("k", 2, &value).ok());
    ASSERT_TRUE(db.GetAsOf("k", 3, &value).ok());
    EXPECT_EQ(value, "three");
}

TEST(DBTest, SeedsTimestampFromDiskWhenWalIsEmpty) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->Seed("persisted", 41, ValueType::kValue, "disk-value");
    DBImpl db(fixture.Options(disk, 1));
    ASSERT_TRUE(db.Open().ok());

    std::vector<std::pair<std::string, std::string>> rows;
    ASSERT_TRUE(db.Scan("", "", &rows).ok());
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows.front(), (std::pair<std::string, std::string>{"persisted", "disk-value"}));

    ASSERT_TRUE(db.Put("new", "new-value").ok());
    ASSERT_TRUE(db.WaitForFlush().ok());
    InternalKey persisted_key;
    std::string persisted_value;
    ASSERT_TRUE(disk->GetAsOf("new", std::numeric_limits<Timestamp>::max(),
                              &persisted_key, &persisted_value).ok());
    EXPECT_GT(persisted_key.version, 41U);
}

TEST(DBTest, MemTableTombstoneShadowsDiskKeyInGetAndScan) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->Seed("k", 1, ValueType::kValue, "disk-value");
    DBImpl db(fixture.Options(disk));
    ASSERT_TRUE(db.Put("k", "new").ok());
    ASSERT_TRUE(db.Delete("k").ok());
    std::string value;
    EXPECT_TRUE(db.Get("k", &value).is_not_found());
    std::vector<std::pair<std::string, std::string>> rows;
    ASSERT_TRUE(db.Scan("", "", &rows).ok());
    EXPECT_TRUE(rows.empty());
}

TEST(DBTest, GetAsOfHonorsExactTombstoneVersionAcrossSources) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->Seed("disk", 2, ValueType::kTombstone, "");
    DBImpl db(fixture.Options(disk, 1));
    ASSERT_TRUE(db.Put("immutable", "old").ok());
    ASSERT_TRUE(db.Delete("immutable").ok());
    ASSERT_TRUE(db.Put("active", "old").ok());
    ASSERT_TRUE(db.Delete("active").ok());
    std::string value;
    EXPECT_TRUE(db.GetAsOf("disk", 2, &value).is_not_found());
    EXPECT_TRUE(db.GetAsOf("immutable", 2, &value).is_not_found());
    EXPECT_TRUE(db.GetAsOf("active", 4, &value).is_not_found());
}

TEST(DBTest, DiffTracksTombstonesMissingKeysAndErrors) {
    Fixture fixture;
    DBImpl db(fixture.Options());
    ASSERT_TRUE(db.Put("k", "value").ok());
    ASSERT_TRUE(db.Delete("k").ok());
    DiffResult diff;
    ASSERT_TRUE(db.Diff("k", 1, 2, &diff).ok());
    EXPECT_EQ(diff.old_value, std::optional<std::string>("value"));
    EXPECT_FALSE(diff.new_value.has_value());
    EXPECT_TRUE(diff.changed);
    ASSERT_TRUE(db.Diff("missing", 1, 2, &diff).ok());
    EXPECT_FALSE(diff.changed);
}

TEST(DBTest, FlushErrorsWakeWaiterAndAreReturned) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->FailFlush(true);
    DBImpl db(fixture.Options(disk, 1));
    ASSERT_TRUE(db.Put("key", "value").ok());
    Status status = db.WaitForFlush();
    EXPECT_EQ(status.code, Status::Code::kIOError);
    EXPECT_NE(status.message.find("injected"), std::string::npos);
}

TEST(DBTest, FlushRotatesWalAndDeletesFlushedSegment) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    DBImpl db(fixture.Options(disk, 1));
    ASSERT_TRUE(db.Put("key", "value").ok());
    ASSERT_TRUE(db.WaitForFlush().ok());

    EXPECT_FALSE(std::filesystem::exists(fixture.Path() + ".000001"));
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000002"));
}

TEST(DBTest, FailedFlushPreservesWalAndReopeningRecoversWrites) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->FailFlush(true);
    {
        DBImpl db(fixture.Options(disk, 1));
        ASSERT_TRUE(db.Put("key", "value").ok());
        EXPECT_FALSE(db.WaitForFlush().ok());
    }
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000001"));

    disk->FailFlush(false);
    DBImpl reopened(fixture.Options(disk));
    ASSERT_TRUE(reopened.Open().ok());
    std::string value;
    ASSERT_TRUE(reopened.Get("key", &value).ok());
    EXPECT_EQ(value, "value");
}

TEST(DBTest, ReplaysMultipleWalSegmentsInOrderAndCleansThemAfterFlush) {
    Fixture fixture;
    auto disk = std::make_shared<DiskMock>();
    disk->FailFlush(true);
    {
        DBImpl db(fixture.Options(disk, 1));
        ASSERT_TRUE(db.Put("first", "one").ok());
        EXPECT_FALSE(db.WaitForFlush().ok());
        ASSERT_TRUE(db.Put("second", "two").ok());
        ASSERT_TRUE(db.Put("third", "three").ok());
    }
    ASSERT_TRUE(std::filesystem::exists(fixture.Path() + ".000001"));
    ASSERT_TRUE(std::filesystem::exists(fixture.Path() + ".000002"));
    ASSERT_TRUE(std::filesystem::exists(fixture.Path() + ".000003"));

    disk->FailFlush(false);
    DBImpl reopened(fixture.Options(disk, 1));
    ASSERT_TRUE(reopened.Open().ok());
    std::string value;
    ASSERT_TRUE(reopened.GetAsOf("first", 1, &value).ok());
    EXPECT_EQ(value, "one");
    ASSERT_TRUE(reopened.GetAsOf("second", 2, &value).ok());
    EXPECT_EQ(value, "two");
    ASSERT_TRUE(reopened.GetAsOf("third", 3, &value).ok());
    EXPECT_EQ(value, "three");
    ASSERT_TRUE(reopened.Put("trigger", "flush").ok());
    ASSERT_TRUE(reopened.WaitForFlush().ok());

    EXPECT_FALSE(std::filesystem::exists(fixture.Path() + ".000001"));
    EXPECT_FALSE(std::filesystem::exists(fixture.Path() + ".000002"));
    EXPECT_FALSE(std::filesystem::exists(fixture.Path() + ".000003"));
    EXPECT_FALSE(std::filesystem::exists(fixture.Path() + ".000004"));
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000005"));
}

TEST(DBTest, InMemoryModePreservesRotatedWalSegmentsForRecovery) {
    Fixture fixture;
    {
        DBImpl db(fixture.Options({}, 1));
        ASSERT_TRUE(db.Put("first", "one").ok());
        ASSERT_TRUE(db.Put("second", "two").ok());
        ASSERT_TRUE(db.Put("third", "three").ok());
        ASSERT_TRUE(db.WaitForFlush().ok());
    }
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000001"));
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000002"));
    EXPECT_TRUE(std::filesystem::exists(fixture.Path() + ".000003"));

    DBImpl reopened(fixture.Options());
    ASSERT_TRUE(reopened.Open().ok());
    std::string value;
    ASSERT_TRUE(reopened.GetAsOf("first", 1, &value).ok());
    EXPECT_EQ(value, "one");
    ASSERT_TRUE(reopened.GetAsOf("second", 2, &value).ok());
    EXPECT_EQ(value, "two");
    ASSERT_TRUE(reopened.GetAsOf("third", 3, &value).ok());
    EXPECT_EQ(value, "three");
}

TEST(DBTest, ConcurrentReadersAndWriter) {
    Fixture fixture;
    DBImpl db(fixture.Options());
    ASSERT_TRUE(db.Open().ok());
    std::atomic<bool> failed = false;
    std::thread writer([&] {
        for (int i = 0; i < 150; ++i) {
            if (!db.Put("key", std::to_string(i)).ok()) failed = true;
        }
    });
    std::thread reader([&] {
        for (int i = 0; i < 150; ++i) {
            std::string value;
            Status status = db.Get("key", &value);
            if (!status.ok() && !status.is_not_found()) failed = true;
        }
    });
    writer.join();
    reader.join();
    EXPECT_FALSE(failed.load());
}

TEST(DBTest, RejectsNullOutputs) {
    Fixture fixture;
    DBImpl db(fixture.Options());
    EXPECT_EQ(db.Get("x", nullptr).code, Status::Code::kInvalidArgument);
    EXPECT_EQ(db.Diff("x", 1, 2, nullptr).code, Status::Code::kInvalidArgument);
}

TEST(DBTest, VLogValuesSupportReadsTimeTravelScanAndReopen) {
    Fixture fixture;
    DBOptions options = fixture.Options();
    options.vlog_dir = fixture.Path() + ".vlog";
    options.vlog_threshold = 8;
    std::string large_value = "large-value";
    {
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        ASSERT_TRUE(db.Put("small", "tiny").ok());
        ASSERT_TRUE(db.Put("large", large_value).ok());
        ASSERT_TRUE(db.Put("large", "second-large-value").ok());

        std::string value;
        ASSERT_TRUE(db.Get("small", &value).ok());
        EXPECT_EQ(value, "tiny");
        ASSERT_TRUE(db.GetAsOf("large", 2, &value).ok());
        EXPECT_EQ(value, large_value);
        DiffResult diff;
        ASSERT_TRUE(db.Diff("large", 2, 3, &diff).ok());
        EXPECT_TRUE(diff.changed);
        EXPECT_EQ(diff.old_value, std::optional<std::string>(large_value));
        EXPECT_EQ(diff.new_value, std::optional<std::string>("second-large-value"));

        std::vector<std::pair<std::string, std::string>> rows;
        ASSERT_TRUE(db.Scan("", "", &rows).ok());
        ASSERT_EQ(rows.size(), 2U);
        EXPECT_EQ(rows[0], (std::pair<std::string, std::string>{"large", "second-large-value"}));
    }
    {
        VLog vlog(options.vlog_dir, options.vlog_max_segment_size);
        ASSERT_TRUE(vlog.Open().ok());
        std::vector<VLogRecord> records;
        ASSERT_TRUE(vlog.ScanSegment(1, [&](const VLogRecord& record) {
            records.push_back(record);
            return Status::Ok();
        }).ok());
        ASSERT_EQ(records.size(), 2U);
        EXPECT_EQ(records[0].user_key, "large");
        EXPECT_EQ(records[1].user_key, "large");
    }
    {
        DBImpl reopened(options);
        ASSERT_TRUE(reopened.Open().ok());
        std::string value;
        ASSERT_TRUE(reopened.Get("large", &value).ok());
        EXPECT_EQ(value, "second-large-value");
        ASSERT_TRUE(reopened.GetAsOf("large", 2, &value).ok());
        EXPECT_EQ(value, large_value);
    }
}

TEST(DBTest, VLogGCReclaimsDeadRecordsAndRelocatesLiveValues) {
    Fixture fixture;
    DBOptions options = fixture.Options();
    options.vlog_dir = fixture.Path() + ".vlog";
    options.vlog_threshold = 1;
    options.vlog_max_segment_size = 35;
    DBImpl db(options);
    ASSERT_TRUE(db.Open().ok());
    ASSERT_TRUE(db.Put("live", "large-value-one").ok());
    ASSERT_TRUE(db.Put("other", "large-value-other").ok());
    ASSERT_TRUE(db.Put("live", "large-value-two").ok());
    ASSERT_TRUE(db.Delete("other").ok());
    ASSERT_TRUE(db.Put("tail", "large-value-tail").ok());

    VLogGCStats stats;
    ASSERT_TRUE(db.RunVLogGC(0, &stats).ok());
    EXPECT_EQ(stats.segments_reclaimed, 3U);
    EXPECT_EQ(stats.records_relocated, 1U);
    EXPECT_EQ(stats.records_dropped, 2U);

    std::string value;
    ASSERT_TRUE(db.Get("live", &value).ok());
    EXPECT_EQ(value, "large-value-two");
    std::vector<std::pair<std::string, std::string>> rows;
    ASSERT_TRUE(db.Scan("", "", &rows).ok());
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0], (std::pair<std::string, std::string>{"live", "large-value-two"}));
    EXPECT_EQ(rows[1], (std::pair<std::string, std::string>{"tail", "large-value-tail"}));
}

TEST(DBTest, VLogGCPreservesHistoryAtOrAboveRetentionTimestamp) {
    Fixture fixture;
    DBOptions options = fixture.Options();
    options.vlog_dir = fixture.Path() + ".vlog";
    options.vlog_threshold = 1;
    options.vlog_max_segment_size = 35;
    DBImpl db(options);
    ASSERT_TRUE(db.Put("key", "large-value-one").ok());
    ASSERT_TRUE(db.Put("key", "large-value-two").ok());
    ASSERT_TRUE(db.Put("key", "large-value-three").ok());

    VLogGCStats stats;
    ASSERT_TRUE(db.RunVLogGC(2, &stats).ok());
    EXPECT_EQ(stats.segments_reclaimed, 2U);
    EXPECT_EQ(stats.records_relocated, 1U);
    EXPECT_EQ(stats.records_dropped, 1U);
    std::string value;
    ASSERT_TRUE(db.GetAsOf("key", 2, &value).ok());
    EXPECT_EQ(value, "large-value-two");
    ASSERT_TRUE(db.GetAsOf("key", 3, &value).ok());
    EXPECT_EQ(value, "large-value-three");
}

TEST(DBTest, VLogPointersSurviveLevelManagerFlushAndGarbageCollection) {
    Fixture fixture;
    const std::string disk_dir = fixture.Path() + ".disk";
    DBOptions options = fixture.Options(std::make_shared<LevelManager>(disk_dir, 1024 * 1024), 100);
    options.vlog_dir = fixture.Path() + ".vlog";
    options.vlog_threshold = 1;
    options.vlog_max_segment_size = 35;
    {
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        ASSERT_TRUE(db.Put("key", "large-value-one").ok());
        ASSERT_TRUE(db.Put("key", "large-value-two").ok());
        ASSERT_TRUE(db.Put("dummy", "large-value-dummy").ok());
        ASSERT_TRUE(db.Delete("dummy").ok());
        ASSERT_TRUE(db.WaitForFlush().ok());
        VLogGCStats stats;
        ASSERT_TRUE(db.RunVLogGC(0, &stats).ok());
        EXPECT_EQ(stats.segments_reclaimed, 2U);
        EXPECT_EQ(stats.records_relocated, 1U);
        std::string value;
        ASSERT_TRUE(db.Get("key", &value).ok());
        EXPECT_EQ(value, "large-value-two");
    }
    options.disk = std::make_shared<LevelManager>(disk_dir, 1024 * 1024);
    DBImpl reopened(options);
    ASSERT_TRUE(reopened.Open().ok());
    std::string value;
    ASSERT_TRUE(reopened.Get("key", &value).ok());
    EXPECT_EQ(value, "large-value-two");
    std::vector<std::pair<std::string, std::string>> rows;
    const Status scan_status = reopened.Scan("", "", &rows);
    ASSERT_TRUE(scan_status.ok()) << scan_status.message;
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0], (std::pair<std::string, std::string>{"key", "large-value-two"}));
}

TEST(DBTest, VLogConcurrentReadersDuringGC) {
    Fixture fixture;
    DBOptions options = fixture.Options({}, 1024 * 1024);
    options.vlog_dir = fixture.Path() + ".vlog";
    options.vlog_threshold = 1;
    options.vlog_max_segment_size = 50;
    DBImpl db(options);
    ASSERT_TRUE(db.Open().ok());

    const std::vector<std::string> keys = {"k0", "k1", "k2"};
    const auto make_value = [](const std::string& key, int generation) {
        return "payload-" + key + "-" + std::to_string(generation);
    };
    const auto is_known_value = [&](const std::string& key, const std::string& value) {
        for (int generation = 0; generation <= 12; ++generation) {
            if (value == make_value(key, generation)) return true;
        }
        return false;
    };
    for (const auto& key : keys) ASSERT_TRUE(db.Put(key, make_value(key, 0)).ok());

    std::atomic<bool> finished = false;
    std::atomic<bool> failed = false;
    std::thread reader([&] {
        do {
            for (const auto& key : keys) {
                std::string value;
                const Status status = db.Get(key, &value);
                if (!status.ok() || !is_known_value(key, value)) failed = true;
            }
            std::vector<std::pair<std::string, std::string>> rows;
            const Status scan_status = db.Scan("", "", &rows);
            if (!scan_status.ok() || rows.size() != keys.size()) {
                failed = true;
            } else {
                for (const auto& [key, value] : rows) {
                    if (std::find(keys.begin(), keys.end(), key) == keys.end() ||
                        !is_known_value(key, value)) {
                        failed = true;
                    }
                }
            }
        } while (!finished.load());
    });

    for (int generation = 1; generation <= 12; ++generation) {
        for (const auto& key : keys) {
            if (!db.Put(key, make_value(key, generation)).ok()) {
                failed = true;
                break;
            }
        }
        if (!db.RunVLogGC().ok()) {
            failed = true;
            break;
        }
    }
    finished = true;
    reader.join();
    EXPECT_FALSE(failed.load());

    for (const auto& key : keys) {
        std::string value;
        ASSERT_TRUE(db.Get(key, &value).ok());
        EXPECT_EQ(value, make_value(key, 12));
    }
}

} // namespace
} // namespace kvstore
