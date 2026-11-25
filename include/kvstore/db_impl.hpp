#pragma once

#include "kvstore/db.hpp"
#include "kvstore/memtable.hpp"
#include "kvstore/wal.hpp"
#include "kvstore/vlog.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

namespace kvstore {

// Disk implementations can be supplied by the SSTable/level-manager layer.
class DiskStorage {
public:
    virtual ~DiskStorage() = default;
    virtual Timestamp MaxVersion() const = 0;
    virtual Status GetAsOf(const std::string&, Timestamp, InternalKey*, std::string*) const = 0;
    virtual std::unique_ptr<Iterator> NewIterator() const = 0;
    virtual Status Flush(Iterator&) = 0;
};

struct DBOptions {
    std::string wal_path;
    std::size_t memtable_limit = 4U * 1024U * 1024U;
    std::shared_ptr<DiskStorage> disk;
    std::string vlog_dir;
    std::size_t vlog_threshold = 1024;
    std::size_t vlog_max_segment_size = 16U * 1024U * 1024U;
};

struct VLogGCStats {
    uint64_t segments_reclaimed = 0;
    uint64_t records_relocated = 0;
    uint64_t records_dropped = 0;
};

class DBImpl final : public DB {
public:
    explicit DBImpl(DBOptions options);
    ~DBImpl() override;
    Status Open();
    Status Put(const std::string&, const std::string&) override;
    Status Get(const std::string&, std::string*) override;
    Status Delete(const std::string&) override;
    Status Scan(const std::string&, const std::string&,
                std::vector<std::pair<std::string,std::string>>*) override;
    Status GetAsOf(const std::string&, Timestamp, std::string*) override;
    Status Diff(const std::string&, Timestamp, Timestamp, DiffResult*) override;
    Status WaitForFlush();
    Status RunVLogGC(Timestamp min_retained_ts = 0, VLogGCStats* stats = nullptr);

private:
    struct ImmutableEntry {
        std::shared_ptr<MemTable> table;
        std::vector<std::string> wal_paths;
    };

    Status Write(const std::string&, const std::string&, ValueType);
    Status OpenLocked();
    Status ReadAt(const std::string&, Timestamp, std::string*) const;
    Status LookupRawAt(const std::string&, Timestamp, InternalKey*, std::string*) const;
    Status RotateIfNeededLocked(const std::shared_ptr<MemTable>& active);
    void FlushWorker();
    DBOptions options_;
    std::unique_ptr<VLog> vlog_;
    std::unique_ptr<WAL> wal_;
    uint64_t current_wal_id_ = 0;
    std::vector<std::string> active_wal_paths_;
    mutable std::shared_mutex vlog_gc_mutex_;
    mutable std::mutex state_mutex_;
    std::mutex writer_mutex_;
    std::shared_ptr<MemTable> active_;
    std::vector<ImmutableEntry> immutable_;
    Timestamp next_version_ = 1;
    bool opened_ = false;
    bool stopping_ = false;
    bool flushing_ = false;
    Status flush_status_ = Status::Ok();
    std::mutex flush_mutex_;
    std::condition_variable flush_cv_;
    std::thread flush_thread_;
};
} // namespace kvstore
