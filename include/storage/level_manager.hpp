#pragma once

#include "kvstore/db_impl.hpp"
#include "kvstore/iterator.hpp"
#include "kvstore/types.hpp"
#include "storage/block_cache.hpp"
#include "storage/sstable.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

struct TableMeta {
    uint64_t file_id{0};
    size_t level{0};
    std::string file_path;
    uint64_t file_size{0};
    InternalKey min_key;
    InternalKey max_key;
    Timestamp max_version{0};
    std::shared_ptr<SSTableReader> reader;
};

class MergingIterator final : public Iterator {
public:
    explicit MergingIterator(std::vector<std::unique_ptr<Iterator>> children,
                             std::vector<std::shared_ptr<TableMeta>> metas = {});
    ~MergingIterator() override = default;

    bool Valid() const override { return valid_; }
    void SeekToFirst() override;
    void Seek(const InternalKey& target) override;
    void Next() override;

    InternalKey Key() const override { return current_key_; }
    std::string_view Value() const override { return current_value_; }

private:
    struct HeapItem {
        InternalKey key;
        size_t child_idx{0};

        // Min-heap comparator: smallest key has highest priority
        bool operator>(const HeapItem& other) const {
            return other.key < key;
        }
    };

    void UpdateCurrent();

    std::vector<std::unique_ptr<Iterator>> children_;
    std::vector<std::shared_ptr<TableMeta>> metas_; // Retains active reader references

    std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<HeapItem>> min_heap_;
    InternalKey current_key_;
    std::string_view current_value_;
    bool valid_{false};
};

class LevelManager : public DiskStorage {
public:
    explicit LevelManager(std::string db_dir,
                          size_t block_cache_capacity = 8 * 1024 * 1024,
                          size_t max_levels = 7,
                          size_t bits_per_key = 10);
    ~LevelManager() override = default;

    LevelManager(const LevelManager&) = delete;
    LevelManager& operator=(const LevelManager&) = delete;

    // Initializes directory and recovers active SSTable set from MANIFEST
    Status Recover();

    // 4 DiskStorage virtual methods matching kvstore::DiskStorage:
    Timestamp MaxVersion() const override;
    Status GetAsOf(const std::string& key, Timestamp timestamp, InternalKey* out_key, std::string* out_value) const override;
    std::unique_ptr<Iterator> NewIterator() const override;
    Status Flush(Iterator& iter) override;

    // Metadata accessors
    const std::string& db_dir() const noexcept { return db_dir_; }
    std::shared_ptr<BlockCache> block_cache() const noexcept { return block_cache_; }
    size_t max_levels() const noexcept { return max_levels_; }
    size_t NumFiles(size_t level) const;
    std::vector<std::shared_ptr<TableMeta>> GetLevelFiles(size_t level) const;

    // Version edit operations
    Status AddFile(size_t level, std::shared_ptr<TableMeta> meta);
    Status DeleteFile(size_t level, uint64_t file_id);

private:
    Status RecordAddFile(size_t level, const std::shared_ptr<TableMeta>& meta);
    Status RecordDeleteFile(size_t level, uint64_t file_id);

    std::string db_dir_;
    std::string manifest_path_;
    size_t max_levels_{7};
    size_t bits_per_key_{10};

    std::shared_ptr<BlockCache> block_cache_;

    mutable std::shared_mutex mutex_;
    std::mutex manifest_mutex_;

    uint64_t next_file_id_{1};
    std::vector<std::vector<std::shared_ptr<TableMeta>>> levels_;
};

} // namespace kvstore

namespace lsm {
    using TableMeta = kvstore::TableMeta;
    using MergingIterator = kvstore::MergingIterator;
    using LevelManager = kvstore::LevelManager;
} // namespace lsm
