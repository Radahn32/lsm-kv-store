#pragma once

#include "kvstore/iterator.hpp"
#include "kvstore/types.hpp"
#include "storage/bloom_filter.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

class BlockCache;

constexpr uint64_t kSSTableMagicNumber = 0x53535461626C6531ULL; // "SSTable1"
constexpr size_t kFooterSize = 48;
constexpr size_t kTargetBlockSize = 4096;

struct BlockHandle {
    InternalKey last_key;
    uint64_t offset{0};
    uint64_t size{0};
};

struct Footer {
    uint64_t filter_offset{0};
    uint64_t filter_size{0};
    uint64_t index_offset{0};
    uint64_t index_size{0};
    uint64_t max_version{0};
    uint64_t magic_number{kSSTableMagicNumber};
};

class SSTableBuilder {
public:
    explicit SSTableBuilder(const std::string& filepath, size_t bits_per_key = 10);
    ~SSTableBuilder();

    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;

    Status Add(const InternalKey& key, std::string_view value);
    Status Finish();

    static Status BuildFromIterator(const std::string& filepath, Iterator* iter, size_t bits_per_key = 10);

    const InternalKey& min_key() const noexcept { return min_key_; }
    const InternalKey& max_key() const noexcept { return max_key_; }
    Timestamp MaxVersion() const noexcept { return max_version_; }
    Timestamp max_version() const noexcept { return max_version_; }
    size_t entry_count() const noexcept { return entry_count_; }
    uint64_t file_size() const noexcept { return file_size_; }

private:
    Status FlushCurrentBlock();

    std::string filepath_;
    size_t bits_per_key_;
    int fd_{-1};
    bool closed_{false};

    std::string current_block_data_;
    uint64_t current_file_offset_{0};

    std::vector<std::string> all_user_keys_;
    std::vector<BlockHandle> index_entries_;

    InternalKey last_key_in_block_;
    InternalKey min_key_;
    InternalKey max_key_;
    Timestamp max_version_{0};
    size_t entry_count_{0};
    uint64_t file_size_{0};
};

class SSTableReader {
public:
    static Status Open(const std::string& filepath,
                       std::unique_ptr<SSTableReader>* reader,
                       uint64_t file_id = 0,
                       BlockCache* cache = nullptr);
    ~SSTableReader();

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    bool MayContain(std::string_view user_key) const;

    // Matches DiskStorage::GetAsOf signature & semantics: returns newest version <= timestamp,
    // including tombstones (as Status::Ok() with out_key->type == kTombstone), or NotFound.
    Status GetAsOf(const std::string& key, Timestamp timestamp, InternalKey* out_key, std::string* out_value) const;

    // Point lookup returning the value or NotFound if absent or deleted at snapshot
    Status Get(std::string_view user_key, std::string* value, Timestamp snapshot_timestamp = UINT64_MAX) const;

    // Range iteration across all Data Blocks in the SSTable
    std::unique_ptr<Iterator> NewIterator() const;

    uint64_t file_id() const noexcept { return file_id_; }
    BlockCache* cache() const noexcept { return cache_; }
    Timestamp MaxVersion() const noexcept { return max_version_; }
    Timestamp max_version() const noexcept { return max_version_; }
    uint64_t file_size() const noexcept { return file_size_; }
    size_t block_count() const noexcept { return index_entries_.size(); }
    const std::vector<BlockHandle>& index() const noexcept { return index_entries_; }

private:
    friend class SSTableIterator;

    SSTableReader() = default;
    Status Init(const std::string& filepath, uint64_t file_id, BlockCache* cache);

    int fd_{-1};
    const char* data_{nullptr};
    size_t file_size_{0};

    uint64_t file_id_{0};
    BlockCache* cache_{nullptr};

    std::string_view filter_data_;
    std::vector<BlockHandle> index_entries_;
    Timestamp max_version_{0};
};

} // namespace kvstore

namespace lsm {
    using BlockHandle = kvstore::BlockHandle;
    using Footer = kvstore::Footer;
    using SSTableBuilder = kvstore::SSTableBuilder;
    using SSTableReader = kvstore::SSTableReader;
} // namespace lsm
