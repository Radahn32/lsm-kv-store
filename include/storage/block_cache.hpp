#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

struct BlockCacheKey {
    uint64_t file_id{0};
    uint64_t block_offset{0};

    bool operator==(const BlockCacheKey& other) const noexcept {
        return file_id == other.file_id && block_offset == other.block_offset;
    }
};

struct BlockCacheKeyHash {
    size_t operator()(const BlockCacheKey& key) const noexcept {
        uint64_t h1 = key.file_id;
        uint64_t h2 = key.block_offset;
        uint64_t h = h1 ^ (h2 + 0x517cc1b727220a95ULL + (h1 << 6) + (h1 >> 2));
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        return static_cast<size_t>(h);
    }
};

class BlockCache {
public:
    explicit BlockCache(size_t capacity_bytes);
    ~BlockCache() = default;

    BlockCache(const BlockCache&) = delete;
    BlockCache& operator=(const BlockCache&) = delete;

    // Returns the cached block and promotes it to Most Recently Used.
    // Returns nullptr on miss.
    std::shared_ptr<const std::string> Get(uint64_t file_id, uint64_t block_offset);

    // Inserts or updates the block, evicting Least Recently Used blocks until
    // current_bytes_ <= capacity_bytes. If the single block exceeds capacity_bytes,
    // it is skipped cleanly.
    void Put(uint64_t file_id, uint64_t block_offset, std::shared_ptr<const std::string> block_data);

    // Evicts all blocks belonging to file_id (e.g. after table deletion or compaction).
    void EvictFile(uint64_t file_id);

    // Telemetry helpers
    size_t Hits() const noexcept { return hits_.load(std::memory_order_relaxed); }
    size_t Misses() const noexcept { return misses_.load(std::memory_order_relaxed); }
    size_t CurrentBytes() const noexcept;
    size_t CapacityBytes() const noexcept { return capacity_bytes_; }
    void ResetTelemetry() noexcept;

private:
    struct Entry {
        BlockCacheKey key;
        std::shared_ptr<const std::string> data;
        size_t size{0};
    };

    const size_t capacity_bytes_;
    size_t current_bytes_{0};

    mutable std::mutex mutex_;
    std::list<Entry> lru_list_; // front = MRU, back = LRU
    std::unordered_map<BlockCacheKey, std::list<Entry>::iterator, BlockCacheKeyHash> table_;

    std::atomic<size_t> hits_{0};
    std::atomic<size_t> misses_{0};
};

} // namespace kvstore

namespace lsm {
    using BlockCacheKey = kvstore::BlockCacheKey;
    using BlockCacheKeyHash = kvstore::BlockCacheKeyHash;
    using BlockCache = kvstore::BlockCache;
} // namespace lsm
