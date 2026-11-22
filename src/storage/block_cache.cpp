#include "storage/block_cache.hpp"

namespace kvstore {

BlockCache::BlockCache(size_t capacity_bytes) : capacity_bytes_(capacity_bytes) {}

std::shared_ptr<const std::string> BlockCache::Get(uint64_t file_id, uint64_t block_offset) {
    std::lock_guard lock(mutex_);
    const BlockCacheKey key{file_id, block_offset};
    auto it = table_.find(key);
    if (it == table_.end()) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    hits_.fetch_add(1, std::memory_order_relaxed);
    // Splice accessed entry to the front of LRU list (Most Recently Used)
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    return it->second->data;
}

void BlockCache::Put(uint64_t file_id, uint64_t block_offset, std::shared_ptr<const std::string> block_data) {
    if (!block_data) {
        return;
    }

    const size_t block_size = block_data->size();
    if (block_size > capacity_bytes_) {
        // If a single block exceeds the cache capacity, skip caching it cleanly
        return;
    }

    std::lock_guard lock(mutex_);
    const BlockCacheKey key{file_id, block_offset};
    auto it = table_.find(key);

    if (it != table_.end()) {
        // Key already exists, update data, size, and move to MRU front
        current_bytes_ -= it->second->size;
        it->second->data = std::move(block_data);
        it->second->size = block_size;
        current_bytes_ += block_size;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    } else {
        // Insert new entry at the front of MRU list
        lru_list_.push_front(Entry{key, std::move(block_data), block_size});
        table_[key] = lru_list_.begin();
        current_bytes_ += block_size;
    }

    // Evict Least Recently Used entries from the back until current_bytes_ <= capacity_bytes_
    while (current_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
        auto last = --lru_list_.end();
        current_bytes_ -= last->size;
        table_.erase(last->key);
        lru_list_.pop_back();
    }
}

void BlockCache::EvictFile(uint64_t file_id) {
    std::lock_guard lock(mutex_);
    for (auto it = lru_list_.begin(); it != lru_list_.end(); ) {
        if (it->key.file_id == file_id) {
            current_bytes_ -= it->size;
            table_.erase(it->key);
            it = lru_list_.erase(it);
        } else {
            ++it;
        }
    }
}

size_t BlockCache::CurrentBytes() const noexcept {
    std::lock_guard lock(mutex_);
    return current_bytes_;
}

void BlockCache::ResetTelemetry() noexcept {
    hits_.store(0, std::memory_order_relaxed);
    misses_.store(0, std::memory_order_relaxed);
}

} // namespace kvstore
