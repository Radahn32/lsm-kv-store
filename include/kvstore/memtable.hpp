#pragma once

#include "kvstore/iterator.hpp"
#include "kvstore/skip_list.hpp"

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>

namespace kvstore {

class MemTable {
public:
    MemTable() = default;
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Put(const InternalKey& key, std::string value);
    Status Get(const std::string& user_key, std::string* value) const;
    Status GetAsOf(const std::string& user_key, Timestamp timestamp, std::string* value) const;
    std::size_t ApproximateMemoryUsage() const noexcept { return memory_usage_.load(); }
    std::unique_ptr<Iterator> NewIterator() const;

private:
    class MemTableIterator;
    mutable std::shared_mutex mutex_;
    SkipList entries_;
    std::atomic<std::size_t> memory_usage_{0};
};

} // namespace kvstore
