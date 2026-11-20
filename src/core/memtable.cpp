#include "kvstore/memtable.hpp"

#include <mutex>
#include <utility>

namespace kvstore {

class MemTable::MemTableIterator final : public Iterator {
public:
    explicit MemTableIterator(const MemTable& table)
        : table_(table), lock_(table_.mutex_), current_(table_.entries_.First()) {}

    bool Valid() const override { return current_ != nullptr; }
    void SeekToFirst() override { current_ = table_.entries_.First(); }
    void Seek(const InternalKey& target) override { current_ = table_.entries_.LowerBound(target); }
    void Next() override { if (current_) current_ = SkipList::Next(current_); }
    InternalKey Key() const override { return current_ ? current_->key : InternalKey{}; }
    std::string_view Value() const override { return current_ ? std::string_view(current_->value) : std::string_view{}; }

private:
    const MemTable& table_;
    std::shared_lock<std::shared_mutex> lock_;
    SkipList::Node* current_;
};

void MemTable::Put(const InternalKey& key, std::string value) {
    std::unique_lock lock(mutex_);
    auto* previous = entries_.Find(key);
    const std::size_t old_size = previous ? previous->key.user_key.capacity() + previous->value.capacity() + sizeof(InternalKey) : 0;
    auto* inserted = entries_.InsertOrAssign(key, std::move(value));
    const std::size_t new_size = inserted->key.user_key.capacity() + inserted->value.capacity() + sizeof(InternalKey);
    memory_usage_.fetch_add(new_size, std::memory_order_relaxed);
    if (previous) memory_usage_.fetch_sub(old_size, std::memory_order_relaxed);
}

Status MemTable::Get(const std::string& user_key, std::string* value) const {
    return GetAsOf(user_key, UINT64_MAX, value);
}

Status MemTable::GetAsOf(const std::string& user_key, Timestamp timestamp, std::string* value) const {
    if (!value) return {Status::Code::kInvalidArgument, "value output must not be null"};
    std::shared_lock lock(mutex_);
    auto* node = entries_.LowerBound(InternalKey{user_key, UINT64_MAX, ValueType::kValue});
    while (node && node->key.user_key == user_key) {
        if (node->key.version <= timestamp) {
            if (node->key.type == ValueType::kTombstone) return Status::NotFound();
            *value = node->value;
            return Status::Ok();
        }
        node = SkipList::Next(node);
    }
    return Status::NotFound();
}

std::unique_ptr<Iterator> MemTable::NewIterator() const {
    return std::make_unique<MemTableIterator>(*this);
}

} // namespace kvstore
