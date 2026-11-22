#include "kvstore/db_impl.hpp"

#include <algorithm>
#include <limits>
#include <set>

namespace kvstore {

DBImpl::DBImpl(DBOptions options)
    : options_(std::move(options)),
      wal_(options_.wal_path),
      active_(std::make_shared<MemTable>()) {}

DBImpl::~DBImpl() {
    {
        std::lock_guard lock(flush_mutex_);
        stopping_ = true;
    }
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
}

Status DBImpl::Open() {
    std::lock_guard writer_lock(writer_mutex_);
    return OpenLocked();
}

Status DBImpl::OpenLocked() {
    if (opened_) {
        return Status::Ok();
    }

    Status status = wal_.Replay(active_.get());
    if (!status.ok()) {
        return status;
    }

    Timestamp largest_version = 0;
    auto iterator = active_->NewIterator();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        largest_version = std::max(largest_version, iterator->Key().version);
    }
    if (options_.disk) {
        largest_version = std::max(largest_version, options_.disk->MaxVersion());
    }
    if (largest_version == std::numeric_limits<Timestamp>::max()) {
        return Status::Corruption("WAL timestamp exhausted");
    }
    next_version_ = largest_version + 1;
    opened_ = true;
    flush_thread_ = std::thread(&DBImpl::FlushWorker, this);
    return Status::Ok();
}

Status DBImpl::Write(const std::string& key, const std::string& value, ValueType type) {
    std::lock_guard writer_lock(writer_mutex_);
    Status open_status = OpenLocked();
    if (!open_status.ok()) {
        return open_status;
    }
    if (next_version_ == std::numeric_limits<Timestamp>::max()) {
        return Status::IOError("timestamp exhausted");
    }

    InternalKey internal_key{key, next_version_, type};
    Status status = wal_.Append(internal_key, value);
    if (!status.ok()) {
        return status;
    }
    ++next_version_;

    std::shared_ptr<MemTable> active;
    {
        std::lock_guard state_lock(state_mutex_);
        active = active_;
    }
    active->Put(internal_key, value);

    bool queued_flush = false;
    if (active->ApproximateMemoryUsage() >= options_.memtable_limit) {
        // Queue mutation and notification share flush_mutex_ with the worker predicate.
        std::lock_guard flush_lock(flush_mutex_);
        std::lock_guard state_lock(state_mutex_);
        if (active_ == active) {
            immutable_.push_back(active_);
            active_ = std::make_shared<MemTable>();
            queued_flush = true;
        }
    }
    if (queued_flush) {
        flush_cv_.notify_one();
    }
    return Status::Ok();
}

Status DBImpl::Put(const std::string& key, const std::string& value) {
    return Write(key, value, ValueType::kValue);
}

Status DBImpl::Delete(const std::string& key) {
    return Write(key, "", ValueType::kTombstone);
}

Status DBImpl::ReadAt(const std::string& key, Timestamp timestamp, std::string* value) const {
    if (!value) {
        return {Status::Code::kInvalidArgument, "value output must not be null"};
    }

    std::shared_ptr<MemTable> active;
    std::vector<std::shared_ptr<MemTable>> immutable;
    {
        std::lock_guard state_lock(state_mutex_);
        active = active_;
        immutable = immutable_;
    }

    bool found = false;
    bool deleted = false;
    Timestamp best_version = 0;
    std::string best_value;
    const auto consider = [&](const std::shared_ptr<MemTable>& table) {
        auto iterator = table->NewIterator();
        iterator->Seek({key, timestamp, ValueType::kValue});
        if (iterator->Valid() && iterator->Key().user_key == key &&
            iterator->Key().version <= timestamp &&
            (!found || iterator->Key().version > best_version)) {
            const InternalKey internal_key = iterator->Key();
            found = true;
            best_version = internal_key.version;
            deleted = internal_key.type == ValueType::kTombstone;
            best_value = std::string(iterator->Value());
        }
    };
    consider(active);
    for (const auto& table : immutable) {
        consider(table);
    }

    if (options_.disk) {
        InternalKey disk_key;
        std::string disk_value;
        Status status = options_.disk->GetAsOf(key, timestamp, &disk_key, &disk_value);
        if (status.ok() && (!found || disk_key.version > best_version)) {
            found = true;
            best_version = disk_key.version;
            deleted = disk_key.type == ValueType::kTombstone;
            best_value = std::move(disk_value);
        } else if (!status.ok() && !status.is_not_found()) {
            return status;
        }
    }

    if (!found || deleted) {
        return Status::NotFound();
    }
    *value = std::move(best_value);
    return Status::Ok();
}

Status DBImpl::Get(const std::string& key, std::string* value) {
    return GetAsOf(key, std::numeric_limits<Timestamp>::max(), value);
}

Status DBImpl::GetAsOf(const std::string& key, Timestamp timestamp, std::string* value) {
    return ReadAt(key, timestamp, value);
}

Status DBImpl::Diff(const std::string& key, Timestamp t1, Timestamp t2, DiffResult* result) {
    if (!result) {
        return {Status::Code::kInvalidArgument, "result output must not be null"};
    }

    result->old_value.reset();
    result->new_value.reset();
    std::string value;
    Status old_status = ReadAt(key, t1, &value);
    if (old_status.ok()) {
        result->old_value = value;
    } else if (!old_status.is_not_found()) {
        return old_status;
    }
    Status new_status = ReadAt(key, t2, &value);
    if (new_status.ok()) {
        result->new_value = value;
    } else if (!new_status.is_not_found()) {
        return new_status;
    }
    result->changed = result->old_value != result->new_value;
    return Status::Ok();
}

Status DBImpl::Scan(const std::string& start_key, const std::string& end_key,
                    std::vector<std::pair<std::string, std::string>>* results) {
    if (!results) {
        return {Status::Code::kInvalidArgument, "results output must not be null"};
    }

    // Serialize with writes while taking the table snapshot and its read timestamp.
    std::shared_ptr<MemTable> active;
    std::vector<std::shared_ptr<MemTable>> immutable;
    Timestamp read_timestamp;
    {
        std::lock_guard writer_lock(writer_mutex_);
        std::lock_guard state_lock(state_mutex_);
        active = active_;
        immutable = immutable_;
        read_timestamp = next_version_ == 0 ? 0 : next_version_ - 1;
    }

    std::map<std::string, std::pair<InternalKey, std::string>> merged;
    for (const auto& table : immutable) {
        auto iterator = table->NewIterator();
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            InternalKey key = iterator->Key();
            if (key.user_key < start_key || (!end_key.empty() && key.user_key >= end_key) ||
                key.version > read_timestamp) continue;
            auto& current = merged[key.user_key];
            if (current.first.version < key.version) current = {key, std::string(iterator->Value())};
        }
    }
    {
        auto iterator = active->NewIterator();
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            InternalKey key = iterator->Key();
            if (key.user_key < start_key || (!end_key.empty() && key.user_key >= end_key) ||
                key.version > read_timestamp) continue;
            auto& current = merged[key.user_key];
            if (current.first.version < key.version) current = {key, std::string(iterator->Value())};
        }
    }

    // The snapshot tables above are fixed. The iterator is consumed synchronously without
    // holding any MemTable lock, so slow disk traversal does not block writers.
    if (options_.disk) {
        auto iterator = options_.disk->NewIterator();
        if (iterator) {
            for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
                InternalKey key = iterator->Key();
                if (key.user_key < start_key || (!end_key.empty() && key.user_key >= end_key) ||
                    key.version > read_timestamp) continue;
                auto& current = merged[key.user_key];
                if (current.first.version < key.version) {
                    current = {key, std::string(iterator->Value())};
                }
            }
        }
    }

    results->clear();
    for (auto& [key, entry] : merged) {
        if (entry.first.version != 0 && entry.first.type != ValueType::kTombstone) {
            results->emplace_back(key, std::move(entry.second));
        }
    }
    return Status::Ok();
}

void DBImpl::FlushWorker() {
    std::unique_lock flush_lock(flush_mutex_);
    while (true) {
        flush_cv_.wait(flush_lock, [this] {
            std::lock_guard state_lock(state_mutex_);
            return stopping_ || !immutable_.empty();
        });
        if (stopping_) {
            return;
        }
        flushing_ = true;
        flush_lock.unlock();

        Status status = Status::Ok();
        while (status.ok()) {
            std::shared_ptr<MemTable> table;
            {
                std::lock_guard state_lock(state_mutex_);
                if (immutable_.empty()) break;
                table = immutable_.front();
            }
            if (options_.disk) {
                auto iterator = table->NewIterator();
                status = options_.disk->Flush(*iterator);
            }
            if (status.ok()) {
                std::lock_guard state_lock(state_mutex_);
                if (!immutable_.empty() && immutable_.front() == table) {
                    immutable_.erase(immutable_.begin());
                }
            }
        }

        flush_lock.lock();
        flushing_ = false;
        if (!status.ok()) {
            flush_status_ = status;
        }
        flush_cv_.notify_all();
        if (!status.ok()) {
            flush_cv_.wait(flush_lock, [this] { return stopping_; });
            return;
        }
    }
}

Status DBImpl::WaitForFlush() {
    std::unique_lock flush_lock(flush_mutex_);
    flush_cv_.wait(flush_lock, [this] {
        std::lock_guard state_lock(state_mutex_);
        return !flush_status_.ok() || (immutable_.empty() && !flushing_);
    });
    return flush_status_;
}

} // namespace kvstore
