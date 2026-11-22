#include "storage/sstable.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore {

namespace {

inline void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(uint32_t)];
    std::memcpy(buf, &value, sizeof(uint32_t));
    dst->append(buf, sizeof(uint32_t));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(uint64_t)];
    std::memcpy(buf, &value, sizeof(uint64_t));
    dst->append(buf, sizeof(uint64_t));
}

Status WriteBuffer(int fd, const char* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t res = write(fd, data + written, len - written);
        if (res < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError("Failed to write to file: " + std::string(strerror(errno)));
        }
        written += static_cast<size_t>(res);
    }
    return Status::Ok();
}

} // namespace

SSTableBuilder::SSTableBuilder(const std::string& filepath, size_t bits_per_key)
    : filepath_(filepath), bits_per_key_(bits_per_key) {
    fd_ = open(filepath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
}

SSTableBuilder::~SSTableBuilder() {
    if (!closed_ && fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

Status SSTableBuilder::FlushCurrentBlock() {
    if (current_block_data_.empty()) {
        return Status::Ok();
    }
    const uint64_t block_offset = current_file_offset_;
    const uint64_t block_size = current_block_data_.size();

    Status s = WriteBuffer(fd_, current_block_data_.data(), block_size);
    if (!s.ok()) {
        return s;
    }

    current_file_offset_ += block_size;
    index_entries_.push_back(BlockHandle{last_key_in_block_, block_offset, block_size});
    current_block_data_.clear();
    return Status::Ok();
}

Status SSTableBuilder::Add(const InternalKey& key, std::string_view value) {
    if (closed_) {
        return Status::Corruption("Cannot add to closed SSTableBuilder");
    }
    if (fd_ < 0) {
        return Status::IOError("Failed to open SSTable file: " + filepath_ + " (" + strerror(errno) + ")");
    }
    if (entry_count_ > 0 && key < max_key_) {
        return {Status::Code::kInvalidArgument, "Keys must be added in sorted order"};
    }

    // Append to current data block:
    // [user_key_len: uint32][user_key][timestamp: uint64][value_type: uint8][value_len: uint32][value_bytes]
    PutFixed32(&current_block_data_, static_cast<uint32_t>(key.user_key.size()));
    current_block_data_.append(key.user_key);
    PutFixed64(&current_block_data_, key.version);
    current_block_data_.push_back(static_cast<char>(key.type));
    PutFixed32(&current_block_data_, static_cast<uint32_t>(value.size()));
    current_block_data_.append(value.data(), value.size());

    last_key_in_block_ = key;

    if (all_user_keys_.empty() || all_user_keys_.back() != key.user_key) {
        all_user_keys_.push_back(key.user_key);
    }

    if (entry_count_ == 0) {
        min_key_ = key;
    }
    max_key_ = key;
    max_version_ = std::max(max_version_, key.version);
    entry_count_++;

    if (current_block_data_.size() >= kTargetBlockSize) {
        return FlushCurrentBlock();
    }

    return Status::Ok();
}

Status SSTableBuilder::Finish() {
    if (closed_) {
        return Status::Ok();
    }
    if (fd_ < 0) {
        return Status::IOError("File descriptor is invalid for: " + filepath_);
    }

    // Flush any pending data block
    if (!current_block_data_.empty()) {
        Status s = FlushCurrentBlock();
        if (!s.ok()) {
            return s;
        }
    }

    // 1. Filter Block
    BloomFilter filter(bits_per_key_);
    std::string filter_data = filter.CreateFilter(all_user_keys_);
    const uint64_t filter_offset = current_file_offset_;
    const uint64_t filter_size = filter_data.size();
    if (filter_size > 0) {
        Status s = WriteBuffer(fd_, filter_data.data(), filter_size);
        if (!s.ok()) {
            return s;
        }
        current_file_offset_ += filter_size;
    }

    // 2. Index Block
    std::string index_data;
    PutFixed32(&index_data, static_cast<uint32_t>(index_entries_.size()));
    for (const auto& handle : index_entries_) {
        PutFixed32(&index_data, static_cast<uint32_t>(handle.last_key.user_key.size()));
        index_data.append(handle.last_key.user_key);
        PutFixed64(&index_data, handle.last_key.version);
        index_data.push_back(static_cast<char>(handle.last_key.type));
        PutFixed64(&index_data, handle.offset);
        PutFixed64(&index_data, handle.size);
    }

    const uint64_t index_offset = current_file_offset_;
    const uint64_t index_size = index_data.size();
    if (index_size > 0) {
        Status s = WriteBuffer(fd_, index_data.data(), index_size);
        if (!s.ok()) {
            return s;
        }
        current_file_offset_ += index_size;
    }

    // 3. Footer (48 bytes fixed size)
    std::string footer_data;
    PutFixed64(&footer_data, filter_offset);
    PutFixed64(&footer_data, filter_size);
    PutFixed64(&footer_data, index_offset);
    PutFixed64(&footer_data, index_size);
    PutFixed64(&footer_data, max_version_);
    PutFixed64(&footer_data, kSSTableMagicNumber);

    Status s = WriteBuffer(fd_, footer_data.data(), footer_data.size());
    if (!s.ok()) {
        return s;
    }
    current_file_offset_ += footer_data.size();

    // 4. fsync and close
    if (fsync(fd_) != 0) {
        return Status::IOError("Failed to fsync SSTable file: " + std::string(strerror(errno)));
    }

    if (close(fd_) != 0) {
        return Status::IOError("Failed to close SSTable file: " + std::string(strerror(errno)));
    }
    fd_ = -1;
    closed_ = true;
    file_size_ = current_file_offset_;

    return Status::Ok();
}

Status SSTableBuilder::BuildFromIterator(const std::string& filepath, Iterator* iter, size_t bits_per_key) {
    if (!iter) {
        return {Status::Code::kInvalidArgument, "Iterator cannot be null"};
    }
    SSTableBuilder builder(filepath, bits_per_key);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Status s = builder.Add(iter->Key(), iter->Value());
        if (!s.ok()) {
            return s;
        }
    }
    return builder.Finish();
}

} // namespace kvstore
