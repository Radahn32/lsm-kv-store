#include "storage/sstable.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore {

namespace {

inline uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    std::memcpy(&result, ptr, sizeof(uint32_t));
    return result;
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    std::memcpy(&result, ptr, sizeof(uint64_t));
    return result;
}

} // namespace

class SSTableIterator final : public Iterator {
public:
    explicit SSTableIterator(const SSTableReader* reader) : reader_(reader) {}

    bool Valid() const override { return valid_; }

    void SeekToFirst() override {
        block_idx_ = 0;
        offset_in_block_ = 0;
        DecodeCurrent();
    }

    void Seek(const InternalKey& target) override {
        if (!reader_ || reader_->index_entries_.empty()) {
            valid_ = false;
            return;
        }

        auto it = std::lower_bound(
            reader_->index_entries_.begin(),
            reader_->index_entries_.end(),
            target,
            [](const BlockHandle& handle, const InternalKey& t) {
                return handle.last_key < t;
            }
        );

        if (it == reader_->index_entries_.end()) {
            valid_ = false;
            block_idx_ = reader_->index_entries_.size();
            return;
        }

        block_idx_ = static_cast<size_t>(std::distance(reader_->index_entries_.begin(), it));
        offset_in_block_ = 0;

        while (true) {
            DecodeCurrent();
            if (!valid_) {
                break;
            }
            if (!(current_key_ < target)) {
                // Found first key not less than target
                break;
            }
            offset_in_block_ += entry_total_len_;
        }
    }

    void Next() override {
        if (!valid_) {
            return;
        }
        offset_in_block_ += entry_total_len_;
        DecodeCurrent();
    }

    InternalKey Key() const override {
        return current_key_;
    }

    std::string_view Value() const override {
        return current_value_;
    }

private:
    void DecodeCurrent() {
        valid_ = false;
        if (!reader_ || !reader_->data_) {
            return;
        }

        while (block_idx_ < reader_->index_entries_.size()) {
            const auto& block = reader_->index_entries_[block_idx_];
            if (offset_in_block_ >= block.size) {
                block_idx_++;
                offset_in_block_ = 0;
                continue;
            }

            const char* block_ptr = reader_->data_ + block.offset + offset_in_block_;
            const size_t remaining = block.size - offset_in_block_;

            if (remaining < 4 + 8 + 1 + 4) {
                valid_ = false;
                return;
            }

            const uint32_t klen = DecodeFixed32(block_ptr);
            if (remaining < 4 + klen + 8 + 1 + 4) {
                valid_ = false;
                return;
            }

            std::string user_key(block_ptr + 4, klen);
            const uint64_t version = DecodeFixed64(block_ptr + 4 + klen);
            const uint8_t type_byte = static_cast<uint8_t>(block_ptr[4 + klen + 8]);
            const uint32_t vlen = DecodeFixed32(block_ptr + 4 + klen + 8 + 1);

            if (remaining < 4 + klen + 8 + 1 + 4 + vlen) {
                valid_ = false;
                return;
            }

            const char* val_ptr = block_ptr + 4 + klen + 8 + 1 + 4;
            current_key_ = InternalKey{std::move(user_key), version, static_cast<ValueType>(type_byte)};
            current_value_ = std::string_view(val_ptr, vlen);
            entry_total_len_ = 4 + klen + 8 + 1 + 4 + vlen;
            valid_ = true;
            return;
        }
    }

    const SSTableReader* reader_{nullptr};
    size_t block_idx_{0};
    size_t offset_in_block_{0};
    size_t entry_total_len_{0};

    InternalKey current_key_;
    std::string_view current_value_;
    bool valid_{false};
};

Status SSTableReader::Open(const std::string& filepath, std::unique_ptr<SSTableReader>* reader) {
    if (!reader) {
        return {Status::Code::kInvalidArgument, "Output reader pointer cannot be null"};
    }

    auto r = std::unique_ptr<SSTableReader>(new SSTableReader());
    Status s = r->Init(filepath);
    if (!s.ok()) {
        return s;
    }

    *reader = std::move(r);
    return Status::Ok();
}

SSTableReader::~SSTableReader() {
    if (data_ && data_ != MAP_FAILED) {
        munmap(const_cast<char*>(data_), file_size_);
        data_ = nullptr;
    }
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

Status SSTableReader::Init(const std::string& filepath) {
    fd_ = open(filepath.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return Status::IOError("Failed to open SSTable file: " + filepath + " (" + strerror(errno) + ")");
    }

    struct stat st{};
    if (fstat(fd_, &st) != 0) {
        close(fd_);
        fd_ = -1;
        return Status::IOError("Failed to stat SSTable file: " + std::string(strerror(errno)));
    }

    file_size_ = static_cast<size_t>(st.st_size);
    if (file_size_ < kFooterSize) {
        close(fd_);
        fd_ = -1;
        return Status::Corruption("File size is smaller than footer size");
    }

    void* mmap_addr = mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (mmap_addr == MAP_FAILED) {
        close(fd_);
        fd_ = -1;
        return Status::IOError("Failed to mmap SSTable file: " + std::string(strerror(errno)));
    }
    data_ = static_cast<const char*>(mmap_addr);

    // Read and validate Footer (last 48 bytes)
    const char* footer_ptr = data_ + file_size_ - kFooterSize;
    const uint64_t filter_offset = DecodeFixed64(footer_ptr);
    const uint64_t filter_size = DecodeFixed64(footer_ptr + 8);
    const uint64_t index_offset = DecodeFixed64(footer_ptr + 16);
    const uint64_t index_size = DecodeFixed64(footer_ptr + 24);
    max_version_ = DecodeFixed64(footer_ptr + 32);
    const uint64_t magic_number = DecodeFixed64(footer_ptr + 40);

    if (magic_number != kSSTableMagicNumber) {
        return Status::Corruption("Invalid SSTable magic number");
    }

    if (filter_offset + filter_size > file_size_ - kFooterSize ||
        index_offset + index_size > file_size_ - kFooterSize) {
        return Status::Corruption("SSTable block offsets exceed file boundaries");
    }

    // Filter block
    if (filter_size > 0) {
        filter_data_ = std::string_view(data_ + filter_offset, filter_size);
    }

    // Index block
    if (index_size > 0) {
        const char* ptr = data_ + index_offset;
        const char* end = ptr + index_size;

        if (end - ptr < 4) {
            return Status::Corruption("Index block too small for header");
        }

        const uint32_t num_entries = DecodeFixed32(ptr);
        ptr += 4;
        index_entries_.reserve(num_entries);

        for (uint32_t i = 0; i < num_entries; ++i) {
            if (end - ptr < 4) {
                return Status::Corruption("Index block corrupted at entry user key length");
            }
            const uint32_t klen = DecodeFixed32(ptr);
            ptr += 4;

            if (static_cast<size_t>(end - ptr) < klen + 8 + 1 + 8 + 8) {
                return Status::Corruption("Index block corrupted at entry contents");
            }

            std::string user_key(ptr, klen);
            ptr += klen;

            const uint64_t version = DecodeFixed64(ptr);
            ptr += 8;

            const uint8_t type = static_cast<uint8_t>(*ptr);
            ptr += 1;

            const uint64_t offset = DecodeFixed64(ptr);
            ptr += 8;

            const uint64_t size = DecodeFixed64(ptr);
            ptr += 8;

            index_entries_.push_back(BlockHandle{
                InternalKey{std::move(user_key), version, static_cast<ValueType>(type)},
                offset,
                size
            });
        }
    }

    return Status::Ok();
}

bool SSTableReader::MayContain(std::string_view user_key) const {
    if (filter_data_.empty()) {
        return true;
    }
    return BloomFilter::MayContain(user_key, filter_data_);
}

Status SSTableReader::GetAsOf(const std::string& key, Timestamp timestamp,
                              InternalKey* out_key, std::string* out_value) const {
    if (!out_key || !out_value) {
        return {Status::Code::kInvalidArgument, "Output pointers cannot be null"};
    }

    if (!MayContain(key)) {
        return Status::NotFound("Key absent in Bloom filter");
    }

    if (index_entries_.empty() || !data_) {
        return Status::NotFound("SSTable contains no data blocks");
    }

    // Search target: key at timestamp
    const InternalKey search_target{key, timestamp, ValueType::kValue};

    auto it = std::lower_bound(
        index_entries_.begin(),
        index_entries_.end(),
        search_target,
        [](const BlockHandle& handle, const InternalKey& target) {
            return handle.last_key < target;
        }
    );

    if (it == index_entries_.end()) {
        return Status::NotFound("Key beyond last entry in SSTable");
    }

    for (; it != index_entries_.end(); ++it) {
        const char* block_ptr = data_ + it->offset;
        const size_t block_size = it->size;
        size_t offset = 0;
        bool found_user_key = false;

        while (offset + 4 <= block_size) {
            const uint32_t klen = DecodeFixed32(block_ptr + offset);
            offset += 4;
            if (offset + klen + 8 + 1 + 4 > block_size) {
                break;
            }

            std::string_view entry_user_key(block_ptr + offset, klen);
            offset += klen;

            const uint64_t version = DecodeFixed64(block_ptr + offset);
            offset += 8;

            const uint8_t type_byte = static_cast<uint8_t>(block_ptr[offset]);
            offset += 1;

            const uint32_t vlen = DecodeFixed32(block_ptr + offset);
            offset += 4;
            if (offset + vlen > block_size) {
                break;
            }

            std::string_view val(block_ptr + offset, vlen);
            offset += vlen;

            if (entry_user_key == key) {
                found_user_key = true;
                if (version <= timestamp) {
                    *out_key = InternalKey{key, version, static_cast<ValueType>(type_byte)};
                    *out_value = std::string(val);
                    return Status::Ok();
                }
            } else if (found_user_key || entry_user_key > key) {
                return Status::NotFound("Key not found at or before requested timestamp");
            }
        }

        if (it->last_key.user_key > key) {
            break;
        }
    }

    return Status::NotFound("Key not found");
}

Status SSTableReader::Get(std::string_view user_key, std::string* value, Timestamp snapshot_timestamp) const {
    if (!value) {
        return {Status::Code::kInvalidArgument, "Output value pointer cannot be null"};
    }
    InternalKey out_key;
    Status s = GetAsOf(std::string(user_key), snapshot_timestamp, &out_key, value);
    if (!s.ok()) {
        return s;
    }
    if (out_key.type == ValueType::kTombstone) {
        return Status::NotFound("Key is deleted (tombstone)");
    }
    return Status::Ok();
}

std::unique_ptr<Iterator> SSTableReader::NewIterator() const {
    return std::make_unique<SSTableIterator>(this);
}

} // namespace kvstore
