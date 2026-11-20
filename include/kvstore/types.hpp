#pragma once

#include <cstdint>
#include <string>

namespace kvstore {

using Timestamp = uint64_t;

enum class ValueType : uint8_t {
    kValue = 0,       // Inline value stored directly in MemTable/SSTable
    kTombstone = 1,   // Deletion marker
    kValuePointer = 2 // Pointer to a value stored in the vLog
};

// Every key in the LSM tree is versioned.
// Sort order: user_key ASC, version DESC (newest version first).
struct InternalKey {
    std::string user_key;
    Timestamp version = 0;
    ValueType type = ValueType::kValue;

    bool operator<(const InternalKey& other) const {
        if (user_key != other.user_key) {
            return user_key < other.user_key;
        }
        return version > other.version; // Newest version comes first
    }

    bool operator==(const InternalKey& other) const = default;
};

// Stored in SSTables when value separation (vLog) is active
struct ValuePointer {
    uint32_t file_id = 0;
    uint64_t offset = 0;
    uint32_t size = 0;
};

struct Status {
    enum class Code { kOk, kNotFound, kCorruption, kIOError, kInvalidArgument };
    Code code = Code::kOk;
    std::string message;

    bool ok() const { return code == Code::kOk; }
    bool is_not_found() const { return code == Code::kNotFound; }

    static Status Ok() { return {Code::kOk, ""}; }
    static Status NotFound(std::string msg = "") { return {Code::kNotFound, std::move(msg)}; }
    static Status Corruption(std::string msg) { return {Code::kCorruption, std::move(msg)}; }
    static Status IOError(std::string msg) { return {Code::kIOError, std::move(msg)}; }
};

} // namespace kvstore
