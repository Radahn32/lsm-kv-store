#pragma once

#include "kvstore/types.hpp"
#include <string_view>

namespace kvstore {

// Implemented by both MemTable (Person A) and SSTable (Person B)
// so flushing, scans, and compaction use one unified interface.
class Iterator {
public:
    virtual ~Iterator() = default;

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void Seek(const InternalKey& target) = 0;
    virtual void Next() = 0;

    virtual InternalKey Key() const = 0;
    virtual std::string_view Value() const = 0;
};

} // namespace kvstore
