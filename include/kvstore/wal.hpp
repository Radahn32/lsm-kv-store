#pragma once

#include "kvstore/memtable.hpp"

#include <string>

namespace kvstore {

class WAL {
public:
    explicit WAL(std::string path);
    ~WAL();
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    Status Open();
    Status Append(const InternalKey& key, const std::string& value);
    // Replays valid records and truncates an incomplete/corrupt tail.
    Status Replay(MemTable* memtable);
    Status Close();

private:
    std::string path_;
    int fd_ = -1;
};

} // namespace kvstore
