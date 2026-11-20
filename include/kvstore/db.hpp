#pragma once

#include "kvstore/types.hpp"
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace kvstore {

struct DiffResult {
    std::optional<std::string> old_value;
    std::optional<std::string> new_value;
    bool changed = false;
};

class DB {
public:
    virtual ~DB() = default;

    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Delete(const std::string& key) = 0;
    virtual Status Scan(const std::string& start_key,
                        const std::string& end_key,
                        std::vector<std::pair<std::string, std::string>>* results) = 0;

    // Time-travel queries
    virtual Status GetAsOf(const std::string& key, Timestamp ts, std::string* value) = 0;
    virtual Status Diff(const std::string& key, Timestamp t1, Timestamp t2, DiffResult* result) = 0;
};

} // namespace kvstore
