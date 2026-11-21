#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

class BloomFilter {
public:
    explicit BloomFilter(size_t bits_per_key = 10);

    // Hashes user_key strings only (no timestamps), enforces a minimum bit array
    // size of 64 bits, and appends 1 trailing byte storing k_ (self-describing).
    std::string CreateFilter(const std::vector<std::string>& user_keys) const;

    // Returns false if any probed bit is 0 (0% false negatives) and true otherwise.
    // Safely returns true if filter_data is empty or malformed (size < 2 or k > 30).
    static bool MayContain(std::string_view user_key, std::string_view filter_data);

    size_t bits_per_key() const noexcept { return bits_per_key_; }
    size_t k() const noexcept { return k_; }

private:
    size_t bits_per_key_;
    size_t k_;
};

} // namespace kvstore

namespace lsm {
    using BloomFilter = kvstore::BloomFilter;
} // namespace lsm
