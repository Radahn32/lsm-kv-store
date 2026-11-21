#include "storage/bloom_filter.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

namespace {

constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

// Deterministic 64-bit FNV-1a hash function (zero external dependencies)
inline uint64_t Fnv1a64(std::string_view data) noexcept {
    uint64_t hash = kFnvOffsetBasis;
    for (unsigned char c : data) {
        hash ^= static_cast<uint64_t>(c);
        hash *= kFnvPrime;
    }
    return hash;
}

} // namespace

BloomFilter::BloomFilter(size_t bits_per_key)
    : bits_per_key_(bits_per_key),
      k_(std::clamp<size_t>(static_cast<size_t>(bits_per_key * 0.69), 1, 30)) {}

std::string BloomFilter::CreateFilter(const std::vector<std::string>& user_keys) const {
    size_t bits = user_keys.size() * bits_per_key_;
    if (bits < 64) {
        bits = 64;
    }

    const size_t num_bytes = (bits + 7) / 8;
    const size_t total_bits = num_bytes * 8;

    std::string filter(num_bytes, '\0');

    for (const auto& key : user_keys) {
        const uint64_t hash = Fnv1a64(key);
        const uint32_t h1 = static_cast<uint32_t>(hash);
        const uint32_t h2 = static_cast<uint32_t>(hash >> 32);

        for (size_t i = 0; i < k_; ++i) {
            const size_t bit_pos = (static_cast<uint64_t>(h1) + static_cast<uint64_t>(i) * h2) % total_bits;
            filter[bit_pos / 8] |= static_cast<char>(1U << (bit_pos % 8));
        }
    }

    filter.push_back(static_cast<char>(k_));
    return filter;
}

bool BloomFilter::MayContain(std::string_view user_key, std::string_view filter_data) {
    if (filter_data.size() < 2) {
        return true;
    }

    const size_t k = static_cast<uint8_t>(filter_data.back());
    if (k == 0 || k > 30) {
        return true;
    }

    const size_t num_bytes = filter_data.size() - 1;
    const size_t total_bits = num_bytes * 8;
    if (total_bits == 0) {
        return true;
    }

    const uint64_t hash = Fnv1a64(user_key);
    const uint32_t h1 = static_cast<uint32_t>(hash);
    const uint32_t h2 = static_cast<uint32_t>(hash >> 32);

    for (size_t i = 0; i < k; ++i) {
        const size_t bit_pos = (static_cast<uint64_t>(h1) + static_cast<uint64_t>(i) * h2) % total_bits;
        const uint8_t byte_val = static_cast<uint8_t>(filter_data[bit_pos / 8]);
        if ((byte_val & (1U << (bit_pos % 8))) == 0) {
            return false;
        }
    }

    return true;
}

} // namespace kvstore
