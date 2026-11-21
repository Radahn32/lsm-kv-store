#include "storage/bloom_filter.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace kvstore {

TEST(BloomFilterTest, ConstructorProbesClamping) {
    BloomFilter bf_default;
    EXPECT_EQ(bf_default.k(), 6U); // clamp(10 * 0.69 = 6, 1, 30)

    BloomFilter bf_small(1);
    EXPECT_EQ(bf_small.k(), 1U); // clamp(1 * 0.69 = 0, 1, 30) -> 1

    BloomFilter bf_zero(0);
    EXPECT_EQ(bf_zero.k(), 1U); // clamp(0, 1, 30) -> 1

    BloomFilter bf_large(50);
    EXPECT_EQ(bf_large.k(), 30U); // clamp(50 * 0.69 = 34, 1, 30) -> 30
}

TEST(BloomFilterTest, EmptyAndMalformedFilterSafelyReturnsTrue) {
    // Empty data
    EXPECT_TRUE(BloomFilter::MayContain("any_key", ""));

    // Size < 2 (only 1 byte)
    std::string one_byte = "x";
    EXPECT_TRUE(BloomFilter::MayContain("any_key", one_byte));

    // Malformed k > 30
    std::string malformed_high_k(10, '\0');
    malformed_high_k.back() = static_cast<char>(31);
    EXPECT_TRUE(BloomFilter::MayContain("any_key", malformed_high_k));

    // Malformed k == 0
    std::string malformed_zero_k(10, '\0');
    malformed_zero_k.back() = static_cast<char>(0);
    EXPECT_TRUE(BloomFilter::MayContain("any_key", malformed_zero_k));
}

TEST(BloomFilterTest, MinimumBitArraySizeEnforced) {
    BloomFilter bf(10);
    // 0 keys: minimum 64 bits (8 bytes) + 1 trailing byte = 9 bytes
    std::string filter_empty = bf.CreateFilter({});
    EXPECT_EQ(filter_empty.size(), 9U);
    EXPECT_EQ(static_cast<uint8_t>(filter_empty.back()), bf.k());
    // Since no keys were inserted, all bits are 0, so MayContain should be false
    EXPECT_FALSE(BloomFilter::MayContain("any_key", filter_empty));

    // 1 key: 1 * 10 = 10 bits < 64 bits -> clamped to 64 bits (8 bytes + 1 trailing byte = 9 bytes)
    std::string filter_single = bf.CreateFilter({"key1"});
    EXPECT_EQ(filter_single.size(), 9U);
    EXPECT_TRUE(BloomFilter::MayContain("key1", filter_single));
    EXPECT_EQ(static_cast<uint8_t>(filter_single.back()), bf.k());
}

TEST(BloomFilterTest, ZeroFalseNegativesOnTenThousandKeys) {
    BloomFilter bf(10);
    constexpr size_t kNumKeys = 10000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    for (size_t i = 0; i < kNumKeys; ++i) {
        keys.push_back("user_key_" + std::to_string(i));
    }

    std::string filter = bf.CreateFilter(keys);
    // Minimum 10000 * 10 = 100,000 bits -> 12,500 bytes + 1 trailing byte = 12,501 bytes
    EXPECT_EQ(filter.size(), 12501U);
    EXPECT_EQ(static_cast<uint8_t>(filter.back()), 6U);

    // Verify 100% of inserted keys return true (0 false negatives)
    for (size_t i = 0; i < kNumKeys; ++i) {
        ASSERT_TRUE(BloomFilter::MayContain(keys[i], filter))
            << "False negative encountered for key: " << keys[i];
    }
}

TEST(BloomFilterTest, FalsePositiveRateBelowTwoPercent) {
    BloomFilter bf(10);
    constexpr size_t kNumKeys = 10000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    for (size_t i = 0; i < kNumKeys; ++i) {
        keys.push_back("user_key_" + std::to_string(i));
    }

    std::string filter = bf.CreateFilter(keys);

    // Query 10,000 non-existent keys
    size_t false_positives = 0;
    for (size_t i = kNumKeys; i < 2 * kNumKeys; ++i) {
        std::string non_existent_key = "user_key_" + std::to_string(i);
        if (BloomFilter::MayContain(non_existent_key, filter)) {
            ++false_positives;
        }
    }

    const double fp_rate = static_cast<double>(false_positives) / static_cast<double>(kNumKeys);
    // Standard 10 bits/key Bloom filter has theoretical FP rate ~0.84%
    // Verify false positive rate is strictly below 2%
    EXPECT_LT(fp_rate, 0.02)
        << "False positive rate too high: " << (fp_rate * 100.0)
        << "% (" << false_positives << "/" << kNumKeys << ")";
}

} // namespace kvstore
