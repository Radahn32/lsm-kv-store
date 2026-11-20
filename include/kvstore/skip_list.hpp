#pragma once

#include "kvstore/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace kvstore {

// A stable-node skip list. The owner supplies synchronization so readers can
// keep a shared lock while traversing nodes.
class SkipList {
public:
    static constexpr std::size_t kMaxHeight = 12;

    struct Node {
        InternalKey key;
        std::string value;
        std::array<Node*, kMaxHeight> next{};
        std::size_t height = 1;
    };

    SkipList() : head_(std::make_unique<Node>()) { head_->height = kMaxHeight; }
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    Node* Find(const InternalKey& key) const {
        Node* node = head_.get();
        for (std::size_t level = kMaxHeight; level-- > 0;) {
            while (node->next[level] && node->next[level]->key < key) node = node->next[level];
        }
        node = node->next[0];
        return node && node->key == key ? node : nullptr;
    }

    Node* LowerBound(const InternalKey& key) const {
        Node* node = head_.get();
        for (std::size_t level = kMaxHeight; level-- > 0;) {
            while (node->next[level] && node->next[level]->key < key) node = node->next[level];
        }
        return node->next[0];
    }

    Node* First() const { return head_->next[0]; }
    static Node* Next(Node* node) { return node ? node->next[0] : nullptr; }

    // Replaces an existing exact InternalKey or inserts a new stable node.
    Node* InsertOrAssign(InternalKey key, std::string value) {
        std::array<Node*, kMaxHeight> prev{};
        Node* node = head_.get();
        for (std::size_t level = kMaxHeight; level-- > 0;) {
            while (node->next[level] && node->next[level]->key < key) node = node->next[level];
            prev[level] = node;
        }
        node = node->next[0];
        if (node && node->key == key) {
            node->value = std::move(value);
            return node;
        }
        const std::size_t height = HeightFor(key);
        auto inserted = std::make_unique<Node>();
        inserted->key = std::move(key);
        inserted->value = std::move(value);
        inserted->height = height;
        for (std::size_t level = 0; level < height; ++level) {
            inserted->next[level] = prev[level]->next[level];
            prev[level]->next[level] = inserted.get();
        }
        node = inserted.get();
        nodes_.push_back(std::move(inserted));
        return node;
    }

private:
    static std::size_t HeightFor(const InternalKey& key) {
        uint64_t hash = 1469598103934665603ULL;
        for (unsigned char ch : key.user_key) { hash ^= ch; hash *= 1099511628211ULL; }
        hash ^= key.version;
        hash *= 1099511628211ULL;
        hash ^= static_cast<uint8_t>(key.type);
        std::size_t height = 1;
        while (height < kMaxHeight && (hash & 3U) == 0U) { ++height; hash >>= 2U; }
        return height;
    }

    std::unique_ptr<Node> head_;
    std::vector<std::unique_ptr<Node>> nodes_;
};

} // namespace kvstore
