#pragma once

#include "kvstore/types.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

struct VLogRecord {
    std::string user_key;
    Timestamp version = 0;
    std::string value;
    ValuePointer pointer;
};

std::string EncodeValuePointer(const ValuePointer& pointer);
Status DecodeValuePointer(std::string_view encoded, ValuePointer* pointer);

class VLog {
public:
    explicit VLog(std::string vlog_dir,
                  std::size_t max_segment_size = 16U * 1024U * 1024U);
    ~VLog();
    VLog(const VLog&) = delete;
    VLog& operator=(const VLog&) = delete;

    Status Open();
    Status Append(const std::string& user_key, Timestamp version,
                  const std::string& value, ValuePointer* out_ptr);
    Status Read(const ValuePointer& ptr, std::string* out_value) const;
    std::vector<uint32_t> SealedSegmentIds() const;
    Status ScanSegment(uint32_t file_id,
                       const std::function<Status(const VLogRecord&)>& visitor) const;
    Status RemoveSegment(uint32_t file_id);

private:
    std::string SegmentPath(uint32_t file_id) const;

    std::string vlog_dir_;
    std::size_t max_segment_size_;
    mutable std::shared_mutex mutex_;
    uint32_t active_file_id_ = 0;
    uint64_t active_size_ = 0;
    int active_fd_ = -1;
    std::vector<uint32_t> segment_ids_;
    std::map<uint32_t, std::string> segment_paths_;
};

} // namespace kvstore
