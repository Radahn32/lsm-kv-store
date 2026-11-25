#include "kvstore/vlog.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace kvstore {
namespace {

constexpr uint32_t kMagic = 0x31474c56U;
constexpr std::size_t kHeaderSize = 20;
constexpr std::size_t kTrailerSize = 4;
constexpr uint32_t kMaxFieldSize = 64U * 1024U * 1024U;

void Encode32(uint8_t* output, uint32_t value) {
    for (int i = 0; i < 4; ++i) output[i] = static_cast<uint8_t>(value >> (i * 8));
}

void Encode64(uint8_t* output, uint64_t value) {
    for (int i = 0; i < 8; ++i) output[i] = static_cast<uint8_t>(value >> (i * 8));
}

uint32_t Decode32(const uint8_t* input) {
    uint32_t value = 0;
    for (int i = 3; i >= 0; --i) value = (value << 8) | input[i];
    return value;
}

uint64_t Decode64(const uint8_t* input) {
    uint64_t value = 0;
    for (int i = 7; i >= 0; --i) value = (value << 8) | input[i];
    return value;
}

uint32_t Crc32c(const uint8_t* data, std::size_t size) {
    uint32_t crc = ~uint32_t{0};
    for (std::size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ (0x82f63b78U & (0U - (crc & 1U)));
        }
    }
    return ~crc;
}

bool WriteAll(int fd, const uint8_t* data, std::size_t size) {
    while (size > 0) {
        const ssize_t written = ::write(fd, data, size);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) return false;
        data += written;
        size -= static_cast<std::size_t>(written);
    }
    return true;
}

bool ReadAllAt(int fd, uint8_t* data, std::size_t size, uint64_t offset) {
    std::size_t read_bytes = 0;
    while (read_bytes < size) {
        const ssize_t count = ::pread(fd, data + read_bytes, size - read_bytes,
                                      static_cast<off_t>(offset + read_bytes));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        read_bytes += static_cast<std::size_t>(count);
    }
    return true;
}

Status IOErrorFromErrno(const std::string& context) {
    return Status::IOError(context + ": " + std::strerror(errno));
}

Status ParseRecord(int fd, uint64_t offset, uint64_t file_size,
                   VLogRecord* record, uint32_t* record_size) {
    if (offset > file_size || file_size - offset < kHeaderSize) {
        return Status::IOError("short vLog record header");
    }
    std::array<uint8_t, kHeaderSize> header{};
    if (!ReadAllAt(fd, header.data(), header.size(), offset)) {
        return IOErrorFromErrno("read vLog record header");
    }
    if (Decode32(header.data()) != kMagic) {
        return Status::Corruption("invalid vLog record magic");
    }
    const uint32_t key_size = Decode32(header.data() + 4);
    const uint32_t value_size = Decode32(header.data() + 8);
    if (key_size > kMaxFieldSize || value_size > kMaxFieldSize) {
        return Status::Corruption("vLog field is too large");
    }
    const uint64_t total_size = kHeaderSize + static_cast<uint64_t>(key_size) +
                                value_size + kTrailerSize;
    if (total_size > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("vLog record size overflow");
    }
    if (total_size > file_size - offset) {
        return Status::IOError("short vLog record body");
    }
    std::vector<uint8_t> frame(static_cast<std::size_t>(total_size));
    std::copy(header.begin(), header.end(), frame.begin());
    if (!ReadAllAt(fd, frame.data() + kHeaderSize,
                   frame.size() - kHeaderSize, offset + kHeaderSize)) {
        return IOErrorFromErrno("read vLog record body");
    }
    const uint32_t stored_crc = Decode32(frame.data() + frame.size() - kTrailerSize);
    const uint32_t actual_crc = Crc32c(frame.data(), frame.size() - kTrailerSize);
    if (stored_crc != actual_crc) {
        return Status::Corruption("vLog record checksum mismatch");
    }
    if (record) {
        record->user_key.assign(reinterpret_cast<const char*>(frame.data() + kHeaderSize), key_size);
        record->version = Decode64(header.data() + 12);
        record->value.assign(reinterpret_cast<const char*>(frame.data() + kHeaderSize + key_size),
                             value_size);
        record->pointer = {0, offset, static_cast<uint32_t>(total_size)};
    }
    *record_size = static_cast<uint32_t>(total_size);
    return Status::Ok();
}

} // namespace

std::string EncodeValuePointer(const ValuePointer& pointer) {
    std::array<uint8_t, 16> encoded{};
    Encode32(encoded.data(), pointer.file_id);
    Encode64(encoded.data() + 4, pointer.offset);
    Encode32(encoded.data() + 12, pointer.size);
    return {reinterpret_cast<const char*>(encoded.data()), encoded.size()};
}

Status DecodeValuePointer(std::string_view encoded, ValuePointer* pointer) {
    if (!pointer) return {Status::Code::kInvalidArgument, "pointer output must not be null"};
    if (encoded.size() != 16) return Status::Corruption("invalid encoded value pointer size");
    const auto* bytes = reinterpret_cast<const uint8_t*>(encoded.data());
    *pointer = {Decode32(bytes), Decode64(bytes + 4), Decode32(bytes + 12)};
    return Status::Ok();
}

VLog::VLog(std::string vlog_dir, std::size_t max_segment_size)
    : vlog_dir_(std::move(vlog_dir)), max_segment_size_(max_segment_size) {}

VLog::~VLog() {
    std::unique_lock lock(mutex_);
    if (active_fd_ >= 0) ::close(active_fd_);
}

std::string VLog::SegmentPath(uint32_t file_id) const {
    std::ostringstream filename;
    filename << std::setw(6) << std::setfill('0') << file_id << ".vlog";
    return (std::filesystem::path(vlog_dir_) / filename.str()).string();
}

Status VLog::Open() {
    std::unique_lock lock(mutex_);
    if (active_fd_ >= 0) return Status::Ok();
    std::error_code ec;
    std::filesystem::create_directories(vlog_dir_, ec);
    if (ec) return Status::IOError(ec.message());

    std::vector<std::pair<uint32_t, std::string>> discovered;
    for (std::filesystem::directory_iterator item(vlog_dir_, ec), end;
         !ec && item != end; item.increment(ec)) {
        const std::string filename = item->path().filename().string();
        constexpr std::string_view suffix = ".vlog";
        if (filename.size() <= suffix.size() ||
            filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) != 0) {
            continue;
        }
        if (!item->is_regular_file(ec)) {
            if (ec) return Status::IOError(ec.message());
            continue;
        }
        const std::string_view digits(filename.data(), filename.size() - suffix.size());
        if (!std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; })) {
            continue;
        }
        uint32_t id = 0;
        const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), id);
        if (parsed.ec == std::errc{} && parsed.ptr == digits.data() + digits.size() && id != 0) {
            discovered.emplace_back(id, item->path().string());
        }
    }
    if (ec) return Status::IOError(ec.message());
    std::sort(discovered.begin(), discovered.end(), [](const auto& left, const auto& right) {
        if (left.first != right.first) return left.first < right.first;
        return left.second < right.second;
    });
    std::map<uint32_t, std::string> paths_by_id;
    for (const auto& [id, path] : discovered) paths_by_id.emplace(id, path);
    std::vector<uint32_t> ids;
    ids.reserve(paths_by_id.size());
    for (const auto& [id, path] : paths_by_id) {
        (void)path;
        ids.push_back(id);
    }

    const uint32_t active_id = ids.empty() ? 1U : ids.back();
    const std::string active_path = ids.empty() ? SegmentPath(active_id) : paths_by_id.at(active_id);
    const int fd = ::open(active_path.c_str(), O_CREAT | O_RDWR | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return IOErrorFromErrno("open vLog segment");

    uint64_t size = 0;
    if (!ids.empty()) {
        struct stat stat_buffer {};
        if (::fstat(fd, &stat_buffer) != 0) {
            const Status status = IOErrorFromErrno("stat vLog segment");
            ::close(fd);
            return status;
        }
        uint64_t offset = 0;
        while (offset < static_cast<uint64_t>(stat_buffer.st_size)) {
            uint32_t record_size = 0;
            const Status status = ParseRecord(fd, offset, static_cast<uint64_t>(stat_buffer.st_size),
                                              nullptr, &record_size);
            if (!status.ok()) {
                if (::ftruncate(fd, static_cast<off_t>(offset)) != 0 || ::fsync(fd) != 0) {
                    const Status truncate_status = IOErrorFromErrno("truncate damaged vLog tail");
                    ::close(fd);
                    return truncate_status;
                }
                size = offset;
                break;
            }
            offset += record_size;
            size = offset;
        }
    }

    active_fd_ = fd;
    active_file_id_ = active_id;
    active_size_ = size;
    segment_ids_ = std::move(ids);
    segment_paths_ = std::move(paths_by_id);
    if (segment_ids_.empty()) segment_ids_.push_back(active_id);
    if (segment_paths_.empty()) segment_paths_.emplace(active_id, active_path);
    return Status::Ok();
}

Status VLog::Append(const std::string& user_key, Timestamp version,
                    const std::string& value, ValuePointer* out_ptr) {
    if (!out_ptr) return {Status::Code::kInvalidArgument, "value pointer output must not be null"};
    if (user_key.size() > kMaxFieldSize || value.size() > kMaxFieldSize) {
        return {Status::Code::kInvalidArgument, "vLog record field too large"};
    }
    const uint64_t total_size = kHeaderSize + static_cast<uint64_t>(user_key.size()) +
                                value.size() + kTrailerSize;
    if (total_size > std::numeric_limits<uint32_t>::max()) {
        return {Status::Code::kInvalidArgument, "vLog record too large"};
    }

    std::unique_lock lock(mutex_);
    if (active_fd_ < 0) return Status::IOError("vLog is not open");
    if (active_size_ > 0 &&
        (total_size > std::numeric_limits<uint64_t>::max() - active_size_ ||
         active_size_ + total_size > max_segment_size_)) {
        if (active_file_id_ == std::numeric_limits<uint32_t>::max()) {
            return Status::IOError("vLog file ID exhausted");
        }
        const uint32_t next_id = active_file_id_ + 1;
        const std::string path = SegmentPath(next_id);
        const int next_fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_APPEND | O_CLOEXEC,
                                   0644);
        if (next_fd < 0) return IOErrorFromErrno("create vLog segment");
        if (::close(active_fd_) != 0) {
            const Status status = IOErrorFromErrno("close vLog segment");
            ::close(next_fd);
            return status;
        }
        active_fd_ = next_fd;
        active_file_id_ = next_id;
        active_size_ = 0;
        segment_ids_.push_back(next_id);
        segment_paths_.emplace(next_id, path);
    }

    std::vector<uint8_t> frame(static_cast<std::size_t>(total_size));
    Encode32(frame.data(), kMagic);
    Encode32(frame.data() + 4, static_cast<uint32_t>(user_key.size()));
    Encode32(frame.data() + 8, static_cast<uint32_t>(value.size()));
    Encode64(frame.data() + 12, version);
    std::copy(user_key.begin(), user_key.end(), frame.begin() + kHeaderSize);
    std::copy(value.begin(), value.end(), frame.begin() + kHeaderSize + user_key.size());
    const std::size_t crc_offset = frame.size() - kTrailerSize;
    Encode32(frame.data() + crc_offset, Crc32c(frame.data(), crc_offset));

    const uint64_t offset = active_size_;
    if (!WriteAll(active_fd_, frame.data(), frame.size()) || ::fsync(active_fd_) != 0) {
        return IOErrorFromErrno("append vLog record");
    }
    active_size_ += total_size;
    *out_ptr = {active_file_id_, offset, static_cast<uint32_t>(total_size)};
    return Status::Ok();
}

Status VLog::Read(const ValuePointer& ptr, std::string* out_value) const {
    if (!out_value) return {Status::Code::kInvalidArgument, "value output must not be null"};
    std::shared_lock lock(mutex_);
    if (ptr.file_id == 0 || ptr.size < kHeaderSize + kTrailerSize ||
        !std::binary_search(segment_ids_.begin(), segment_ids_.end(), ptr.file_id)) {
        return Status::IOError("vLog segment is unavailable");
    }
    int fd = -1;
    bool close_fd = false;
    if (ptr.file_id == active_file_id_) {
        fd = active_fd_;
    } else {
        fd = ::open(segment_paths_.at(ptr.file_id).c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return IOErrorFromErrno("open vLog segment for read");
        close_fd = true;
    }
    struct stat stat_buffer {};
    if (::fstat(fd, &stat_buffer) != 0) {
        const Status status = IOErrorFromErrno("stat vLog segment for read");
        if (close_fd) ::close(fd);
        return status;
    }
    VLogRecord record;
    uint32_t record_size = 0;
    const Status parse_status = ParseRecord(fd, ptr.offset,
                                            static_cast<uint64_t>(stat_buffer.st_size),
                                            &record, &record_size);
    if (close_fd) ::close(fd);
    if (!parse_status.ok()) return parse_status;
    if (record_size != ptr.size) return Status::Corruption("vLog pointer size mismatch");
    record.pointer.file_id = ptr.file_id;
    *out_value = std::move(record.value);
    return Status::Ok();
}

std::vector<uint32_t> VLog::SealedSegmentIds() const {
    std::shared_lock lock(mutex_);
    std::vector<uint32_t> sealed;
    for (uint32_t id : segment_ids_) {
        if (id < active_file_id_) sealed.push_back(id);
    }
    return sealed;
}

Status VLog::ScanSegment(uint32_t file_id,
                         const std::function<Status(const VLogRecord&)>& visitor) const {
    if (!visitor) return {Status::Code::kInvalidArgument, "vLog visitor must not be empty"};
    int fd = -1;
    {
        std::shared_lock lock(mutex_);
        if (!std::binary_search(segment_ids_.begin(), segment_ids_.end(), file_id)) {
            return Status::IOError("vLog segment is unavailable");
        }
        fd = ::open(segment_paths_.at(file_id).c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return IOErrorFromErrno("open vLog segment for scan");
    }
    struct stat stat_buffer {};
    if (::fstat(fd, &stat_buffer) != 0) {
        const Status status = IOErrorFromErrno("stat vLog segment for scan");
        ::close(fd);
        return status;
    }
    uint64_t offset = 0;
    while (offset < static_cast<uint64_t>(stat_buffer.st_size)) {
        VLogRecord record;
        uint32_t record_size = 0;
        Status status = ParseRecord(fd, offset, static_cast<uint64_t>(stat_buffer.st_size),
                                    &record, &record_size);
        if (!status.ok()) {
            ::close(fd);
            return status;
        }
        record.pointer.file_id = file_id;
        status = visitor(record);
        if (!status.ok()) {
            ::close(fd);
            return status;
        }
        offset += record_size;
    }
    ::close(fd);
    return Status::Ok();
}

Status VLog::RemoveSegment(uint32_t file_id) {
    std::unique_lock lock(mutex_);
    if (file_id == active_file_id_) {
        return {Status::Code::kInvalidArgument, "cannot remove active vLog segment"};
    }
    const auto it = std::lower_bound(segment_ids_.begin(), segment_ids_.end(), file_id);
    if (it == segment_ids_.end() || *it != file_id) {
        return Status::IOError("vLog segment is unavailable");
    }
    std::error_code ec;
    const bool removed = std::filesystem::remove(segment_paths_.at(file_id), ec);
    if (ec) return Status::IOError(ec.message());
    if (!removed) return Status::IOError("vLog segment file is missing");
    segment_ids_.erase(it);
    segment_paths_.erase(file_id);
    return Status::Ok();
}

} // namespace kvstore
