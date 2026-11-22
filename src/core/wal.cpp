#include "kvstore/wal.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace kvstore {
namespace {
constexpr uint32_t kMagic = 0x314c4157; // WAL1, encoded little-endian
constexpr std::size_t kHeaderSize = 24;
constexpr uint32_t kMaxFieldSize = 64U * 1024U * 1024U;

void Encode32(uint8_t* p, uint32_t v) { for (int i=0;i<4;++i) p[i]=static_cast<uint8_t>(v>>(i*8)); }
void Encode64(uint8_t* p, uint64_t v) { for (int i=0;i<8;++i) p[i]=static_cast<uint8_t>(v>>(i*8)); }
uint32_t Decode32(const uint8_t* p) { uint32_t v=0; for(int i=3;i>=0;--i) v=(v<<8)|p[i]; return v; }
uint64_t Decode64(const uint8_t* p) { uint64_t v=0; for(int i=7;i>=0;--i) v=(v<<8)|p[i]; return v; }

uint32_t Crc32c(const uint8_t* data, std::size_t size) {
    uint32_t crc = ~uint32_t{0};
    for (std::size_t i=0;i<size;++i) {
        crc ^= data[i];
        for (int bit=0;bit<8;++bit) crc=(crc>>1) ^ (0x82f63b78U & (0U-(crc&1U)));
    }
    return ~crc;
}

bool WriteAll(int fd, const uint8_t* data, std::size_t size) {
    while (size) {
        ssize_t n = ::write(fd, data, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        data += n; size -= static_cast<std::size_t>(n);
    }
    return true;
}
}

WAL::WAL(std::string path) : path_(std::move(path)) {}
WAL::~WAL() { if (fd_ >= 0) ::close(fd_); }

Status WAL::Open() {
    if (fd_ >= 0) return Status::Ok();
    fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR | O_APPEND | O_CLOEXEC, 0644);
    return fd_ < 0 ? Status::IOError(std::strerror(errno)) : Status::Ok();
}

Status WAL::Append(const InternalKey& key, const std::string& value) {
    if (fd_ < 0) { auto s=Open(); if (!s.ok()) return s; }
    if (key.user_key.size()>kMaxFieldSize || value.size()>kMaxFieldSize)
        return {Status::Code::kInvalidArgument, "WAL record field too large"};
    std::vector<uint8_t> record(kHeaderSize + key.user_key.size() + value.size());
    Encode32(record.data(), kMagic);
    Encode32(record.data()+4, static_cast<uint32_t>(key.user_key.size()));
    Encode32(record.data()+8, static_cast<uint32_t>(value.size()));
    Encode64(record.data()+12, key.version);
    record[20]=static_cast<uint8_t>(key.type);
    record[21]=record[22]=record[23]=0;
    std::memcpy(record.data()+kHeaderSize, key.user_key.data(), key.user_key.size());
    std::memcpy(record.data()+kHeaderSize+key.user_key.size(), value.data(), value.size());
    const auto checksum=Crc32c(record.data(), record.size());
    const auto old=record.size(); record.resize(old+4); Encode32(record.data()+old, checksum);
    if (!WriteAll(fd_, record.data(), record.size()) || ::fsync(fd_) != 0)
        return Status::IOError(std::strerror(errno));
    return Status::Ok();
}

Status WAL::Replay(MemTable* memtable) {
    if (!memtable) return {Status::Code::kInvalidArgument, "memtable must not be null"};
    if (fd_ < 0) { auto s=Open(); if (!s.ok()) return s; }
    struct stat st{};
    if (::fstat(fd_, &st) != 0) return Status::IOError(std::strerror(errno));
    off_t offset=0;
    while (offset < st.st_size) {
        std::array<uint8_t,kHeaderSize> header{};
        ssize_t n=::pread(fd_, header.data(), header.size(), offset);
        if (n != static_cast<ssize_t>(header.size())) break;
        uint32_t key_len=Decode32(header.data()+4), value_len=Decode32(header.data()+8);
        if (Decode32(header.data())!=kMagic || key_len>kMaxFieldSize || value_len>kMaxFieldSize ||
            header[20]>static_cast<uint8_t>(ValueType::kValuePointer)) break;
        uint64_t total=kHeaderSize+static_cast<uint64_t>(key_len)+value_len+4;
        if (total > static_cast<uint64_t>(st.st_size-offset)) break;
        std::vector<uint8_t> body(static_cast<std::size_t>(total));
        std::size_t done=0;
        while(done<body.size()) {
            n=::pread(fd_, body.data()+done, body.size()-done, offset+static_cast<off_t>(done));
            if(n<0 && errno==EINTR) continue;
            if(n<=0) break;
            done+=static_cast<std::size_t>(n);
        }
        if(done!=body.size() || Crc32c(body.data(), body.size()-4)!=Decode32(body.data()+body.size()-4)) break;
        std::string key(reinterpret_cast<char*>(body.data()+kHeaderSize),key_len);
        std::string value(reinterpret_cast<char*>(body.data()+kHeaderSize+key_len),value_len);
        memtable->Put({std::move(key),Decode64(header.data()+12),static_cast<ValueType>(header[20])},std::move(value));
        offset += static_cast<off_t>(total);
    }
    if (::ftruncate(fd_, offset)!=0 || ::fsync(fd_)!=0) return Status::IOError(std::strerror(errno));
    return Status::Ok();
}

Status WAL::Close() {
    if(fd_<0) return Status::Ok();
    int fd=fd_; fd_=-1;
    if(::close(fd)!=0) return Status::IOError(std::strerror(errno));
    return Status::Ok();
}
} // namespace kvstore
