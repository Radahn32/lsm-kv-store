#include "storage/level_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore {

namespace {

inline void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(uint32_t)];
    std::memcpy(buf, &value, sizeof(uint32_t));
    dst->append(buf, sizeof(uint32_t));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(uint64_t)];
    std::memcpy(buf, &value, sizeof(uint64_t));
    dst->append(buf, sizeof(uint64_t));
}

inline uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    std::memcpy(&result, ptr, sizeof(uint32_t));
    return result;
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    std::memcpy(&result, ptr, sizeof(uint64_t));
    return result;
}

inline std::string EncodeKeyHex(const InternalKey& key) {
    std::string bin;
    PutFixed32(&bin, static_cast<uint32_t>(key.user_key.size()));
    bin.append(key.user_key);
    PutFixed64(&bin, key.version);
    bin.push_back(static_cast<char>(key.type));

    static const char kHexChars[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(bin.size() * 2);
    for (unsigned char c : bin) {
        hex.push_back(kHexChars[c >> 4]);
        hex.push_back(kHexChars[c & 0x0f]);
    }
    return hex;
}

inline InternalKey DecodeKeyHex(std::string_view hex) {
    if (hex.size() % 2 != 0) return InternalKey{};
    auto hex_val = [](char c) -> unsigned char {
        if (c >= '0' && c <= '9') return static_cast<unsigned char>(c - '0');
        if (c >= 'a' && c <= 'f') return static_cast<unsigned char>(c - 'a' + 10);
        if (c >= 'A' && c <= 'F') return static_cast<unsigned char>(c - 'A' + 10);
        return 0;
    };
    std::string bin;
    bin.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        unsigned char c = static_cast<unsigned char>((hex_val(hex[i]) << 4) | hex_val(hex[i + 1]));
        bin.push_back(static_cast<char>(c));
    }
    if (bin.size() < 4 + 8 + 1) return InternalKey{};
    uint32_t klen = DecodeFixed32(bin.data());
    if (bin.size() < 4 + klen + 8 + 1) return InternalKey{};
    std::string ukey(bin.data() + 4, klen);
    uint64_t ver = DecodeFixed64(bin.data() + 4 + klen);
    uint8_t type = static_cast<uint8_t>(bin[4 + klen + 8]);
    return InternalKey{std::move(ukey), ver, static_cast<ValueType>(type)};
}

Status WriteBuffer(int fd, const char* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t res = write(fd, data + written, len - written);
        if (res < 0) {
            if (errno == EINTR) continue;
            return Status::IOError("Failed to write to file: " + std::string(strerror(errno)));
        }
        written += static_cast<size_t>(res);
    }
    return Status::Ok();
}

} // namespace

// ===================== MergingIterator =====================

MergingIterator::MergingIterator(std::vector<std::unique_ptr<Iterator>> children,
                                 std::vector<std::shared_ptr<TableMeta>> metas)
    : children_(std::move(children)), metas_(std::move(metas)) {
    SeekToFirst();
}

void MergingIterator::UpdateCurrent() {
    if (min_heap_.empty()) {
        valid_ = false;
        current_key_ = InternalKey{};
        current_value_ = std::string_view{};
        return;
    }
    const auto& top = min_heap_.top();
    current_key_ = top.key;
    current_value_ = children_[top.child_idx]->Value();
    valid_ = true;
}

void MergingIterator::SeekToFirst() {
    while (!min_heap_.empty()) {
        min_heap_.pop();
    }
    for (size_t i = 0; i < children_.size(); ++i) {
        if (children_[i]) {
            children_[i]->SeekToFirst();
            if (children_[i]->Valid()) {
                min_heap_.push(HeapItem{children_[i]->Key(), i});
            }
        }
    }
    UpdateCurrent();
}

void MergingIterator::Seek(const InternalKey& target) {
    while (!min_heap_.empty()) {
        min_heap_.pop();
    }
    for (size_t i = 0; i < children_.size(); ++i) {
        if (children_[i]) {
            children_[i]->Seek(target);
            if (children_[i]->Valid()) {
                min_heap_.push(HeapItem{children_[i]->Key(), i});
            }
        }
    }
    UpdateCurrent();
}

void MergingIterator::Next() {
    if (min_heap_.empty()) {
        valid_ = false;
        return;
    }
    const size_t child_idx = min_heap_.top().child_idx;
    min_heap_.pop();
    children_[child_idx]->Next();
    if (children_[child_idx]->Valid()) {
        min_heap_.push(HeapItem{children_[child_idx]->Key(), child_idx});
    }
    UpdateCurrent();
}

// ===================== LevelManager =====================

LevelManager::LevelManager(std::string db_dir,
                           size_t block_cache_capacity,
                           size_t max_levels,
                           size_t bits_per_key,
                           CompactionOptions options)
    : db_dir_(std::move(db_dir)),
      manifest_path_((std::filesystem::path(db_dir_) / "MANIFEST").string()),
      max_levels_(max_levels),
      bits_per_key_(bits_per_key),
      options_(std::move(options)),
      block_cache_(std::make_shared<BlockCache>(block_cache_capacity)) {
    options_.bits_per_key = bits_per_key_;
    levels_.resize(max_levels_);
    (void)Recover();
    StartBackgroundThread();
}

LevelManager::~LevelManager() {
    StopBackgroundThread();
}

Status LevelManager::Recover() {
    std::filesystem::create_directories(db_dir_);
    if (!std::filesystem::exists(manifest_path_)) {
        return Status::Ok();
    }

    std::ifstream file(manifest_path_);
    if (!file.is_open()) {
        return Status::IOError("Failed to open MANIFEST for reading: " + manifest_path_);
    }

    std::string line;
    std::map<size_t, std::map<uint64_t, std::shared_ptr<TableMeta>>> active_files;

    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string op;
        ss >> op;
        if (op == "ADD_FILE") {
            size_t level = 0;
            uint64_t file_id = 0;
            std::string min_hex;
            std::string max_hex;
            uint64_t max_version = 0;
            uint64_t file_size = 0;
            if (ss >> level >> file_id >> min_hex >> max_hex >> max_version >> file_size) {
                auto meta = std::make_shared<TableMeta>();
                meta->level = level;
                meta->file_id = file_id;
                meta->min_key = DecodeKeyHex(min_hex);
                meta->max_key = DecodeKeyHex(max_hex);
                meta->max_version = max_version;
                meta->file_size = file_size;
                meta->file_path = (std::filesystem::path(db_dir_) / ("sst_" + std::to_string(file_id) + ".sst")).string();
                active_files[level][file_id] = meta;
                next_file_id_ = std::max(next_file_id_, file_id + 1);
            }
        } else if (op == "DELETE_FILE" || op == "DEL_FILE") {
            size_t level = 0;
            uint64_t file_id = 0;
            if (ss >> level >> file_id) {
                active_files[level].erase(file_id);
            }
        }
    }

    std::unique_lock lock(mutex_);
    levels_.clear();
    levels_.resize(max_levels_);

    for (auto& [level, file_map] : active_files) {
        if (level >= max_levels_) continue;
        for (auto& [file_id, meta] : file_map) {
            std::unique_ptr<SSTableReader> reader;
            Status s = SSTableReader::Open(meta->file_path, &reader, meta->file_id, block_cache_.get());
            if (!s.ok()) {
                return Status::Corruption("Failed to open recovered SSTable: " + meta->file_path + " (" + s.message + ")");
            }
            meta->reader = std::shared_ptr<SSTableReader>(std::move(reader));
            levels_[level].push_back(meta);
        }
    }

    // Sort L0 descending by file_id (newest first)
    if (!levels_.empty()) {
        std::sort(levels_[0].begin(), levels_[0].end(),
                  [](const std::shared_ptr<TableMeta>& a, const std::shared_ptr<TableMeta>& b) {
                      return a->file_id > b->file_id;
                  });
    }

    // Sort L1..Ln ascending by min_key
    for (size_t lvl = 1; lvl < levels_.size(); ++lvl) {
        std::sort(levels_[lvl].begin(), levels_[lvl].end(),
                  [](const std::shared_ptr<TableMeta>& a, const std::shared_ptr<TableMeta>& b) {
                      return a->min_key.user_key < b->min_key.user_key;
                  });
    }

    return Status::Ok();
}

Status LevelManager::RecordAddFile(size_t level, const std::shared_ptr<TableMeta>& meta) {
    std::lock_guard lock(manifest_mutex_);
    int fd = open(manifest_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        return Status::IOError("Failed to open MANIFEST for writing: " + std::string(strerror(errno)));
    }

    std::string min_hex = EncodeKeyHex(meta->min_key);
    std::string max_hex = EncodeKeyHex(meta->max_key);
    std::string line = "ADD_FILE " + std::to_string(level) + " " +
                       std::to_string(meta->file_id) + " " +
                       min_hex + " " + max_hex + " " +
                       std::to_string(meta->max_version) + " " +
                       std::to_string(meta->file_size) + "\n";

    Status s = WriteBuffer(fd, line.data(), line.size());
    if (!s.ok()) {
        close(fd);
        return s;
    }

    if (fsync(fd) != 0) {
        close(fd);
        return Status::IOError("Failed to fsync MANIFEST: " + std::string(strerror(errno)));
    }

    if (close(fd) != 0) {
        return Status::IOError("Failed to close MANIFEST: " + std::string(strerror(errno)));
    }
    return Status::Ok();
}

Status LevelManager::RecordDeleteFile(size_t level, uint64_t file_id) {
    std::lock_guard lock(manifest_mutex_);
    int fd = open(manifest_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        return Status::IOError("Failed to open MANIFEST for writing: " + std::string(strerror(errno)));
    }

    std::string line = "DELETE_FILE " + std::to_string(level) + " " + std::to_string(file_id) + "\n";

    Status s = WriteBuffer(fd, line.data(), line.size());
    if (!s.ok()) {
        close(fd);
        return s;
    }

    if (fsync(fd) != 0) {
        close(fd);
        return Status::IOError("Failed to fsync MANIFEST: " + std::string(strerror(errno)));
    }

    if (close(fd) != 0) {
        return Status::IOError("Failed to close MANIFEST: " + std::string(strerror(errno)));
    }
    return Status::Ok();
}

Timestamp LevelManager::MaxVersion() const {
    std::shared_lock lock(mutex_);
    Timestamp max_v = 0;
    for (const auto& level : levels_) {
        for (const auto& meta : level) {
            max_v = std::max(max_v, meta->max_version);
        }
    }
    return max_v;
}

Status LevelManager::Flush(Iterator& iter) {
    iter.SeekToFirst();
    if (!iter.Valid()) {
        return Status::Ok();
    }

    uint64_t file_id = AllocateFileId();

    const std::string sst_path = (std::filesystem::path(db_dir_) / ("sst_" + std::to_string(file_id) + ".sst")).string();
    SSTableBuilder builder(sst_path, bits_per_key_);

    for (; iter.Valid(); iter.Next()) {
        Status s = builder.Add(iter.Key(), iter.Value());
        if (!s.ok()) {
            return s;
        }
    }

    Status finish_status = builder.Finish();
    if (!finish_status.ok()) {
        return finish_status;
    }

    std::unique_ptr<SSTableReader> reader;
    Status open_status = SSTableReader::Open(sst_path, &reader, file_id, block_cache_.get());
    if (!open_status.ok()) {
        return open_status;
    }

    auto meta = std::make_shared<TableMeta>();
    meta->file_id = file_id;
    meta->level = 0;
    meta->file_path = sst_path;
    meta->file_size = builder.file_size();
    meta->min_key = builder.min_key();
    meta->max_key = builder.max_key();
    meta->max_version = builder.MaxVersion();
    meta->reader = std::shared_ptr<SSTableReader>(std::move(reader));

    Status manifest_status = RecordAddFile(0, meta);
    if (!manifest_status.ok()) {
        return manifest_status;
    }

    {
        std::unique_lock lock(mutex_);
        if (levels_.empty()) {
            levels_.resize(max_levels_);
        }
        // Insert at the front of L0 (newest first)
        levels_[0].insert(levels_[0].begin(), meta);
    }

    {
        std::lock_guard bg_lock(bg_mutex_);
        bg_cv_.notify_one();
    }

    return Status::Ok();
}

Status LevelManager::GetAsOf(const std::string& key, Timestamp timestamp,
                             InternalKey* out_key, std::string* out_value) const {
    if (!out_key || !out_value) {
        return {Status::Code::kInvalidArgument, "Output pointers cannot be null"};
    }

    std::shared_lock lock(mutex_);
    bool found = false;
    InternalKey best_key;
    std::string best_value;

    // 1. Search L0 (from newest to oldest)
    if (!levels_.empty()) {
        for (const auto& meta : levels_[0]) {
            if (!meta || !meta->reader) continue;
            if (found && meta->max_version <= best_key.version) continue;
            if (!meta->reader->MayContain(key)) continue;

            InternalKey cand_key;
            std::string cand_value;
            Status s = meta->reader->GetAsOf(key, timestamp, &cand_key, &cand_value);
            if (s.ok()) {
                if (!found || cand_key.version > best_key.version) {
                    found = true;
                    best_key = cand_key;
                    best_value = std::move(cand_value);
                }
            }
        }
    }

    // 2. Search L1 through Ln
    for (size_t lvl = 1; lvl < levels_.size(); ++lvl) {
        const auto& level_files = levels_[lvl];
        if (level_files.empty()) continue;

        // Binary search the non-overlapping key ranges
        auto it = std::upper_bound(
            level_files.begin(), level_files.end(), key,
            [](const std::string& k, const std::shared_ptr<TableMeta>& m) {
                return k < m->min_key.user_key;
            }
        );

        if (it != level_files.begin()) {
            const auto& cand_meta = *(it - 1);
            if (key <= cand_meta->max_key.user_key) {
                if (!found || cand_meta->max_version > best_key.version) {
                    if (cand_meta->reader && cand_meta->reader->MayContain(key)) {
                        InternalKey cand_key;
                        std::string cand_value;
                        Status s = cand_meta->reader->GetAsOf(key, timestamp, &cand_key, &cand_value);
                        if (s.ok()) {
                            if (!found || cand_key.version > best_key.version) {
                                found = true;
                                best_key = cand_key;
                                best_value = std::move(cand_value);
                            }
                        }
                    }
                }
            }
        }
    }

    if (!found) {
        return Status::NotFound();
    }

    *out_key = best_key;
    *out_value = std::move(best_value);
    return Status::Ok();
}

std::unique_ptr<Iterator> LevelManager::NewIterator() const {
    std::shared_lock lock(mutex_);
    std::vector<std::unique_ptr<Iterator>> children;
    std::vector<std::shared_ptr<TableMeta>> metas;

    for (const auto& level : levels_) {
        for (const auto& meta : level) {
            if (meta && meta->reader) {
                children.push_back(meta->reader->NewIterator());
                metas.push_back(meta);
            }
        }
    }

    return std::make_unique<MergingIterator>(std::move(children), std::move(metas));
}

size_t LevelManager::NumFiles(size_t level) const {
    std::shared_lock lock(mutex_);
    if (level < levels_.size()) {
        return levels_[level].size();
    }
    return 0;
}

std::vector<std::shared_ptr<TableMeta>> LevelManager::GetLevelFiles(size_t level) const {
    std::shared_lock lock(mutex_);
    if (level < levels_.size()) {
        return levels_[level];
    }
    return {};
}

Status LevelManager::AddFile(size_t level, std::shared_ptr<TableMeta> meta) {
    if (!meta) {
        return {Status::Code::kInvalidArgument, "TableMeta cannot be null"};
    }
    if (level >= max_levels_) {
        return {Status::Code::kInvalidArgument, "Level exceeds max_levels"};
    }

    meta->level = level;
    Status s = RecordAddFile(level, meta);
    if (!s.ok()) {
        return s;
    }

    std::unique_lock lock(mutex_);
    if (levels_.size() <= level) {
        levels_.resize(max_levels_);
    }
    if (level == 0) {
        levels_[0].insert(levels_[0].begin(), meta);
    } else {
        levels_[level].push_back(meta);
        std::sort(levels_[level].begin(), levels_[level].end(),
                  [](const std::shared_ptr<TableMeta>& a, const std::shared_ptr<TableMeta>& b) {
                      return a->min_key.user_key < b->min_key.user_key;
                  });
    }

    next_file_id_ = std::max(next_file_id_, meta->file_id + 1);
    return Status::Ok();
}

Status LevelManager::DeleteFile(size_t level, uint64_t file_id) {
    if (level >= max_levels_) {
        return {Status::Code::kInvalidArgument, "Level exceeds max_levels"};
    }

    Status s = RecordDeleteFile(level, file_id);
    if (!s.ok()) {
        return s;
    }

    std::unique_lock lock(mutex_);
    if (level < levels_.size()) {
        auto& files = levels_[level];
        for (auto it = files.begin(); it != files.end(); ++it) {
            if ((*it)->file_id == file_id) {
                files.erase(it);
                break;
            }
        }
    }

    if (block_cache_) {
        block_cache_->EvictFile(file_id);
    }

    return Status::Ok();
}

uint64_t LevelManager::TotalLevelSize(size_t level) const {
    std::shared_lock lock(mutex_);
    if (level >= levels_.size()) return 0;
    uint64_t total = 0;
    for (const auto& meta : levels_[level]) {
        if (meta) total += meta->file_size;
    }
    return total;
}

uint64_t LevelManager::AllocateFileId() {
    std::unique_lock lock(mutex_);
    return next_file_id_++;
}

void LevelManager::StartBackgroundThread() {
    bg_stopped_ = false;
    bg_thread_ = std::thread(&LevelManager::BackgroundWorker, this);
}

void LevelManager::StopBackgroundThread() {
    {
        std::unique_lock lock(bg_mutex_);
        if (bg_stopped_) return;
        bg_stopped_ = true;
        bg_cv_.notify_all();
    }
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
}

void LevelManager::BackgroundWorker() {
    while (!bg_stopped_.load()) {
        std::unique_lock lock(bg_mutex_);
        bg_cv_.wait(lock, [this] {
            return bg_stopped_.load() || NeedsCompaction();
        });
        if (bg_stopped_.load()) {
            break;
        }

        bg_compaction_running_ = true;
        lock.unlock();

        {
            std::unique_lock comp_lock(compaction_mutex_);
            while (!bg_stopped_.load()) {
                auto job = Compactor::PickCompaction(*this, options_);
                if (!job) break;
                Status s = Compactor::RunCompaction(*this, options_, *job);
                if (!s.ok()) break;
            }
        }

        lock.lock();
        bg_compaction_running_ = false;
        bg_compaction_done_cv_.notify_all();
    }
}

void LevelManager::WaitForCompaction() {
    std::unique_lock lock(bg_mutex_);
    bg_compaction_done_cv_.wait(lock, [this] {
        return !bg_compaction_running_.load() && !NeedsCompaction();
    });
}

bool LevelManager::NeedsCompaction() const {
    std::shared_lock lock(mutex_);
    if (levels_.empty()) return false;
    if (levels_[0].size() >= options_.l0_trigger) {
        return true;
    }
    for (size_t lvl = 1; lvl < levels_.size() - 1; ++lvl) {
        if (lvl < options_.level_size_budget.size() && options_.level_size_budget[lvl] > 0) {
            uint64_t sz = 0;
            for (const auto& f : levels_[lvl]) {
                if (f) sz += f->file_size;
            }
            if (sz >= options_.level_size_budget[lvl]) {
                return true;
            }
        }
    }
    return false;
}

Status LevelManager::CompactNow(Timestamp current_time) {
    std::unique_lock comp_lock(compaction_mutex_);
    auto job = Compactor::PickCompaction(*this, options_, current_time);
    if (!job) {
        return Status::Ok();
    }
    return Compactor::RunCompaction(*this, options_, *job);
}

Status LevelManager::CompactLevel(size_t level, Timestamp current_time) {
    std::unique_lock comp_lock(compaction_mutex_);
    auto job = Compactor::PickCompactionForLevel(*this, level, options_, current_time);
    if (!job) {
        return Status::Ok();
    }
    return Compactor::RunCompaction(*this, options_, *job);
}

Status LevelManager::ApplyCompactionEdit(
    size_t from_level, size_t to_level,
    const std::vector<std::shared_ptr<TableMeta>>& inputs_from,
    const std::vector<std::shared_ptr<TableMeta>>& inputs_to,
    const std::vector<std::shared_ptr<TableMeta>>& outputs) {

    std::string manifest_records;
    for (const auto& meta : outputs) {
        manifest_records += "ADD_FILE " + std::to_string(to_level) + " " +
                            std::to_string(meta->file_id) + " " +
                            EncodeKeyHex(meta->min_key) + " " +
                            EncodeKeyHex(meta->max_key) + " " +
                            std::to_string(meta->max_version) + " " +
                            std::to_string(meta->file_size) + "\n";
    }
    for (const auto& meta : inputs_from) {
        manifest_records += "DELETE_FILE " + std::to_string(from_level) + " " +
                            std::to_string(meta->file_id) + "\n";
    }
    for (const auto& meta : inputs_to) {
        manifest_records += "DELETE_FILE " + std::to_string(to_level) + " " +
                            std::to_string(meta->file_id) + "\n";
    }

    {
        std::unique_lock lock(mutex_);
        std::lock_guard m_lock(manifest_mutex_);

        if (!manifest_records.empty()) {
            int fd = open(manifest_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd < 0) {
                return Status::IOError("Failed to open MANIFEST for writing: " + std::string(strerror(errno)));
            }
            Status s = WriteBuffer(fd, manifest_records.data(), manifest_records.size());
            if (!s.ok()) {
                close(fd);
                return s;
            }
            if (fsync(fd) != 0) {
                close(fd);
                return Status::IOError("Failed to fsync MANIFEST: " + std::string(strerror(errno)));
            }
            if (close(fd) != 0) {
                return Status::IOError("Failed to close MANIFEST: " + std::string(strerror(errno)));
            }
        }

        // Update in-memory levels_
        if (from_level < levels_.size()) {
            auto& files = levels_[from_level];
            for (const auto& del : inputs_from) {
                auto it = std::remove_if(files.begin(), files.end(),
                                         [&](const auto& f) { return f->file_id == del->file_id; });
                files.erase(it, files.end());
            }
        }
        if (to_level < levels_.size()) {
            auto& files = levels_[to_level];
            for (const auto& del : inputs_to) {
                auto it = std::remove_if(files.begin(), files.end(),
                                         [&](const auto& f) { return f->file_id == del->file_id; });
                files.erase(it, files.end());
            }
            for (const auto& out : outputs) {
                files.push_back(out);
            }
            if (to_level == 0) {
                std::sort(files.begin(), files.end(),
                          [](const auto& a, const auto& b) { return a->file_id > b->file_id; });
            } else {
                std::sort(files.begin(), files.end(),
                          [](const auto& a, const auto& b) { return a->min_key.user_key < b->min_key.user_key; });
            }
        }
    }

    auto delete_input_file = [&](const std::shared_ptr<TableMeta>& meta) {
        if (!meta) return;
        if (block_cache_) {
            block_cache_->EvictFile(meta->file_id);
        }
        std::error_code ec;
        std::filesystem::remove(meta->file_path, ec);
    };

    for (const auto& meta : inputs_from) {
        delete_input_file(meta);
    }
    for (const auto& meta : inputs_to) {
        delete_input_file(meta);
    }

    return Status::Ok();
}

} // namespace kvstore
