#include "kvstore/db_impl.hpp"
#include "storage/level_manager.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace kvstore {
namespace {

class CrashFixture {
public:
    CrashFixture()
        : root_(std::filesystem::temp_directory_path() /
                ("kvstore-crash-" + std::to_string(::getpid()) + "-" +
                 std::to_string(++counter_))) {
        std::filesystem::create_directories(root_);
    }
    ~CrashFixture() {
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    std::string WalPath() const { return (root_ / "wal").string(); }
    std::string VLogDir() const { return (root_ / "vlog").string(); }
    std::string SstDir() const { return (root_ / "sst").string(); }

private:
    std::filesystem::path root_;
    static inline int counter_ = 0;
};

DBOptions MakeOptions(const CrashFixture& fixture, std::size_t memtable_limit = 512,
                      std::size_t vlog_threshold = 64,
                      std::size_t vlog_max_segment_size = 1024) {
    DBOptions options;
    options.wal_path = fixture.WalPath();
    options.memtable_limit = memtable_limit;
    options.disk = std::make_shared<LevelManager>(fixture.SstDir(), 1024 * 1024);
    options.vlog_dir = fixture.VLogDir();
    options.vlog_threshold = vlog_threshold;
    options.vlog_max_segment_size = vlog_max_segment_size;
    return options;
}

std::string KeyFor(uint64_t index) {
    std::ostringstream key;
    key << "key_" << std::setw(6) << std::setfill('0') << index;
    return key.str();
}

std::string LargeValue(const std::string& label) {
    return label + ":" + std::string(224, 'L');
}

std::string StreamValue(uint64_t index) {
    if ((index & 1U) == 0) return "small-" + std::to_string(index);
    return LargeValue("stream-" + std::to_string(index));
}

bool WriteBytes(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    std::size_t written = 0;
    while (written < size) {
        const ssize_t count = ::write(fd, bytes + written, size - written);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        written += static_cast<std::size_t>(count);
    }
    return true;
}

bool ReadBytes(int fd, void* data, std::size_t size, bool* clean_eof = nullptr) {
    auto* bytes = static_cast<uint8_t*>(data);
    std::size_t received = 0;
    if (clean_eof) *clean_eof = false;
    while (received < size) {
        const ssize_t count = ::read(fd, bytes + received, size - received);
        if (count < 0 && errno == EINTR) continue;
        if (count == 0) {
            if (clean_eof && received == 0) *clean_eof = true;
            return false;
        }
        if (count < 0) return false;
        received += static_cast<std::size_t>(count);
    }
    return true;
}

bool WriteAck(int fd, uint64_t sequence) {
    return WriteBytes(fd, &sequence, sizeof(sequence));
}

bool ReadAck(int fd, uint64_t* sequence, bool* clean_eof = nullptr) {
    return ReadBytes(fd, sequence, sizeof(*sequence), clean_eof);
}

bool WriteEvent(int fd, char event) { return WriteBytes(fd, &event, sizeof(event)); }

bool ReadEvent(int fd, char* event) { return ReadBytes(fd, event, sizeof(*event)); }

bool SignalKill(pid_t child) {
    const int kill_result = ::kill(child, SIGKILL);
    return kill_result == 0 || errno == ESRCH;
}

bool ReapKilled(pid_t child, int* wait_status) {
    pid_t waited;
    do {
        waited = ::waitpid(child, wait_status, 0);
    } while (waited < 0 && errno == EINTR);
    return waited == child && WIFSIGNALED(*wait_status) &&
           WTERMSIG(*wait_status) == SIGKILL;
}

bool KillAndReap(pid_t child, int* wait_status) {
    return SignalKill(child) && ReapKilled(child, wait_status);
}

void ChildWriteUntilKilled(const CrashFixture& fixture, int ack_fd) {
    auto disk = std::make_shared<LevelManager>(fixture.SstDir(), 1024 * 1024);
    DBOptions options;
    options.wal_path = fixture.WalPath();
    options.memtable_limit = 512;
    options.disk = disk;
    options.vlog_dir = fixture.VLogDir();
    options.vlog_threshold = 64;
    options.vlog_max_segment_size = 1024;
    DBImpl db(options);
    if (!db.Open().ok()) ::_exit(20);
    for (uint64_t index = 0;; ++index) {
        if (!db.Put(KeyFor(index), StreamValue(index)).ok()) ::_exit(21);
        if (!WriteAck(ack_fd, index)) ::_exit(22);
    }
}

bool VerifyStream(const DBOptions& options, uint64_t max_acked, bool allow_one_extra,
                  std::string* error) {
    {
        DBImpl db(options);
        Status status = db.Open();
        if (!status.ok()) {
            *error = "Open failed after crash: " + status.message;
            return false;
        }
        for (uint64_t index = 0; index <= max_acked; ++index) {
            std::string value;
            status = db.Get(KeyFor(index), &value);
            if (!status.ok() || value != StreamValue(index)) {
                *error = "acknowledged key missing or damaged: " + KeyFor(index);
                return false;
            }
        }

        bool has_extra = false;
        std::string extra_value;
        status = db.Get(KeyFor(max_acked + 1), &extra_value);
        if (status.ok()) {
            has_extra = true;
            if (!allow_one_extra || extra_value != StreamValue(max_acked + 1)) {
                *error = "unexpected value after acknowledged prefix";
                return false;
            }
        } else if (!status.is_not_found()) {
            *error = "error reading possible in-flight key: " + status.message;
            return false;
        }
        status = db.Get(KeyFor(max_acked + 2), &extra_value);
        if (!status.is_not_found()) {
            *error = "key beyond possible in-flight operation survived";
            return false;
        }

        std::vector<std::pair<std::string, std::string>> rows;
        status = db.Scan("", "", &rows);
        if (!status.ok()) {
            *error = "Scan failed after crash: " + status.message;
            return false;
        }
        const uint64_t expected_size = max_acked + 1 + (has_extra ? 1 : 0);
        if (rows.size() != expected_size) {
            *error = "Scan returned unexpected number of recovered keys";
            return false;
        }
        for (uint64_t index = 0; index < expected_size; ++index) {
            if (rows[static_cast<std::size_t>(index)].first != KeyFor(index) ||
                rows[static_cast<std::size_t>(index)].second != StreamValue(index)) {
                *error = "Scan contents differ from the recovered prefix";
                return false;
            }
        }
    }
    return true;
}

struct Operation {
    std::string key;
    std::string value;
    bool deletion = false;
};

std::vector<Operation> OperationsForCycle(int cycle) {
    if (cycle == 0) {
        return {{"alpha", "alpha-inline-0", false},
                {"beta", LargeValue("beta-large-0"), false},
                {"gamma", "gamma-inline-0", false},
                {"gone", LargeValue("gone-large-0"), false}};
    }
    if (cycle == 1) {
        return {{"alpha", LargeValue("alpha-large-1"), false},
                {"beta", "beta-inline-1", false},
                {"gamma", "", true},
                {"gone", LargeValue("gone-large-1"), false}};
    }
    return {{"alpha", "alpha-inline-2", false},
            {"beta", "", true},
            {"gamma", LargeValue("gamma-large-2"), false},
            {"gone", "", true}};
}

void ChildWriteBatch(const CrashFixture& fixture, int ack_fd,
                     const std::vector<Operation>& operations, uint64_t base_sequence) {
    auto disk = std::make_shared<LevelManager>(fixture.SstDir(), 1024 * 1024);
    DBOptions options;
    options.wal_path = fixture.WalPath();
    options.memtable_limit = 512;
    options.disk = disk;
    options.vlog_dir = fixture.VLogDir();
    options.vlog_threshold = 64;
    options.vlog_max_segment_size = 1024;
    DBImpl db(options);
    if (!db.Open().ok()) ::_exit(30);
    for (std::size_t i = 0; i < operations.size(); ++i) {
        const Operation& operation = operations[i];
        const Status status = operation.deletion ? db.Delete(operation.key)
                                                 : db.Put(operation.key, operation.value);
        if (!status.ok()) ::_exit(31);
        if (!WriteAck(ack_fd, base_sequence + i)) ::_exit(32);
    }
    for (;;) ::pause();
}

bool VerifyOperationHistory(const CrashFixture& fixture,
                            const std::vector<Operation>& history,
                            std::string* error) {
    DBOptions options = MakeOptions(fixture);
    {
        DBImpl db(options);
        Status status = db.Open();
        if (!status.ok()) {
            *error = "Open failed in repeated crash cycle: " + status.message;
            return false;
        }
        const std::array<std::string, 4> keys = {"alpha", "beta", "gamma", "gone"};
        for (Timestamp timestamp = 0; timestamp <= history.size(); ++timestamp) {
            for (const std::string& key : keys) {
                const Operation* latest = nullptr;
                for (std::size_t i = 0; i < history.size() && i < timestamp; ++i) {
                    if (history[i].key == key) latest = &history[i];
                }
                std::string value;
                status = db.GetAsOf(key, timestamp, &value);
                if (!latest || latest->deletion) {
                    if (!status.is_not_found()) {
                        *error = "time-travel lookup should be not found for " + key;
                        return false;
                    }
                } else if (!status.ok() || value != latest->value) {
                    *error = "time-travel value mismatch for " + key;
                    return false;
                }
            }
        }

        std::map<std::string, std::string> expected;
        for (const Operation& operation : history) {
            if (operation.deletion) expected.erase(operation.key);
            else expected[operation.key] = operation.value;
        }
        for (const std::string& key : keys) {
            std::string value;
            status = db.Get(key, &value);
            const auto found = expected.find(key);
            if (found == expected.end()) {
                if (!status.is_not_found()) {
                    *error = "deleted key was recovered: " + key;
                    return false;
                }
            } else if (!status.ok() || value != found->second) {
                *error = "latest value mismatch for " + key;
                return false;
            }
        }

        std::vector<std::pair<std::string, std::string>> rows;
        status = db.Scan("", "", &rows);
        if (!status.ok() || rows.size() != expected.size()) {
            *error = "Scan failed or returned an unexpected row count";
            return false;
        }
        std::size_t row = 0;
        for (const auto& [key, value] : expected) {
            if (rows[row] != std::pair<std::string, std::string>{key, value}) {
                *error = "Scan returned unexpected data";
                return false;
            }
            ++row;
        }

        Timestamp first_alpha = 0;
        Timestamp latest_alpha = 0;
        for (std::size_t i = 0; i < history.size(); ++i) {
            if (history[i].key == "alpha") {
                if (first_alpha == 0) first_alpha = i + 1;
                latest_alpha = i + 1;
            }
        }
        if (first_alpha != latest_alpha) {
            DiffResult diff;
            status = db.Diff("alpha", first_alpha, latest_alpha, &diff);
            if (!status.ok() || !diff.changed || !diff.old_value || !diff.new_value ||
                *diff.old_value != history[first_alpha - 1].value ||
                *diff.new_value != history[latest_alpha - 1].value) {
                *error = "Diff failed across recovered versions";
                return false;
            }
        }
    }
    return true;
}

} // namespace

TEST(CrashTest, SigkillMidWritePreservesAckedPrefixAcrossWalVlogAndDisk) {
    CrashFixture fixture;
    int pipe_fds[2];
    ASSERT_EQ(::pipe(pipe_fds), 0);
    const pid_t child = ::fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        ::close(pipe_fds[0]);
        ChildWriteUntilKilled(fixture, pipe_fds[1]);
    }
    ::close(pipe_fds[1]);

    bool protocol_ok = true;
    int64_t max_acked = -1;
    while (max_acked < 79) {
        uint64_t sequence = 0;
        if (!ReadAck(pipe_fds[0], &sequence)) {
            protocol_ok = false;
            break;
        }
        if (sequence != static_cast<uint64_t>(max_acked + 1)) protocol_ok = false;
        max_acked = static_cast<int64_t>(sequence);
    }

    const bool kill_sent = SignalKill(child);
    bool clean_eof = false;
    while (true) {
        uint64_t sequence = 0;
        if (!ReadAck(pipe_fds[0], &sequence, &clean_eof)) {
            if (!clean_eof) protocol_ok = false;
            break;
        }
        if (sequence != static_cast<uint64_t>(max_acked + 1)) protocol_ok = false;
        max_acked = static_cast<int64_t>(sequence);
    }
    ::close(pipe_fds[0]);
    int wait_status = 0;
    const bool killed = kill_sent && ReapKilled(child, &wait_status);
    ASSERT_TRUE(protocol_ok);
    ASSERT_GE(max_acked, 79);
    ASSERT_TRUE(killed);

    std::string error;
    {
        DBOptions options = MakeOptions(fixture);
        ASSERT_TRUE(VerifyStream(options, static_cast<uint64_t>(max_acked), true, &error)) << error;
    }
    {
        DBOptions options = MakeOptions(fixture);
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        ASSERT_TRUE(db.Put("recovery_marker", LargeValue("recovered-value")).ok());
        ASSERT_TRUE(db.WaitForFlush().ok());
    }
    {
        DBOptions options = MakeOptions(fixture);
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        std::string value;
        ASSERT_TRUE(db.Get("recovery_marker", &value).ok());
        EXPECT_EQ(value, LargeValue("recovered-value"));
    }
}

TEST(CrashTest, SigkillDuringVLogGCLeavesConsistentRecoverableState) {
    CrashFixture fixture;
    {
        DBOptions options = MakeOptions(fixture);
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        for (uint64_t i = 0; i < 30; ++i) {
            ASSERT_TRUE(db.Put(KeyFor(i), LargeValue("anchor-" + std::to_string(i))).ok());
        }
        for (int generation = 1; generation <= 3; ++generation) {
            for (uint64_t i = 0; i < 20; ++i) {
                ASSERT_TRUE(db.Put(KeyFor(i), LargeValue("seed-overwrite-" +
                                                           std::to_string(generation) + "-" +
                                                           std::to_string(i))).ok());
            }
        }
        for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(db.Delete(KeyFor(i)).ok());
        ASSERT_TRUE(db.WaitForFlush().ok());
    }

    int pipe_fds[2];
    ASSERT_EQ(::pipe(pipe_fds), 0);
    const pid_t child = ::fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        ::close(pipe_fds[0]);
        auto disk = std::make_shared<LevelManager>(fixture.SstDir(), 1024 * 1024);
        DBOptions options;
        options.wal_path = fixture.WalPath();
        options.memtable_limit = 512;
        options.disk = disk;
        options.vlog_dir = fixture.VLogDir();
        options.vlog_threshold = 64;
        options.vlog_max_segment_size = 1024;
        DBImpl db(options);
        if (!db.Open().ok() || !WriteEvent(pipe_fds[1], 'R')) ::_exit(40);
        for (uint64_t cycle = 0;; ++cycle) {
            for (uint64_t i = 5; i < 20; ++i) {
                const std::string value = LargeValue("child-" + std::to_string(cycle) + "-" +
                                                     std::to_string(i));
                if (!db.Put(KeyFor(i), value).ok()) ::_exit(41);
            }
            if (!WriteEvent(pipe_fds[1], 'S')) ::_exit(42);
            if (!db.RunVLogGC().ok()) ::_exit(43);
            if (!WriteEvent(pipe_fds[1], 'D')) ::_exit(44);
        }
    }
    ::close(pipe_fds[1]);
    bool protocol_ok = true;
    char event = 0;
    protocol_ok = ReadEvent(pipe_fds[0], &event) && event == 'R';
    if (protocol_ok) protocol_ok = ReadEvent(pipe_fds[0], &event) && event == 'S';
    if (protocol_ok) protocol_ok = ReadEvent(pipe_fds[0], &event) && event == 'D';
    if (protocol_ok) protocol_ok = ReadEvent(pipe_fds[0], &event) && event == 'S';
    int wait_status = 0;
    const bool killed = KillAndReap(child, &wait_status);
    ::close(pipe_fds[0]);
    ASSERT_TRUE(protocol_ok);
    ASSERT_TRUE(killed);

    DBOptions options = MakeOptions(fixture);
    {
        DBImpl db(options);
        ASSERT_TRUE(db.Open().ok());
        for (uint64_t i = 20; i < 30; ++i) {
            std::string value;
            ASSERT_TRUE(db.Get(KeyFor(i), &value).ok());
            EXPECT_EQ(value, LargeValue("anchor-" + std::to_string(i)));
        }
        for (uint64_t i = 0; i < 5; ++i) {
            std::string value;
            EXPECT_TRUE(db.Get(KeyFor(i), &value).is_not_found());
        }
        for (uint64_t i = 5; i < 20; ++i) {
            std::string value;
            ASSERT_TRUE(db.Get(KeyFor(i), &value).ok());
            EXPECT_TRUE(value.rfind("child-", 0) == 0);
        }
        std::vector<std::pair<std::string, std::string>> rows;
        ASSERT_TRUE(db.Scan("", "", &rows).ok());
        ASSERT_EQ(rows.size(), 25U);
        std::size_t row = 0;
        for (uint64_t i = 5; i < 30; ++i) {
            const auto& entry = rows[row++];
            ASSERT_EQ(entry.first, KeyFor(i));
            if (i >= 20) {
                EXPECT_EQ(entry.second, LargeValue("anchor-" + std::to_string(i)));
            } else {
                bool is_known_overwrite = false;
                for (uint64_t cycle = 0; cycle < 100; ++cycle) {
                    if (entry.second == LargeValue("child-" + std::to_string(cycle) + "-" +
                                                   std::to_string(i))) {
                        is_known_overwrite = true;
                        break;
                    }
                }
                EXPECT_TRUE(is_known_overwrite);
            }
        }
        VLogGCStats stats;
        ASSERT_TRUE(db.RunVLogGC(0, &stats).ok());
    }
}

TEST(CrashTest, RepeatedSigkillCyclesMaintainMonotonicVersionsAndTombstones) {
    CrashFixture fixture;
    std::vector<Operation> history;
    uint64_t base_sequence = 0;
    for (int cycle = 0; cycle < 3; ++cycle) {
        const std::vector<Operation> batch = OperationsForCycle(cycle);
        int pipe_fds[2];
        ASSERT_EQ(::pipe(pipe_fds), 0);
        const pid_t child = ::fork();
        ASSERT_NE(child, -1);
        if (child == 0) {
            ::close(pipe_fds[0]);
            ChildWriteBatch(fixture, pipe_fds[1], batch, base_sequence);
        }
        ::close(pipe_fds[1]);

        bool protocol_ok = true;
        for (std::size_t i = 0; i < batch.size(); ++i) {
            uint64_t sequence = 0;
            if (!ReadAck(pipe_fds[0], &sequence) || sequence != base_sequence + i) {
                protocol_ok = false;
                break;
            }
        }
        int wait_status = 0;
        const bool killed = KillAndReap(child, &wait_status);
        ::close(pipe_fds[0]);
        ASSERT_TRUE(protocol_ok);
        ASSERT_TRUE(killed);

        history.insert(history.end(), batch.begin(), batch.end());
        base_sequence += batch.size();
        std::string error;
        ASSERT_TRUE(VerifyOperationHistory(fixture, history, &error)) << error;
    }
}

} // namespace kvstore
