#pragma once

#include "kvstore/types.hpp"

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>

namespace kvstore {

class DBImpl;
class LevelManager;

struct CliConfig {
    std::string data_dir = "./kvstore_data";
    std::size_t memtable_limit = 4U * 1024U * 1024U;
    std::size_t block_cache_bytes = 16U * 1024U * 1024U;
    std::size_t vlog_threshold = 1024;
    std::size_t vlog_max_segment_size = 16U * 1024U * 1024U;
    bool enable_vlog = true;
    bool show_help = false;
};

Status ParseCliArgs(int argc, char** argv, CliConfig* config);
void PrintCliUsage(std::ostream& out);

class CliSession {
public:
    explicit CliSession(CliConfig config = {});
    ~CliSession();

    CliSession(const CliSession&) = delete;
    CliSession& operator=(const CliSession&) = delete;

    Status Open();
    bool ExecuteLine(const std::string& line, std::ostream& out, std::ostream& err);
    int Run(std::istream& in, std::ostream& out, std::ostream& err, bool interactive);

private:
    CliConfig config_;
    std::shared_ptr<LevelManager> level_manager_;
    std::unique_ptr<DBImpl> db_;
};

} // namespace kvstore
