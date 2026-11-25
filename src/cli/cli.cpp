#include "kvstore/cli.hpp"

#include "kvstore/db_impl.hpp"
#include "storage/level_manager.hpp"

#include <charconv>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace kvstore {
namespace {

Status InvalidArgument(std::string message) {
    return {Status::Code::kInvalidArgument, std::move(message)};
}

Status ParsePositiveSize(std::string_view text, const char* option, std::size_t* value) {
    if (text.empty()) {
        return InvalidArgument(std::string(option) + " requires a positive integer");
    }
    std::size_t parsed_value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), parsed_value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || parsed_value == 0) {
        return InvalidArgument(std::string(option) + " requires a positive integer");
    }
    *value = parsed_value;
    return Status::Ok();
}

Status ParseTimestamp(std::string_view text, Timestamp* value) {
    if (text.empty()) {
        return InvalidArgument("timestamp must be a non-negative integer");
    }
    Timestamp parsed_value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), parsed_value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
        return InvalidArgument("timestamp must be a non-negative integer");
    }
    *value = parsed_value;
    return Status::Ok();
}

bool Tokenize(const std::string& line, std::vector<std::string>* tokens, std::string* error) {
    tokens->clear();
    std::string token;
    bool in_quotes = false;
    bool token_started = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (in_quotes) {
            if (ch == '"') {
                in_quotes = false;
                token_started = true;
            } else if (ch == '\\') {
                if (i + 1 == line.size()) {
                    *error = "trailing escape in quoted string";
                    return false;
                }
                const char escaped = line[++i];
                switch (escaped) {
                    case '"': token.push_back('"'); break;
                    case '\\': token.push_back('\\'); break;
                    case 'n': token.push_back('\n'); break;
                    case 't': token.push_back('\t'); break;
                    default:
                        *error = std::string("unsupported escape sequence: \\") + escaped;
                        return false;
                }
                token_started = true;
            } else {
                token.push_back(ch);
                token_started = true;
            }
            continue;
        }

        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (token_started) {
                tokens->push_back(std::move(token));
                token.clear();
                token_started = false;
            }
        } else if (ch == '"') {
            in_quotes = true;
            token_started = true;
        } else {
            token.push_back(ch);
            token_started = true;
        }
    }

    if (in_quotes) {
        *error = "unterminated double quote";
        return false;
    }
    if (token_started) tokens->push_back(std::move(token));
    return true;
}

std::string Lowercase(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

bool HasArgumentCount(const std::vector<std::string>& tokens, std::size_t count,
                      const char* usage, std::ostream& err) {
    if (tokens.size() == count) return true;
    err << "Error: usage: " << usage << '\n';
    return false;
}

void ReportStatus(std::ostream& err, const Status& status) {
    err << "Error: ";
    if (!status.message.empty()) {
        err << status.message;
    } else if (status.is_not_found()) {
        err << "not found";
    } else {
        err << "operation failed";
    }
    err << '\n';
}

std::string OptionalValue(const std::optional<std::string>& value) {
    return value ? *value : "(none)";
}

void PrintCommandHelp(std::ostream& out) {
    out << "Commands:\n"
        << "  put <key> <value>       store a value\n"
        << "  get <key>                read the latest value\n"
        << "  del|delete <key>         delete a key\n"
        << "  scan [start [end]]       scan keys in [start, end)\n"
        << "  getasof <key> <ts>       read a value at a timestamp\n"
        << "  diff <key> <ts1> <ts2>   compare values at two timestamps\n"
        << "  flush                    wait for pending memtable flushes\n"
        << "  compact                  run compaction\n"
        << "  gc [min_retained_ts]     collect vLog segments\n"
        << "  stats                    show storage and scheduler telemetry\n"
        << "  help                     show this help\n"
        << "  quit | exit              leave the CLI\n"
        << "Double-quoted arguments support \\\" and \\\\, plus \\n and \\t escapes.\n";
}

} // namespace

Status ParseCliArgs(int argc, char** argv, CliConfig* config) {
    if (!config) return InvalidArgument("configuration output must not be null");
    if (argc < 0 || (argc > 0 && !argv)) return InvalidArgument("invalid argument vector");

    *config = CliConfig{};
    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) return InvalidArgument("argument must not be null");
        const std::string_view option(argv[i]);
        if (option == "-h" || option == "--help") {
            config->show_help = true;
            continue;
        }
        if (option == "--no-vlog") {
            config->enable_vlog = false;
            continue;
        }

        if (option == "--dir" || option == "--memtable-bytes" ||
            option == "--cache-bytes" || option == "--vlog-threshold" ||
            option == "--vlog-segment-bytes") {
            if (i + 1 >= argc || !argv[i + 1]) {
                return InvalidArgument(std::string(option) + " requires a value");
            }
            const std::string_view value(argv[++i]);
            if (option == "--dir") {
                if (value.empty()) return InvalidArgument("--dir requires a non-empty path");
                config->data_dir.assign(value);
                continue;
            }

            std::size_t parsed_value = 0;
            const Status parse_status = ParsePositiveSize(value, std::string(option).c_str(), &parsed_value);
            if (!parse_status.ok()) return parse_status;
            if (option == "--memtable-bytes") config->memtable_limit = parsed_value;
            else if (option == "--cache-bytes") config->block_cache_bytes = parsed_value;
            else if (option == "--vlog-threshold") config->vlog_threshold = parsed_value;
            else config->vlog_max_segment_size = parsed_value;
            continue;
        }

        return InvalidArgument("unknown option: " + std::string(option));
    }
    return Status::Ok();
}

void PrintCliUsage(std::ostream& out) {
    out << "Usage: kvstore_cli [options]\n"
        << "Options:\n"
        << "  --dir <path>                 data directory (default: ./kvstore_data)\n"
        << "  --memtable-bytes <n>         memtable limit in bytes\n"
        << "  --cache-bytes <n>            block cache capacity in bytes\n"
        << "  --vlog-threshold <n>         separate values at or above n bytes\n"
        << "  --vlog-segment-bytes <n>     maximum vLog segment size\n"
        << "  --no-vlog                    store values inline\n"
        << "  -h, --help                   show this help\n";
}

CliSession::CliSession(CliConfig config) : config_(std::move(config)) {}

CliSession::~CliSession() = default;

Status CliSession::Open() {
    if (db_) return Status::Ok();

    const std::filesystem::path data_path(config_.data_dir);
    const std::filesystem::path sst_path = data_path / "sst";
    std::error_code ec;
    std::filesystem::create_directories(data_path, ec);
    if (ec) return Status::IOError("cannot create data directory: " + ec.message());
    std::filesystem::create_directories(sst_path, ec);
    if (ec) return Status::IOError("cannot create SST directory: " + ec.message());

    level_manager_ = std::make_shared<LevelManager>(sst_path.string(), config_.block_cache_bytes);
    DBOptions options;
    options.wal_path = (data_path / "wal").string();
    options.memtable_limit = config_.memtable_limit;
    options.disk = level_manager_;
    options.vlog_dir = config_.enable_vlog ? (data_path / "vlog").string() : std::string{};
    options.vlog_threshold = config_.vlog_threshold;
    options.vlog_max_segment_size = config_.vlog_max_segment_size;

    db_ = std::make_unique<DBImpl>(std::move(options));
    const Status status = db_->Open();
    if (!status.ok()) {
        db_.reset();
        level_manager_.reset();
    }
    return status;
}

bool CliSession::ExecuteLine(const std::string& line, std::ostream& out, std::ostream& err) {
    const auto first = line.find_first_not_of(" \t\r\n\f\v");
    if (first == std::string::npos || line[first] == '#') return true;

    std::vector<std::string> tokens;
    std::string tokenize_error;
    if (!Tokenize(line, &tokens, &tokenize_error)) {
        err << "Error: " << tokenize_error << '\n';
        return true;
    }
    if (tokens.empty()) return true;

    tokens[0] = Lowercase(std::move(tokens[0]));
    const std::string& command = tokens[0];
    if (command == "quit" || command == "exit") {
        if (!HasArgumentCount(tokens, 1, "quit", err)) return true;
        return false;
    }
    if (command == "help") {
        if (!HasArgumentCount(tokens, 1, "help", err)) return true;
        PrintCommandHelp(out);
        return true;
    }
    if (!db_ || !level_manager_) {
        err << "Error: session is not open\n";
        return true;
    }

    if (command == "put") {
        if (!HasArgumentCount(tokens, 3, "put <key> <value>", err)) return true;
        const Status status = db_->Put(tokens[1], tokens[2]);
        if (!status.ok()) ReportStatus(err, status);
        else out << "OK\n";
        return true;
    }
    if (command == "get") {
        if (!HasArgumentCount(tokens, 2, "get <key>", err)) return true;
        std::string value;
        const Status status = db_->Get(tokens[1], &value);
        if (status.is_not_found()) out << "(not found)\n";
        else if (!status.ok()) ReportStatus(err, status);
        else out << value << '\n';
        return true;
    }
    if (command == "del" || command == "delete") {
        if (!HasArgumentCount(tokens, 2, "del <key>", err)) return true;
        const Status status = db_->Delete(tokens[1]);
        if (!status.ok()) ReportStatus(err, status);
        else out << "OK\n";
        return true;
    }
    if (command == "scan") {
        if (tokens.size() > 3) {
            err << "Error: usage: scan [start_key [end_key]]\n";
            return true;
        }
        const std::string start = tokens.size() >= 2 ? tokens[1] : std::string{};
        const std::string end = tokens.size() >= 3 ? tokens[2] : std::string{};
        std::vector<std::pair<std::string, std::string>> results;
        const Status status = db_->Scan(start, end, &results);
        if (!status.ok()) {
            ReportStatus(err, status);
            return true;
        }
        if (results.empty()) out << "(empty)\n";
        else for (const auto& [key, value] : results) out << key << " = " << value << '\n';
        return true;
    }
    if (command == "getasof") {
        if (!HasArgumentCount(tokens, 3, "getasof <key> <timestamp>", err)) return true;
        Timestamp timestamp = 0;
        const Status parse_status = ParseTimestamp(tokens[2], &timestamp);
        if (!parse_status.ok()) {
            ReportStatus(err, parse_status);
            return true;
        }
        std::string value;
        const Status status = db_->GetAsOf(tokens[1], timestamp, &value);
        if (status.is_not_found()) out << "(not found)\n";
        else if (!status.ok()) ReportStatus(err, status);
        else out << value << '\n';
        return true;
    }
    if (command == "diff") {
        if (!HasArgumentCount(tokens, 4, "diff <key> <ts1> <ts2>", err)) return true;
        Timestamp ts1 = 0;
        Timestamp ts2 = 0;
        Status status = ParseTimestamp(tokens[2], &ts1);
        if (status.ok()) status = ParseTimestamp(tokens[3], &ts2);
        if (!status.ok()) {
            ReportStatus(err, status);
            return true;
        }
        DiffResult result;
        status = db_->Diff(tokens[1], ts1, ts2, &result);
        if (!status.ok()) {
            ReportStatus(err, status);
            return true;
        }
        out << "changed: " << (result.changed ? "true" : "false") << '\n'
            << "old: " << OptionalValue(result.old_value) << '\n'
            << "new: " << OptionalValue(result.new_value) << '\n';
        return true;
    }
    if (command == "flush") {
        if (!HasArgumentCount(tokens, 1, "flush", err)) return true;
        const Status status = db_->WaitForFlush();
        if (!status.ok()) ReportStatus(err, status);
        else out << "OK\n";
        return true;
    }
    if (command == "compact") {
        if (!HasArgumentCount(tokens, 1, "compact", err)) return true;
        const Status status = level_manager_->CompactNow();
        if (!status.ok()) ReportStatus(err, status);
        else {
            level_manager_->WaitForCompaction();
            out << "OK\n";
        }
        return true;
    }
    if (command == "gc") {
        if (tokens.size() > 2) {
            err << "Error: usage: gc [min_retained_ts]\n";
            return true;
        }
        Timestamp min_retained_ts = 0;
        if (tokens.size() == 2) {
            const Status parse_status = ParseTimestamp(tokens[1], &min_retained_ts);
            if (!parse_status.ok()) {
                ReportStatus(err, parse_status);
                return true;
            }
        }
        VLogGCStats stats;
        const Status status = db_->RunVLogGC(min_retained_ts, &stats);
        if (!status.ok()) ReportStatus(err, status);
        else {
            out << "reclaimed_segments=" << stats.segments_reclaimed
                << " relocated=" << stats.records_relocated
                << " dropped=" << stats.records_dropped << '\n';
        }
        return true;
    }
    if (command == "stats") {
        if (!HasArgumentCount(tokens, 1, "stats", err)) return true;
        out << "max_version=" << level_manager_->MaxVersion() << '\n';
        for (std::size_t level = 0; level < level_manager_->max_levels(); ++level) {
            out << "L" << level << " sstables=" << level_manager_->NumFiles(level)
                << " bytes=" << level_manager_->TotalLevelSize(level) << '\n';
        }
        const auto cache = level_manager_->block_cache();
        out << "cache hits=" << cache->Hits() << " misses=" << cache->Misses()
            << " usage_bytes=" << cache->CurrentBytes()
            << " capacity_bytes=" << cache->CapacityBytes() << '\n';

        const LoadMonitor& monitor = level_manager_->GetLoadMonitor();
        std::ostringstream rate;
        rate << std::fixed << std::setprecision(2) << monitor.GetRequestsPerSec();
        out << "load requests_per_sec=" << rate.str()
            << " queue_depth=" << monitor.GetQueueDepth()
            << " p99_us=" << monitor.GetP99LatencyUs() << '\n';

        const CompactionPolicy policy = level_manager_->GetSmartScheduler().EvaluatePolicy(
            level_manager_->NumFiles(0));
        out << "scheduler mode=" << policy.mode
            << " should_run=" << (policy.should_run ? "true" : "false")
            << " max_bytes_per_sec=" << policy.max_bytes_per_sec
            << " debt_override=" << (policy.is_debt_override ? "true" : "false") << '\n';
        out << "vlog enabled=" << (config_.enable_vlog ? "true" : "false")
            << " threshold_bytes=" << config_.vlog_threshold
            << " max_segment_bytes=" << config_.vlog_max_segment_size << '\n';
        return true;
    }

    err << "Error: unknown command: " << tokens[0] << '\n';
    return true;
}

int CliSession::Run(std::istream& in, std::ostream& out, std::ostream& err, bool interactive) {
    std::string line;
    while (true) {
        if (interactive) {
            out << "kv> " << std::flush;
        }
        if (!std::getline(in, line)) break;
        if (!ExecuteLine(line, out, err)) break;
    }
    return 0;
}

} // namespace kvstore
