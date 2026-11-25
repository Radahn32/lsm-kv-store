#include "kvstore/benchmark.hpp"

#include "kvstore/db_impl.hpp"
#include "storage/level_manager.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

namespace kvstore {
namespace {

using Clock = std::chrono::steady_clock;

Status InvalidArgument(std::string message) {
    return {Status::Code::kInvalidArgument, std::move(message)};
}

Status ParseUnsigned(std::string_view text, const std::string& option, uint64_t* value) {
    if (text.empty()) return InvalidArgument(option + " requires an unsigned integer");
    uint64_t parsed_value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), parsed_value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
        return InvalidArgument(option + " requires an unsigned integer");
    }
    *value = parsed_value;
    return Status::Ok();
}

Status ParseSize(std::string_view text, const std::string& option, std::size_t* value) {
    uint64_t parsed_value = 0;
    const Status status = ParseUnsigned(text, option, &parsed_value);
    if (!status.ok()) return status;
    if (parsed_value > static_cast<uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return InvalidArgument(option + " is too large");
    }
    *value = static_cast<std::size_t>(parsed_value);
    return Status::Ok();
}

Status WithContext(const Status& status, const std::string& context) {
    if (status.ok()) return status;
    return {status.code, context + (status.message.empty() ? std::string{} : ": " + status.message)};
}

double ElapsedMilliseconds(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

std::string FormatKey(const std::string& prefix, uint64_t index) {
    std::ostringstream key;
    key << prefix << std::setw(6) << std::setfill('0') << index;
    return key.str();
}

std::string RandomValue(std::size_t size, std::mt19937_64& random) {
    std::string value(size, 'a');
    for (char& ch : value) ch = static_cast<char>('a' + random() % 26U);
    return value;
}

struct WorkloadContext {
    // Declaration order is intentional: db is destroyed before level_manager.
    std::shared_ptr<LevelManager> level_manager;
    std::unique_ptr<DBImpl> db;
    std::filesystem::path directory;
};

Status OpenWorkloadContext(const std::filesystem::path& directory,
                           const BenchmarkConfig& config,
                           bool enable_vlog,
                           std::size_t l0_trigger,
                           WorkloadContext* context,
                           std::shared_ptr<LoadMonitor> monitor = {},
                           std::shared_ptr<SmartScheduler> scheduler = {}) {
    std::error_code ec;
    std::filesystem::remove_all(directory, ec);
    if (ec) return Status::IOError("cannot clear workload directory: " + ec.message());
    std::filesystem::create_directories(directory, ec);
    if (ec) return Status::IOError("cannot create workload directory: " + ec.message());

    context->directory = directory;
    CompactionOptions compaction_options;
    compaction_options.l0_trigger = l0_trigger;
    context->level_manager = std::make_shared<LevelManager>(
        (directory / "sst").string(), config.block_cache_bytes, 7, 10,
        compaction_options, std::move(monitor), std::move(scheduler));

    DBOptions options;
    options.wal_path = (directory / "wal").string();
    options.memtable_limit = config.memtable_limit;
    options.disk = context->level_manager;
    if (enable_vlog) options.vlog_dir = (directory / "vlog").string();
    options.vlog_threshold = config.vlog_threshold;
    options.vlog_max_segment_size = config.vlog_max_segment_size;
    context->db = std::make_unique<DBImpl>(std::move(options));
    const Status status = context->db->Open();
    if (!status.ok()) {
        context->db.reset();
        context->level_manager.reset();
        return WithContext(status, "open workload database");
    }
    return Status::Ok();
}

Status WaitForWrites(WorkloadContext& context) {
    const Status status = context.db->WaitForFlush();
    if (!status.ok()) return WithContext(status, "flush workload memtables");
    context.level_manager->WaitForCompaction();
    return Status::Ok();
}

Status CompactAndWait(WorkloadContext& context) {
    const Status status = context.level_manager->CompactNow();
    if (!status.ok()) return WithContext(status, "compact workload SSTables");
    context.level_manager->WaitForCompaction();
    return Status::Ok();
}

Status DirectoryBytes(const std::filesystem::path& directory, uint64_t* bytes) {
    *bytes = 0;
    std::error_code ec;
    if (!std::filesystem::exists(directory, ec)) {
        if (ec) return Status::IOError("cannot inspect directory: " + ec.message());
        return Status::Ok();
    }
    for (std::filesystem::recursive_directory_iterator item(directory, ec), end;
         !ec && item != end; item.increment(ec)) {
        if (!item->is_regular_file(ec)) {
            if (ec) return Status::IOError("cannot inspect file: " + ec.message());
            continue;
        }
        const uintmax_t size = item->file_size(ec);
        if (ec) return Status::IOError("cannot read file size: " + ec.message());
        if (size > std::numeric_limits<uint64_t>::max() - *bytes) {
            *bytes = std::numeric_limits<uint64_t>::max();
        } else {
            *bytes += static_cast<uint64_t>(size);
        }
    }
    if (ec) return Status::IOError("cannot scan directory: " + ec.message());
    return Status::Ok();
}

Status CaptureStorageMetrics(const WorkloadContext& context,
                             bool enable_vlog,
                             BenchmarkResult* result) {
    uint64_t sst_bytes = 0;
    for (std::size_t level = 0; level < context.level_manager->max_levels(); ++level) {
        const uint64_t level_bytes = context.level_manager->TotalLevelSize(level);
        if (level_bytes > std::numeric_limits<uint64_t>::max() - sst_bytes) {
            sst_bytes = std::numeric_limits<uint64_t>::max();
            break;
        }
        sst_bytes += level_bytes;
    }
    result->sst_bytes = sst_bytes;
    if (enable_vlog) {
        const Status status = DirectoryBytes(context.directory / "vlog", &result->vlog_bytes);
        if (!status.ok()) return status;
    } else {
        result->vlog_bytes = 0;
    }
    const auto cache = context.level_manager->block_cache();
    result->cache_hits = static_cast<uint64_t>(cache->Hits());
    result->cache_misses = static_cast<uint64_t>(cache->Misses());
    return Status::Ok();
}

void ReserveLatencies(std::vector<double>* latencies, uint64_t expected) {
    constexpr uint64_t kReasonableReserve = 1'000'000;
    if (expected <= kReasonableReserve && expected <= latencies->max_size()) {
        latencies->reserve(static_cast<std::size_t>(expected));
    }
}

Status PreloadDataset(WorkloadContext& context,
                      const BenchmarkConfig& config,
                      const std::string& prefix,
                      uint64_t count,
                      std::mt19937_64* random) {
    for (uint64_t i = 0; i < count; ++i) {
        const Status status = context.db->Put(FormatKey(prefix, i),
                                              RandomValue(config.value_bytes, *random));
        if (!status.ok()) return WithContext(status, "preload benchmark dataset");
    }

    // Force a final rotation after the requested keys have been loaded so the
    // complete dataset is resident in SSTables before the read workload begins.
    if (count > 0 && config.memtable_limit > 0) {
        const std::size_t stored_value_bytes =
            config.value_bytes >= config.vlog_threshold ? 16U : config.value_bytes;
        const std::size_t estimated_entry_bytes = stored_value_bytes >
                std::numeric_limits<std::size_t>::max() - 128U
            ? std::numeric_limits<std::size_t>::max()
            : stored_value_bytes + 128U;
        const std::size_t filler_count = config.memtable_limit / estimated_entry_bytes + 1U;
        const uint64_t max_fillers = 100'000;
        const std::size_t bounded_fillers = static_cast<std::size_t>(
            std::min<std::size_t>(filler_count, static_cast<std::size_t>(max_fillers)));
        for (std::size_t i = 0; i < bounded_fillers; ++i) {
            const Status status = context.db->Put(
                FormatKey("zz_fill_", static_cast<uint64_t>(i)),
                RandomValue(config.value_bytes, *random));
            if (!status.ok()) return WithContext(status, "fill preload memtable");
        }
    }
    return WaitForWrites(context);
}

Status RunWriteBenchmark(const BenchmarkConfig& config,
                         const std::filesystem::path& directory,
                         const std::string& name,
                         bool random_keys,
                         BenchmarkResult* result) {
    WorkloadContext context;
    Status status = OpenWorkloadContext(directory, config, true, 4, &context);
    if (!status.ok()) return status;

    std::mt19937_64 random(config.seed);
    std::vector<double> latencies;
    ReserveLatencies(&latencies, config.ops);
    const Clock::time_point start = Clock::now();
    for (uint64_t i = 0; i < config.ops; ++i) {
        const uint64_t key_index = random_keys
            ? random() % std::max<uint64_t>(1, config.ops / 2)
            : i;
        const std::string key = FormatKey(random_keys ? "rand_" : "key_", key_index);
        const std::string value = RandomValue(config.value_bytes, random);
        const Clock::time_point operation_start = Clock::now();
        status = context.db->Put(key, value);
        const Clock::time_point operation_end = Clock::now();
        latencies.push_back(std::chrono::duration<double, std::micro>(
                                operation_end - operation_start).count());
        if (!status.ok()) return WithContext(status, "write benchmark operation");
    }
    status = WaitForWrites(context);
    if (!status.ok()) return status;
    const Clock::time_point end = Clock::now();

    *result = ComputeLatencyStats(name, std::move(latencies), ElapsedMilliseconds(start, end));
    result->notes = random_keys ? "random keys with overwrite traffic" : "sequential keys";
    return CaptureStorageMetrics(context, true, result);
}

Status RunPointReadBenchmark(const BenchmarkConfig& config,
                             const std::filesystem::path& directory,
                             bool expect_hit,
                             BenchmarkResult* result) {
    const std::string name = expect_hit ? "point_read_hit" : "point_read_miss";
    WorkloadContext context;
    Status status = OpenWorkloadContext(directory, config, true, 4, &context);
    if (!status.ok()) return status;
    std::mt19937_64 random(config.seed);
    status = PreloadDataset(context, config, "key_", config.ops, &random);
    if (!status.ok()) return status;

    std::vector<double> latencies;
    ReserveLatencies(&latencies, config.ops);
    const Clock::time_point start = Clock::now();
    for (uint64_t i = 0; i < config.ops; ++i) {
        const uint64_t index = random() % std::max<uint64_t>(1, config.ops);
        const std::string key = expect_hit
            ? FormatKey("key_", index)
            : "absent_" + FormatKey("key_", index);
        std::string value;
        const Clock::time_point operation_start = Clock::now();
        status = context.db->Get(key, &value);
        const Clock::time_point operation_end = Clock::now();
        latencies.push_back(std::chrono::duration<double, std::micro>(
                                operation_end - operation_start).count());
        if (expect_hit && !status.ok()) return WithContext(status, "point read hit lookup");
        if (!expect_hit && !status.is_not_found()) {
            return status.ok() ? Status::Corruption("point read miss unexpectedly found a key")
                               : WithContext(status, "point read miss lookup");
        }
    }
    const Clock::time_point end = Clock::now();
    *result = ComputeLatencyStats(name, std::move(latencies), ElapsedMilliseconds(start, end));
    result->notes = expect_hit ? "random point hits" : "guaranteed absent keys";
    return CaptureStorageMetrics(context, true, result);
}

Status RunRangeScanBenchmark(const BenchmarkConfig& config,
                             const std::filesystem::path& directory,
                             BenchmarkResult* result) {
    WorkloadContext context;
    Status status = OpenWorkloadContext(directory, config, true, 4, &context);
    if (!status.ok()) return status;
    std::mt19937_64 random(config.seed);
    status = PreloadDataset(context, config, "key_", config.ops, &random);
    if (!status.ok()) return status;

    const uint64_t scans = std::max<uint64_t>(20, config.ops / 10);
    std::vector<double> latencies;
    ReserveLatencies(&latencies, scans);
    const Clock::time_point start = Clock::now();
    for (uint64_t i = 0; i < scans; ++i) {
        const uint64_t start_index = config.ops == 0 ? i : random() % config.ops;
        const uint64_t end_index = start_index > std::numeric_limits<uint64_t>::max() - 50
            ? std::numeric_limits<uint64_t>::max()
            : start_index + 50;
        std::vector<std::pair<std::string, std::string>> rows;
        const Clock::time_point operation_start = Clock::now();
        status = context.db->Scan(FormatKey("key_", start_index), FormatKey("key_", end_index), &rows);
        const Clock::time_point operation_end = Clock::now();
        latencies.push_back(std::chrono::duration<double, std::micro>(
                                operation_end - operation_start).count());
        if (!status.ok()) return WithContext(status, "range scan");
    }
    const Clock::time_point end = Clock::now();
    *result = ComputeLatencyStats("range_scan", std::move(latencies), ElapsedMilliseconds(start, end));
    result->notes = "bounded scans over approximately 50 keys";
    return CaptureStorageMetrics(context, true, result);
}

std::size_t LargeKeyPadding(const BenchmarkConfig& config) {
    if (config.large_ops == 0 || config.memtable_limit == 0) return 0;
    const uint64_t limit = static_cast<uint64_t>(config.memtable_limit);
    const uint64_t count = config.large_ops;
    const uint64_t per_operation = limit / count + (limit % count == 0 ? 0 : 1);
    const uint64_t padding = per_operation > limit / 2 ? limit : per_operation * 2;
    return static_cast<std::size_t>(std::min(padding, limit));
}

Status RunLargeValueBenchmark(const BenchmarkConfig& config,
                              const std::filesystem::path& directory,
                              bool enable_vlog,
                              BenchmarkResult* result) {
    const std::string name = enable_vlog ? "large_val_vlog" : "large_val_inline";
    WorkloadContext context;
    Status status = OpenWorkloadContext(directory, config, enable_vlog, 2, &context);
    if (!status.ok()) return status;

    const std::size_t key_padding = LargeKeyPadding(config);
    std::mt19937_64 random(config.seed);
    std::vector<double> latencies;
    ReserveLatencies(&latencies, config.large_ops);
    const Clock::time_point start = Clock::now();
    for (uint64_t i = 0; i < config.large_ops; ++i) {
        std::string key = FormatKey("large_", i / 2);
        key.append(key_padding, 'p');
        const std::string value = RandomValue(config.large_value_bytes, random);
        const Clock::time_point operation_start = Clock::now();
        status = context.db->Put(key, value);
        const Clock::time_point operation_end = Clock::now();
        latencies.push_back(std::chrono::duration<double, std::micro>(
                                operation_end - operation_start).count());
        if (!status.ok()) return WithContext(status, "large value write");
    }
    status = WaitForWrites(context);
    if (!status.ok()) return status;
    status = CompactAndWait(context);
    if (!status.ok()) return status;

    VLogGCStats gc_stats;
    if (enable_vlog) {
        status = context.db->RunVLogGC(0, &gc_stats);
        if (!status.ok()) return WithContext(status, "vLog garbage collection");
        status = WaitForWrites(context);
        if (!status.ok()) return status;
        status = CompactAndWait(context);
        if (!status.ok()) return status;
    }
    const Clock::time_point end = Clock::now();

    *result = ComputeLatencyStats(name, std::move(latencies), ElapsedMilliseconds(start, end));
    std::ostringstream notes;
    notes << "large key padding=" << key_padding;
    if (enable_vlog) {
        notes << "; reclaimed=" << gc_stats.segments_reclaimed
              << " relocated=" << gc_stats.records_relocated
              << " dropped=" << gc_stats.records_dropped;
    } else {
        notes << "; vLog disabled";
    }
    result->notes = notes.str();
    return CaptureStorageMetrics(context, enable_vlog, result);
}

Status RunAdaptiveBurstBenchmark(const BenchmarkConfig& config,
                                 const std::filesystem::path& directory,
                                 BenchmarkResult* result) {
    auto monitor = std::make_shared<LoadMonitor>();
    SchedulerOptions scheduler_options;
    scheduler_options.max_l0_debt_threshold = 1;
    auto scheduler = std::make_shared<SmartScheduler>(monitor, scheduler_options);

    WorkloadContext context;
    Status status = OpenWorkloadContext(directory, config, true, 1, &context,
                                        monitor, scheduler);
    if (!status.ok()) return status;
    const uint64_t dataset_size = std::max<uint64_t>(1, std::min<uint64_t>(config.ops, 64));
    std::mt19937_64 preload_random(config.seed);
    status = PreloadDataset(context, config, "mixed_", dataset_size, &preload_random);
    if (!status.ok()) return status;

    std::vector<double> latencies;
    std::mutex latencies_mutex;
    std::mutex status_mutex;
    Status first_error = Status::Ok();
    std::mutex policy_mutex;
    std::set<std::string> policy_modes;
    bool throttled_seen = false;
    bool paused_seen = false;
    uint64_t minimum_rate = std::numeric_limits<uint64_t>::max();
    const auto record_latency = [&](double latency) {
        std::lock_guard lock(latencies_mutex);
        latencies.push_back(latency);
    };
    const auto record_error = [&](const Status& failure, const char* operation) {
        if (failure.ok()) return;
        std::lock_guard lock(status_mutex);
        if (first_error.ok()) first_error = WithContext(failure, operation);
    };
    const auto observe_policy = [&] {
        const std::size_t l0_count = context.level_manager->NumFiles(0);
        const CompactionPolicy policy = context.level_manager->GetSmartScheduler().EvaluatePolicy(l0_count);
        std::lock_guard lock(policy_mutex);
        policy_modes.insert(policy.mode);
        if (policy.max_bytes_per_sec != std::numeric_limits<uint64_t>::max()) {
            throttled_seen = true;
            minimum_rate = std::min(minimum_rate, policy.max_bytes_per_sec);
        }
        if (!policy.should_run) paused_seen = true;
    };

    std::atomic<unsigned int> ready{0};
    std::atomic<bool> start{false};
    const unsigned int reader_count = 2;
    std::vector<std::thread> workers;
    workers.reserve(reader_count + 1);
    workers.emplace_back([&] {
        std::mt19937_64 random(config.seed);
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (uint64_t i = 0; i < config.ops; ++i) {
            const uint64_t index = random() % dataset_size;
            const std::string key = FormatKey("mixed_", index);
            const std::string value = RandomValue(config.value_bytes, random);
            const Clock::time_point before = Clock::now();
            const Status operation_status = context.db->Put(key, value);
            const Clock::time_point after = Clock::now();
            record_latency(std::chrono::duration<double, std::micro>(after - before).count());
            record_error(operation_status, "adaptive burst write");
            observe_policy();
        }
    });
    for (unsigned int reader = 0; reader < reader_count; ++reader) {
        workers.emplace_back([&, reader] {
            std::mt19937_64 random(config.seed + reader + 1);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            for (uint64_t i = 0; i < config.ops; ++i) {
                const uint64_t index = random() % dataset_size;
                const std::string key = FormatKey("mixed_", index);
                std::string value;
                const Clock::time_point before = Clock::now();
                const Status operation_status = context.db->Get(key, &value);
                const Clock::time_point after = Clock::now();
                record_latency(std::chrono::duration<double, std::micro>(after - before).count());
                record_error(operation_status, "adaptive burst read");
                observe_policy();

                if (i % 10 == 0) {
                    const std::string end_key = FormatKey("mixed_", index + 10);
                    std::vector<std::pair<std::string, std::string>> rows;
                    const Clock::time_point scan_before = Clock::now();
                    const Status scan_status = context.db->Scan(key, end_key, &rows);
                    const Clock::time_point scan_after = Clock::now();
                    record_latency(std::chrono::duration<double, std::micro>(
                                       scan_after - scan_before).count());
                    record_error(scan_status, "adaptive burst scan");
                    observe_policy();
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != reader_count + 1) std::this_thread::yield();
    const Clock::time_point start_time = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const Clock::time_point end_time = Clock::now();

    if (!first_error.ok()) return first_error;
    status = WaitForWrites(context);
    if (!status.ok()) return status;
    observe_policy();

    *result = ComputeLatencyStats("adaptive_burst_mixed", std::move(latencies),
                                  ElapsedMilliseconds(start_time, end_time));
    std::ostringstream notes;
    notes << "policy_modes=";
    bool first_mode = true;
    for (const auto& mode : policy_modes) {
        if (!first_mode) notes << '/';
        notes << mode;
        first_mode = false;
    }
    if (first_mode) notes << "unknown";
    notes << " throttled=" << (throttled_seen ? "true" : "false")
          << " paused=" << (paused_seen ? "true" : "false")
          << " min_bytes_per_sec=";
    if (minimum_rate == std::numeric_limits<uint64_t>::max()) notes << "unthrottled";
    else notes << minimum_rate;
    notes << " l0_files=" << context.level_manager->NumFiles(0)
          << " l1_files=" << context.level_manager->NumFiles(1);
    result->notes = notes.str();
    return CaptureStorageMetrics(context, true, result);
}

void WriteCsvString(std::ostream& out, const std::string& value) {
    out.put('"');
    for (char ch : value) {
        if (ch == '"') out.put('"');
        out.put(ch);
    }
    out.put('"');
}

struct WorkspaceGuard {
    std::filesystem::path path;
    bool cleanup = false;

    ~WorkspaceGuard() {
        if (cleanup) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }
};

Status CreateWorkspace(const BenchmarkConfig& config, WorkspaceGuard* workspace) {
    std::error_code ec;
    if (!config.work_dir.empty()) {
        workspace->path = config.work_dir;
        std::filesystem::create_directories(workspace->path, ec);
        if (ec) return Status::IOError("cannot create benchmark workspace: " + ec.message());
        return Status::Ok();
    }

    const std::filesystem::path temp_dir = std::filesystem::temp_directory_path(ec);
    if (ec) return Status::IOError("cannot find temporary directory: " + ec.message());
    static std::atomic<uint64_t> counter{0};
    const auto timestamp = Clock::now().time_since_epoch().count();
    for (unsigned int attempt = 0; attempt < 1000; ++attempt) {
        const uint64_t serial = counter.fetch_add(1, std::memory_order_relaxed);
        workspace->path = temp_dir /
            ("kvstore_bench_" + std::to_string(::getpid()) + "_" +
             std::to_string(timestamp) + "_" + std::to_string(serial));
        ec.clear();
        const bool created = std::filesystem::create_directory(workspace->path, ec);
        if (created) {
            workspace->cleanup = !config.keep_dir;
            return Status::Ok();
        }
        if (ec && ec != std::errc::file_exists) {
            return Status::IOError("cannot create benchmark workspace: " + ec.message());
        }
    }
    return Status::IOError("cannot allocate a unique benchmark workspace");
}

} // namespace

Status ParseBenchmarkArgs(int argc, char** argv, BenchmarkConfig* config) {
    if (!config) return InvalidArgument("configuration output must not be null");
    if (argc < 0 || (argc > 0 && !argv)) return InvalidArgument("invalid argument vector");
    *config = BenchmarkConfig{};

    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) return InvalidArgument("argument must not be null");
        const std::string_view option(argv[i]);
        if (option == "--help" || option == "-h") {
            config->show_help = true;
            continue;
        }
        if (option == "--keep-dir") {
            config->keep_dir = true;
            continue;
        }

        if (option == "--dir" || option == "--csv" || option == "--filter" ||
            option == "--ops" || option == "--large-ops" || option == "--value-bytes" ||
            option == "--large-value-bytes" || option == "--memtable-bytes" ||
            option == "--cache-bytes" || option == "--vlog-threshold" ||
            option == "--vlog-segment-bytes" || option == "--seed") {
            if (i + 1 >= argc || !argv[i + 1]) {
                return InvalidArgument(std::string(option) + " requires a value");
            }
            const std::string_view value(argv[++i]);
            if (option == "--dir") config->work_dir.assign(value);
            else if (option == "--csv") config->csv_path.assign(value);
            else if (option == "--filter") config->filter.assign(value);
            else if (option == "--ops" || option == "--large-ops" || option == "--seed") {
                uint64_t parsed_value = 0;
                const Status status = ParseUnsigned(value, std::string(option), &parsed_value);
                if (!status.ok()) return status;
                if (option == "--ops") config->ops = parsed_value;
                else if (option == "--large-ops") config->large_ops = parsed_value;
                else config->seed = parsed_value;
            } else {
                std::size_t parsed_value = 0;
                const Status status = ParseSize(value, std::string(option), &parsed_value);
                if (!status.ok()) return status;
                if (option == "--value-bytes") config->value_bytes = parsed_value;
                else if (option == "--large-value-bytes") config->large_value_bytes = parsed_value;
                else if (option == "--memtable-bytes") config->memtable_limit = parsed_value;
                else if (option == "--cache-bytes") config->block_cache_bytes = parsed_value;
                else if (option == "--vlog-threshold") config->vlog_threshold = parsed_value;
                else config->vlog_max_segment_size = parsed_value;
            }
            continue;
        }
        return InvalidArgument("unknown option: " + std::string(option));
    }
    return Status::Ok();
}

void PrintBenchmarkUsage(std::ostream& out) {
    out << "Usage: kvstore_bench [options]\n"
        << "Options:\n"
        << "  --dir <path>                 benchmark workspace directory\n"
        << "  --csv <path>                 write results to CSV\n"
        << "  --filter <substring>         run matching workloads only\n"
        << "  --ops <n>                    operation count (default: 500)\n"
        << "  --large-ops <n>              large-value operation count (default: 150)\n"
        << "  --value-bytes <n>            regular value size (default: 128)\n"
        << "  --large-value-bytes <n>      large value size (default: 4096)\n"
        << "  --memtable-bytes <n>         memtable limit (default: 65536)\n"
        << "  --cache-bytes <n>            block cache capacity\n"
        << "  --vlog-threshold <n>         vLog separation threshold\n"
        << "  --vlog-segment-bytes <n>     maximum vLog segment size\n"
        << "  --seed <n>                   deterministic random seed (default: 42)\n"
        << "  --keep-dir                   preserve an automatically created workspace\n"
        << "  -h, --help                   show this help\n";
}

BenchmarkResult ComputeLatencyStats(std::string name,
                                    std::vector<double> latencies_us,
                                    double elapsed_ms) {
    BenchmarkResult result;
    result.name = std::move(name);
    result.operations = static_cast<uint64_t>(latencies_us.size());
    result.elapsed_ms = std::max(0.0, elapsed_ms);
    if (result.elapsed_ms > 0.0) {
        result.ops_per_sec = static_cast<double>(result.operations) * 1000.0 / result.elapsed_ms;
    }
    if (latencies_us.empty()) return result;

    std::sort(latencies_us.begin(), latencies_us.end());
    const auto percentile = [&](double fraction) {
        const long double raw_rank = std::ceil(static_cast<long double>(fraction) *
                                               static_cast<long double>(latencies_us.size()));
        const std::size_t rank = static_cast<std::size_t>(raw_rank);
        const std::size_t index = std::max<std::size_t>(1, rank) - 1;
        return latencies_us[std::min(index, latencies_us.size() - 1)];
    };
    result.p50_us = percentile(0.50);
    result.p95_us = percentile(0.95);
    result.p99_us = percentile(0.99);
    result.max_us = latencies_us.back();
    return result;
}

void PrintBenchmarkTable(const std::vector<BenchmarkResult>& results, std::ostream& out) {
    out << std::left << std::setw(24) << "Benchmark"
        << std::right << std::setw(9) << "Ops"
        << std::setw(14) << "Elapsed(ms)"
        << std::setw(14) << "Ops/s"
        << std::setw(11) << "p50(us)"
        << std::setw(11) << "p95(us)"
        << std::setw(11) << "p99(us)"
        << std::setw(12) << "SST(KB)"
        << std::setw(12) << "vLog(KB)"
        << std::setw(12) << "CacheHit%"
        << "  " << "Notes\n";
    out << std::string(139, '-') << '\n';
    for (const auto& result : results) {
        const double cache_total = static_cast<double>(result.cache_hits) +
                                   static_cast<double>(result.cache_misses);
        const double hit_percent = cache_total > 0.0
            ? static_cast<double>(result.cache_hits) * 100.0 / cache_total
            : 0.0;
        out << std::left << std::setw(24) << result.name
            << std::right << std::setw(9) << result.operations
            << std::fixed << std::setprecision(2)
            << std::setw(14) << result.elapsed_ms
            << std::setw(14) << result.ops_per_sec
            << std::setw(11) << result.p50_us
            << std::setw(11) << result.p95_us
            << std::setw(11) << result.p99_us
            << std::setw(12) << static_cast<double>(result.sst_bytes) / 1024.0
            << std::setw(12) << static_cast<double>(result.vlog_bytes) / 1024.0
            << std::setw(12) << hit_percent
            << "  " << result.notes << '\n';
    }
}

Status WriteBenchmarkCsv(const std::vector<BenchmarkResult>& results, const std::string& path) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) return Status::IOError("cannot open CSV output: " + path);
    out.imbue(std::locale::classic());
    out << "name,operations,elapsed_ms,ops_per_sec,p50_us,p95_us,p99_us,max_us,"
           "sst_bytes,vlog_bytes,cache_hits,cache_misses,notes\n";
    out << std::setprecision(17);
    for (const auto& result : results) {
        WriteCsvString(out, result.name);
        out << ',' << result.operations << ',' << result.elapsed_ms << ',' << result.ops_per_sec
            << ',' << result.p50_us << ',' << result.p95_us << ',' << result.p99_us
            << ',' << result.max_us << ',' << result.sst_bytes << ',' << result.vlog_bytes
            << ',' << result.cache_hits << ',' << result.cache_misses << ',';
        WriteCsvString(out, result.notes);
        out << '\n';
    }
    out.close();
    if (!out) return Status::IOError("cannot write CSV output: " + path);
    return Status::Ok();
}

Status RunBenchmarks(const BenchmarkConfig& config, std::vector<BenchmarkResult>* results) {
    if (!results) return InvalidArgument("results output must not be null");
    results->clear();

    using Runner = std::function<Status(const BenchmarkConfig&,
                                        const std::filesystem::path&,
                                        BenchmarkResult*)>;
    const std::vector<std::pair<std::string, Runner>> workloads = {
        {"seq_write", [](const auto& cfg, const auto& dir, auto* result) {
            return RunWriteBenchmark(cfg, dir, "seq_write", false, result);
        }},
        {"rand_write", [](const auto& cfg, const auto& dir, auto* result) {
            return RunWriteBenchmark(cfg, dir, "rand_write", true, result);
        }},
        {"point_read_hit", [](const auto& cfg, const auto& dir, auto* result) {
            return RunPointReadBenchmark(cfg, dir, true, result);
        }},
        {"point_read_miss", [](const auto& cfg, const auto& dir, auto* result) {
            return RunPointReadBenchmark(cfg, dir, false, result);
        }},
        {"range_scan", [](const auto& cfg, const auto& dir, auto* result) {
            return RunRangeScanBenchmark(cfg, dir, result);
        }},
        {"large_val_inline", [](const auto& cfg, const auto& dir, auto* result) {
            return RunLargeValueBenchmark(cfg, dir, false, result);
        }},
        {"large_val_vlog", [](const auto& cfg, const auto& dir, auto* result) {
            return RunLargeValueBenchmark(cfg, dir, true, result);
        }},
        {"adaptive_burst_mixed", [](const auto& cfg, const auto& dir, auto* result) {
            return RunAdaptiveBurstBenchmark(cfg, dir, result);
        }}
    };

    std::size_t match_count = 0;
    for (const auto& workload : workloads) {
        if (config.filter.empty() || workload.first.find(config.filter) != std::string::npos) {
            ++match_count;
        }
    }
    if (match_count == 0) return InvalidArgument("benchmark filter matched no workloads: " + config.filter);

    WorkspaceGuard workspace;
    Status status = CreateWorkspace(config, &workspace);
    if (!status.ok()) return status;
    results->reserve(match_count);

    for (const auto& workload : workloads) {
        if (!config.filter.empty() && workload.first.find(config.filter) == std::string::npos) continue;
        BenchmarkResult result;
        const std::filesystem::path directory = workspace.path / workload.first;
        status = workload.second(config, directory, &result);
        if (!status.ok()) return WithContext(status, workload.first);
        if (config.work_dir.empty() && config.keep_dir) {
            result.notes += "; work_dir=" + directory.string();
        }
        results->push_back(std::move(result));
    }
    return Status::Ok();
}

} // namespace kvstore
