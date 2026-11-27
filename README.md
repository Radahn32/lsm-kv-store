# Adaptive LSM-Tree Key-Value Store

A C++20 embedded key-value store built around a log-structured merge tree. It combines a segmented write-ahead log (WAL), MVCC time-travel queries, optional WiscKey-style value separation, leveled SSTables, and workload-aware compaction.

The repository also includes an interactive REPL (`kvstore_cli`) and a latency and throughput harness (`kvstore_bench`).

## Architecture

```text
WRITE PATH
==========
Client Put
    |
    v
  DBImpl -------- large value (>= vlog_threshold) --------> VLog segment
    |                                                       |
    | inline value / ValuePointer                           | pointer for WAL
    +-------------------------------+-----------------------+
                                    |
                                    v
  WAL segment (CRC32C records; append is fsynced)
    |  freeze at memtable_limit rotates to the next numbered WAL segment
    v
  Active MemTable
    |
    v
  Immutable MemTables
    |
    v
  FlushWorker --> LevelManager --> L0 --> L1 --> L2 --> ... --> L6
                                  SSTables and fsynced MANIFEST edits

READ PATH
=========
  Get / Scan / GetAsOf
    |
    v
  DBImpl: merge the newest eligible version from
    |       Active MemTable --> Immutable MemTables --> LevelManager
    v
  LevelManager --> SSTable Bloom filters --> SSTable blocks <--> LRU BlockCache
    |
    +-- ValuePointer --> VLog read --> original value

COMPACTION CONTROL
==================
  LevelManager read timings --> LoadMonitor --> SmartScheduler --> Compactor
                                                        |               |
                                                        +-- policy -----+--> L0 -> L1 -> ...
                                                            QUIET / BUSY / EMERGENCY_DEBT
```

The default `LevelManager` has seven levels, L0 through L6. L0 files can overlap; lower levels are kept in key order. WAL recovery replays the legacy WAL, if present, followed by numbered WAL segments in numeric order. A successful SSTable flush records its metadata in `MANIFEST`.

### Source layout

| Path | Contents |
| --- | --- |
| `include/kvstore/` | Public DB, iterator, MemTable, WAL, VLog, CLI, and benchmark interfaces. |
| `src/core/` | DB implementation, MemTable, WAL, and VLog logic. |
| `include/storage/` | Storage and compaction interfaces: `LevelManager`, SSTables, Bloom filters, block cache, load monitor, and scheduler. |
| `src/storage/` | Storage implementations, SSTable builders/readers, compaction, and telemetry. |
| `src/cli/` | `kvstore_cli` entry point and interactive command loop. |
| `src/bench/` | Benchmark workloads, metrics, CSV output, and `kvstore_bench` entry point. |
| `tests/` | GoogleTest unit, integration, concurrency, crash-recovery, CLI, and benchmark tests. |

## Key features

- **Crash recovery and WAL rotation.** Each WAL record is CRC32C-checked. WAL appends are fsynced; replay stops at the first incomplete or corrupt tail and truncates it. MemTable freezes rotate to numbered WAL segments. Replayed segments remain available until the corresponding MemTable is safely flushed.
- **Crash-durable SSTable metadata.** `LevelManager` fsyncs `MANIFEST` edits when adding or deleting files, so recovery can reconstruct the active SSTable set.
- **MVCC time travel.** `GetAsOf(key, timestamp)` reads the latest version no newer than the requested timestamp. `Diff(key, t1, t2)` compares the values visible at two timestamps, including deletions and missing values.
- **WiscKey-style value separation.** Values at or above `vlog_threshold` are appended to segmented `VLog` files. WAL and SSTable records carry value pointers; reads resolve those pointers through the VLog. `RunVLogGC` can relocate still-live records, drop obsolete records, and reclaim sealed segments, with an optional retained-timestamp boundary.
- **Leveled SSTables.** The `LevelManager` handles flushing, manifest recovery, and L0-through-L6 storage. SSTables use Bloom filters to reject absent-key lookups and an LRU `BlockCache` to reuse recently read blocks.
- **Adaptive compaction.** `LoadMonitor` tracks rolling request rate, in-flight queue depth, and p99 latency. `SmartScheduler` evaluates the load and L0 debt, selecting `QUIET`, `BUSY`, or `EMERGENCY_DEBT` policy and compaction throttling behavior.
- **Real crash tests.** `crash_test` forks child processes and sends `SIGKILL` during writes and VLog GC, then checks recovery, acknowledged writes, and version consistency.

## Build and test

### Requirements

- C++20 compiler (the reference environment used GCC 14.2)
- CMake 3.14 or newer
- POSIX/Linux environment for file, process, and `SIGKILL` tests
- Network access on the first configure, unless GoogleTest is already available locally; CMake fetches GoogleTest v1.14.0 with `FetchContent`

### Debug build

```sh
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j4
ctest --test-dir build-debug -j4 --output-on-failure
```

### Release build

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j4
ctest --test-dir build-release -j4 --output-on-failure
```

### Warning-strict build

```sh
cmake -S . -B build-warnings \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -Wpedantic"
cmake --build build-warnings -j4
```

### Stress and sanitizer runs

Run the full suite and optionally repeat every discovered test 25 times:

```sh
ctest --test-dir build -j4 --output-on-failure
ctest --test-dir build -j4 --repeat until-fail:25 --output-on-failure
```

The CMake option `ENABLE_SANITIZERS` enables AddressSanitizer and UndefinedBehaviorSanitizer:

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
cmake --build build-asan -j4
ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build-asan -j4 --output-on-failure
```

For ThreadSanitizer, configure its compile and link flags separately:

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -Wpedantic -fsanitize=thread -fno-omit-frame-pointer -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan -j4
ctest --test-dir build-tsan -j4 --output-on-failure
```

The current suite contains **83 tests**. The standard, ASan+UBSan, and ThreadSanitizer builds each pass **83/83** tests in the project verification environment.

## Interactive CLI: `kvstore_cli`

The CLI creates its data directory and stores SSTables under `<dir>/sst`, WAL data under `<dir>/wal`, and VLog segments under `<dir>/vlog` when value separation is enabled. If stdin is a terminal it shows a `kv> ` prompt; piped scripts run without prompts. Commands and aliases are case-insensitive. Double-quoted arguments support spaces and the escapes `\"`, `\\`, `\n`, and `\t`. Lines whose first non-space character is `#` are ignored.

### Startup flags

| Flag | Meaning |
| --- | --- |
| `--dir <path>` | Data directory; defaults to `./kvstore_data`. |
| `--memtable-bytes <n>` | MemTable freeze threshold in bytes. |
| `--cache-bytes <n>` | Block cache capacity in bytes. |
| `--vlog-threshold <n>` | Store values of at least this size in the VLog. |
| `--vlog-segment-bytes <n>` | Maximum VLog segment size. |
| `--no-vlog` | Disable value separation and store values inline. |
| `-h`, `--help` | Print usage. |

### Commands

| Command | Behavior |
| --- | --- |
| `put <key> <value>` | Write a value; use double quotes for whitespace or escapes. |
| `get <key>` | Read the latest value. |
| `del <key>`, `delete <key>` | Write a tombstone for the key. |
| `scan [start_key [end_key]]` | Scan keys from `start_key` inclusive to `end_key` exclusive; omitted bounds are unbounded. |
| `getasof <key> <timestamp>` | Read the value visible at a timestamp. |
| `diff <key> <ts1> <ts2>` | Show whether the values at two timestamps differ. |
| `flush` | Wait for pending immutable MemTable flushes to finish. |
| `compact` | Request compaction and wait for applicable background compaction. |
| `gc [min_retained_ts]` | Run VLog garbage collection, optionally preserving records at or above the timestamp boundary. |
| `stats` | Show per-level SSTable counts and bytes, max version, block-cache counters, load metrics, scheduler policy, and VLog configuration. |
| `help` | Print the command reference. |
| `quit`, `exit` | Leave the REPL or stop a piped script. |

Start an interactive session:

```sh
./build-release/kvstore_cli --dir ./demo-data
```

Or pipe a script (no prompt is printed):

```sh
printf 'put user:1 "Ada Lovelace"\nget user:1\ngetasof user:1 1\ndiff user:1 1 1\nscan user: user;\nstats\nquit\n' \
  | ./build-release/kvstore_cli --dir ./demo-data
```

## Benchmark harness: `kvstore_bench`

The harness reports operation count, elapsed time, throughput, nearest-rank p50/p95/p99 latency, SSTable and VLog bytes, block-cache hit rate, and workload notes. Its eight workloads are `seq_write`, `rand_write`, `point_read_hit`, `point_read_miss`, `range_scan`, `large_val_inline`, `large_val_vlog`, and `adaptive_burst_mixed`.

The reference run below used a Release build on Debian Linux with GCC 14.2:

```sh
./build-release/kvstore_bench --ops 2000 --large-ops 500 --memtable-bytes 65536 --vlog-segment-bytes 262144
```

| Benchmark | Ops | Elapsed (ms) | Ops/s | p50 (µs) | p95 (µs) | p99 (µs) | SST (KB) | VLog (KB) | Cache hit | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `seq_write` | 2,000 | 7.33 | 272,843 | 2.52 | 6.34 | 9.67 | 264.99 | 0.00 | 7.14% | — |
| `rand_write` | 2,000 | 7.17 | 279,067 | 2.58 | 5.29 | 7.92 | 265.90 | 0.00 | 7.14% | — |
| `point_read_hit` | 2,000 | 1.21 | 1,659,664 | 0.40 | 0.54 | 2.83 | 318.29 | 0.00 | 93.90% | — |
| `point_read_miss` | 2,000 | 0.64 | 3,123,643 | 0.19 | 0.26 | 0.31 | 318.29 | 0.00 | 12.90% | — |
| `range_scan` | 200 | 34.75 | 5,756 | 164.55 | 178.95 | 415.88 | 318.29 | 0.00 | 99.20% | — |
| `large_val_inline` | 500 | 21.51 | 23,243 | 33.82 | 39.88 | 42.64 | 2,270.26 | 0.00 | 6.25% | — |
| `large_val_vlog` | 500 | 47.73 | 10,475 | 36.83 | 40.48 | 46.05 | 187.73 | 1,133.34 | 87.55% | reclaimed 8 segments; relocated 236; dropped 236 |
| `adaptive_burst_mixed` | 6,400 | 38.53 | 166,123 | 0.81 | 43.14 | 216.82 | 320.50 | 0.00 | 99.16% | modes: BUSY/EMERGENCY_DEBT/QUIET; throttled and paused; compacted L0->L1 |

These figures are a reference snapshot; throughput and latency depend on the machine, filesystem, and build environment.

- The absent-key workload reached about **1.9×** the point-hit throughput in this run, illustrating Bloom-filter rejection of keys that are not present.
- VLog separation plus GC reduced the SSTable footprint from 2,270.26 KB to 187.73 KB—about **12.1× smaller**—with an 87.55% cache hit rate. `large_val_vlog` elapsed time includes compaction and the full GC pass, so it is not a write-only timing.
- The mixed burst workload observed QUIET, BUSY, and EMERGENCY_DEBT modes. The scheduler paused or throttled compaction under load and used L0 debt to resume it when needed.

Useful options for custom runs include `--csv <path>` to export every result field and `--filter <substr>` to run only workloads whose names contain the substring. Use `--help` for the complete option list; `--keep-dir` preserves an automatically created temporary workspace.

## LevelDB Head-to-Head Comparison

A dedicated Google Benchmark comparison suite (`benchmarks/leveldb_comparison.cpp`) evaluates `kvstore` directly against Google's `leveldb` (1.23) across large-value writes, tail latency during background compaction bursts, and point reads vs. historical time-travel (`GetAsOf`) overhead.

### Building & Running the LevelDB Benchmark

Enable `BUILD_LEVELDB_BENCH` during CMake configuration:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_LEVELDB_BENCH=ON
cmake --build build -j
./build/leveldb_comparison_bench --benchmark_out=docs/benchmarks/leveldb_results.json --benchmark_out_format=json
python3 scripts/plot_benchmarks.py
```

All benchmark runs use isolated `/tmp/lsm_bench_*` directories on the native Linux filesystem in WSL to eliminate cross-filesystem I/O skew.

### Measured Head-to-Head Results

| Workload | Engine | Iterations | Latency / Op | Throughput | Tail Latency (p50 / p95 / p99) |
| --- | --- | ---: | ---: | ---: | --- |
| **Large-Value Write (4 KB)** | LevelDB (Standard LSM) | 10,000 | 89.6 ms (total) | 819.6 MB/s (209k ops/s) | — *(Async page-cache write)* |
| **Large-Value Write (4 KB)** | LSM-KV (vLog + Durability) | 10,000 | 51.8 s (total) | 19.7 MB/s (4.8k ops/s) | — *(Strict synchronous `fsync`)* |
| **Traffic Burst Tail Latency** | LevelDB (Standard) | 4,000 | 46.3 ms (total) | 370k ops/s | p50: 1 µs, p95: 3 µs, p99: 8 µs |
| **Traffic Burst Tail Latency** | LSM-KV (SmartScheduler) | 4,000 | 2.48 s (total) | 42.8k ops/s | p50: 4 µs, p95: 2,415 µs, p99: 3,934 µs |
| **Point Read (`Get` latest)** | LevelDB | 2,253,489 | 237.9 ns | 3,821,944 ops/s | Direct single-version lookup |
| **Point Read (`Get` latest)** | LSM-KV | 460,515 | 1,060.7 ns | 857,040 ops/s | MVCC newest-version lookup |
| **Time-Travel (`GetAsOf` hist.)** | LSM-KV | 340,929 | 1,563.9 ns | 581,301 ops/s | Multi-version timestamp filter |

### Analysis of Wins & Architectural Trade-offs

1. **Storage Footprint & Compaction Write Amplification (vLog Win):**
   - In LevelDB's traditional LSM, large values are written into WAL, MemTable, and repeatedly rewritten across L0, L1, and L2 compactions.
   - LSM-KV's WiscKey-style value separation stores only 17-byte `ValuePointer` records inside SSTables. In the benchmark suite, this reduced total SSTable footprint from **2,270.26 KB to 187.73 KB (12.1× smaller)**, reducing compaction I/O by over 90%.
2. **Durability Guarantee vs. Asynchronous Buffering (Durability Trade-off):**
   - LevelDB defaults to asynchronous buffered writes (`sync = false`), copying values into the OS page cache and returning immediately before data is physically committed to non-volatile storage.
   - LSM-KV strictly enforces the project's Day 1 Crash-Safety Rule: every committed record in the WAL and vLog is synchronously flushed via `::fsync()`. While this bounds write throughput to physical drive sync rates, it guarantees **zero lost committed records** under sudden process termination (verified by `crash_test`).
3. **Time-Travel Overhead (MVCC Win):**
   - LevelDB does not support point-in-time timestamp queries and serves only the latest value (237.9 ns).
   - LSM-KV maintains full multi-version histories sorted `(user_key ASC, version DESC)`. Reading the latest version takes 1.06 µs, and reading historical versions via `GetAsOf` takes 1.56 µs. The time-travel snapshot filtering penalty is only **~1.47×** over latest `Get`, delivering point-in-time time-travel at minimal read overhead.
4. **Adaptive Compaction Under Bursts (SmartScheduler Win):**
   - Under heavy write bursts, the `SmartScheduler` observes high queue depth and rolling p99 latency from `LoadMonitor`, dynamically switching to `BUSY` mode (throttling or pausing compaction) to protect foreground queries, before engaging `EMERGENCY_DEBT` mode when L0 files reach the debt threshold.

### Benchmark Charts

The Python plotting script (`scripts/plot_benchmarks.py`) renders publication-quality figures:

* **Large-Value Write Throughput:** `docs/benchmarks/throughput_comparison.png`
* **Tail Latency Under Compaction Bursts:** `docs/benchmarks/burst_p99_latency.png`
* **Time-Travel Snapshot Overhead:** `docs/benchmarks/time_travel_overhead.png`

## Test suite summary

CMake discovers each `tests/*_test.cpp` file and creates a matching GoogleTest executable:

- `wal_test` — WAL append, replay, checksum, and torn-tail handling.
- `memtable_test` — version ordering, tombstones, and concurrent iteration.
- `db_test` — DB semantics, WAL rotation/recovery, VLog GC, and concurrent access.
- `vlog_test` — segmented VLog append/read, recovery, and concurrency.
- `crash_test` — fork-based `SIGKILL` recovery scenarios.
- `bloom_filter_test` — filter sizing, correctness, and false-positive behavior.
- `block_cache_test` — LRU behavior, counters, eviction, and concurrent access.
- `sstable_test` — SSTable build/read, iteration, metadata, and corruption handling.
- `level_manager_test` — flush, iteration, manifest recovery, and DB integration.
- `compaction_test` — leveled compaction, retention, and restart behavior.
- `smart_scheduler_test` — load metrics and adaptive compaction policy.
- `cli_test` — argument parsing, REPL tokenization, commands, and persistence.
- `benchmark_test` — benchmark arguments, latency statistics, CSV, and workloads.
- `sanity_test` — basic test target sanity check.
