#include "storage/compactor.hpp"
#include "storage/level_manager.hpp"

#include <algorithm>
#include <filesystem>

namespace kvstore {

namespace {

bool RangesOverlap(const std::string& min_a, const std::string& max_a,
                   const std::string& min_b, const std::string& max_b) {
    if (max_a < min_b || min_a > max_b) {
        return false;
    }
    return true;
}

} // namespace

std::unique_ptr<CompactionJob> Compactor::PickCompaction(const LevelManager& lm,
                                                         const CompactionOptions& options,
                                                         Timestamp current_time) {
    // 1. Check L0 file count threshold
    if (lm.NumFiles(0) >= options.l0_trigger) {
        return PickCompactionForLevel(lm, 0, options, current_time);
    }

    // 2. Check size budget for L1 through Ln-1
    for (size_t lvl = 1; lvl < lm.max_levels() - 1; ++lvl) {
        if (lvl < options.level_size_budget.size() && options.level_size_budget[lvl] > 0) {
            if (lm.TotalLevelSize(lvl) >= options.level_size_budget[lvl]) {
                return PickCompactionForLevel(lm, lvl, options, current_time);
            }
        }
    }

    return nullptr;
}

std::unique_ptr<CompactionJob> Compactor::PickCompactionForLevel(const LevelManager& lm,
                                                                size_t level,
                                                                const CompactionOptions& options,
                                                                Timestamp current_time) {
    if (level >= lm.max_levels() - 1) {
        return nullptr;
    }

    auto job = std::make_unique<CompactionJob>();
    job->from_level = level;
    job->to_level = level + 1;

    std::string union_min;
    std::string union_max;

    if (level == 0) {
        job->inputs_from = lm.GetLevelFiles(0);
        if (job->inputs_from.empty()) {
            return nullptr;
        }

        bool first = true;
        for (const auto& meta : job->inputs_from) {
            if (first) {
                union_min = meta->min_key.user_key;
                union_max = meta->max_key.user_key;
                first = false;
            } else {
                union_min = std::min(union_min, meta->min_key.user_key);
                union_max = std::max(union_max, meta->max_key.user_key);
            }
        }

        auto l1_files = lm.GetLevelFiles(1);
        for (const auto& meta : l1_files) {
            if (RangesOverlap(union_min, union_max, meta->min_key.user_key, meta->max_key.user_key)) {
                job->inputs_to.push_back(meta);
                union_min = std::min(union_min, meta->min_key.user_key);
                union_max = std::max(union_max, meta->max_key.user_key);
            }
        }
    } else {
        auto from_files = lm.GetLevelFiles(level);
        if (from_files.empty()) {
            return nullptr;
        }

        auto largest_it = std::max_element(
            from_files.begin(), from_files.end(),
            [](const auto& a, const auto& b) {
                return a->file_size < b->file_size;
            });
        job->inputs_from.push_back(*largest_it);
        union_min = (*largest_it)->min_key.user_key;
        union_max = (*largest_it)->max_key.user_key;

        auto to_files = lm.GetLevelFiles(job->to_level);
        for (const auto& meta : to_files) {
            if (RangesOverlap(union_min, union_max, meta->min_key.user_key, meta->max_key.user_key)) {
                job->inputs_to.push_back(meta);
                union_min = std::min(union_min, meta->min_key.user_key);
                union_max = std::max(union_max, meta->max_key.user_key);
            }
        }
    }

    // Determine if this compaction outputs to the bottom-most level with no older versions below it
    bool is_bottom = true;
    if (job->to_level < lm.max_levels() - 1) {
        for (size_t lvl = job->to_level + 1; lvl < lm.max_levels(); ++lvl) {
            auto deeper_files = lm.GetLevelFiles(lvl);
            for (const auto& meta : deeper_files) {
                if (RangesOverlap(union_min, union_max, meta->min_key.user_key, meta->max_key.user_key)) {
                    is_bottom = false;
                    break;
                }
            }
            if (!is_bottom) break;
        }
    }
    job->is_bottom_level = is_bottom;

    // Calculate retention cutoff
    Timestamp now = (current_time > 0) ? current_time : lm.MaxVersion();
    uint64_t cutoff = 0;
    if (options.drop_all_historical) {
        cutoff = now;
    } else if (now > options.retention_window) {
        cutoff = now - options.retention_window;
    }
    job->retention_cutoff = cutoff;

    return job;
}

Status Compactor::RunCompaction(LevelManager& lm,
                                const CompactionOptions& options,
                                const CompactionJob& job,
                                CompactionStats* stats) {
    std::vector<std::shared_ptr<TableMeta>> all_inputs;
    all_inputs.reserve(job.inputs_from.size() + job.inputs_to.size());
    all_inputs.insert(all_inputs.end(), job.inputs_from.begin(), job.inputs_from.end());
    all_inputs.insert(all_inputs.end(), job.inputs_to.begin(), job.inputs_to.end());

    if (all_inputs.empty()) {
        return Status::Ok();
    }

    std::vector<std::unique_ptr<Iterator>> iterators;
    iterators.reserve(all_inputs.size());
    for (const auto& meta : all_inputs) {
        if (meta && meta->reader) {
            iterators.push_back(meta->reader->NewIterator());
            if (stats) {
                stats->bytes_read += meta->file_size;
            }
        }
    }

    MergingIterator merge_iter(std::move(iterators), all_inputs);

    std::vector<std::shared_ptr<TableMeta>> output_metas;
    std::unique_ptr<SSTableBuilder> current_builder;
    std::string current_output_path;
    uint64_t current_file_id = 0;

    std::string current_user_key;
    bool has_key = false;
    size_t versions_for_current_key = 0;
    bool has_prev_key = false;
    InternalKey prev_internal_key;

    auto finish_current_builder = [&]() -> Status {
        if (!current_builder) {
            return Status::Ok();
        }
        if (current_builder->entry_count() == 0) {
            current_builder.reset();
            std::error_code ec;
            std::filesystem::remove(current_output_path, ec);
            return Status::Ok();
        }

        Status s = current_builder->Finish();
        if (!s.ok()) {
            return s;
        }

        std::unique_ptr<SSTableReader> reader;
        Status r_s = SSTableReader::Open(current_output_path, &reader, current_file_id, lm.block_cache().get());
        if (!r_s.ok()) {
            return r_s;
        }

        auto meta = std::make_shared<TableMeta>();
        meta->file_id = current_file_id;
        meta->level = job.to_level;
        meta->file_path = current_output_path;
        meta->file_size = current_builder->file_size();
        meta->min_key = current_builder->min_key();
        meta->max_key = current_builder->max_key();
        meta->max_version = current_builder->MaxVersion();
        meta->reader = std::shared_ptr<SSTableReader>(std::move(reader));

        output_metas.push_back(meta);
        current_builder.reset();
        return Status::Ok();
    };

    for (merge_iter.SeekToFirst(); merge_iter.Valid(); merge_iter.Next()) {
        InternalKey key = merge_iter.Key();
        std::string_view value = merge_iter.Value();

        if (stats) {
            stats->keys_read++;
        }

        // Deduplicate identical exact internal keys (same user_key, version, type)
        if (has_prev_key && key == prev_internal_key) {
            if (stats) stats->keys_dropped++;
            continue;
        }
        has_prev_key = true;
        prev_internal_key = key;

        if (!has_key || key.user_key != current_user_key) {
            // New user key encountered. If the active builder reached target file size,
            // finalize it now to keep SSTables in Li+1 strictly non-overlapping.
            if (current_builder && current_builder->EstimatedSize() >= options.target_file_size) {
                Status s = finish_current_builder();
                if (!s.ok()) {
                    return s;
                }
            }

            current_user_key = key.user_key;
            has_key = true;
            versions_for_current_key = 0;
        }

        versions_for_current_key++;
        const bool is_newest = (versions_for_current_key == 1);

        bool keep = false;
        if (is_newest) {
            if (key.type == ValueType::kTombstone && job.is_bottom_level && key.version <= job.retention_cutoff) {
                // Obsolete tombstone: deleted key with no older versions below it and past retention cutoff
                keep = false;
            } else {
                keep = true;
            }
        } else {
            // Older historical version of user_key
            if (options.drop_all_historical) {
                keep = false;
            } else if (key.version >= job.retention_cutoff) {
                // Inside retention window: retain for time-travel queries
                keep = true;
            } else {
                // Strictly older than retention cutoff: purge
                keep = false;
            }
        }

        if (!keep) {
            if (stats) {
                stats->keys_dropped++;
            }
            continue;
        }

        if (!current_builder) {
            current_file_id = lm.AllocateFileId();
            current_output_path = (std::filesystem::path(lm.db_dir()) /
                                  ("sst_" + std::to_string(current_file_id) + ".sst")).string();
            current_builder = std::make_unique<SSTableBuilder>(current_output_path, options.bits_per_key);
        }

        Status add_status = current_builder->Add(key, value);
        if (!add_status.ok()) {
            return add_status;
        }

        if (stats) {
            stats->keys_retained++;
            stats->bytes_written += key.user_key.size() + value.size() + sizeof(uint64_t) + 1;
        }
    }

    Status s = finish_current_builder();
    if (!s.ok()) {
        return s;
    }

    return lm.ApplyCompactionEdit(job.from_level, job.to_level, job.inputs_from, job.inputs_to, output_metas);
}

} // namespace kvstore
