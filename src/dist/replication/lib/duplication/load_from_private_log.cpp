// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>

#include "dist/replication/lib/replica_stub.h"
#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/mutation_log_utils.h"
#include "load_from_private_log.h"
#include "replica_duplicator.h"

namespace dsn {
namespace replication {

/*static*/ constexpr int load_from_private_log::MAX_ALLOWED_BLOCK_REPEATS;
/*static*/ constexpr int load_from_private_log::MAX_ALLOWED_FILE_REPEATS;

bool load_from_private_log::will_fail_skip() const
{
    return _err_file_repeats_num >= MAX_ALLOWED_FILE_REPEATS &&
           _duplicator->fail_mode() == duplication_fail_mode::FAIL_SKIP;
}

bool load_from_private_log::will_fail_fast() const
{
    return _err_file_repeats_num >= MAX_ALLOWED_FILE_REPEATS &&
           _duplicator->fail_mode() == duplication_fail_mode::FAIL_FAST;
}

// Fast path to next file. If next file (_current->index + 1) is invalid,
// we try to list all files and select a new one to start (find_log_file_to_start).
bool load_from_private_log::switch_to_next_log_file()
{
    auto file_map = _private_log->get_log_file_map();
    auto next_file_it = file_map.find(_current->index() + 1);
    if (next_file_it != file_map.end()) {
        log_file_ptr file;
        error_s es = log_utils::open_read(next_file_it->second->path(), file);
        if (!es.is_ok()) {
            derror_replica("{}", es);
            _current = nullptr;
            return false;
        }
        start_from_log_file(file);
        return true;
    } else {
        ddebug_f("no next log file (log.{}) is found", _current->index() + 1);
        _current = nullptr;
        return false;
    }
}

void load_from_private_log::run()
{
    dassert_replica(_start_decree != invalid_decree, "{}", _start_decree);
    _duplicator->verify_start_decree(_start_decree);

    if (_current == nullptr) {
        find_log_file_to_start();
        if (_current == nullptr) {
            ddebug_replica("no private log file is currently available");
            repeat(_repeat_delay);
            return;
        }
    }

    replay_log_block();
}

void load_from_private_log::find_log_file_to_start()
{
    // `file_map` has already excluded the useless log files during replica init.
    auto file_map = _private_log->get_log_file_map();

    // Reopen the files. Because the internal file handle of `file_map`
    // is cleared once WAL replay finished. They are unable to read.
    std::map<int, log_file_ptr> new_file_map;
    for (const auto &pr : file_map) {
        log_file_ptr file;
        error_s es = log_utils::open_read(pr.second->path(), file);
        if (!es.is_ok()) {
            derror_replica("{}", es);
            return;
        }
        new_file_map.emplace(pr.first, file);
    }

    find_log_file_to_start(std::move(new_file_map));
}

void load_from_private_log::find_log_file_to_start(std::map<int, log_file_ptr> log_file_map)
{
    _current = nullptr;
    if (dsn_unlikely(log_file_map.empty())) {
        derror_replica("unable to start duplication since no log file is available");
        return;
    }

    for (auto it = log_file_map.begin(); it != log_file_map.end(); it++) {
        auto next_it = std::next(it);
        if (next_it == log_file_map.end()) {
            // use the last file if no file to read
            if (!_current) {
                _current = it->second;
            }
            break;
        }
        if (it->second->previous_log_max_decree(get_gpid()) < _start_decree &&
            _start_decree <= next_it->second->previous_log_max_decree(get_gpid())) {
            // `start_decree` is within the range
            _current = it->second;
            // find the latest file that matches the condition
        }
    }
    start_from_log_file(_current);
}

void load_from_private_log::replay_log_block()
{
    error_s err =
        mutation_log::replay_block(_current,
                                   [this](int log_bytes_length, mutation_ptr &mu) -> bool {
                                       auto es = _mutation_batch.add(std::move(mu));
                                       dassert_replica(es.is_ok(), es.description());
                                       _counter_dup_log_read_bytes_rate->add(log_bytes_length);
                                       _counter_dup_log_read_mutations_rate->increment();
                                       return true;
                                   },
                                   _start_offset,
                                   _current_global_end_offset);
    if (!err.is_ok()) {
        if (err.code() == ERR_HANDLE_EOF && switch_to_next_log_file()) {
            repeat();
            return;
        }

        // Error handling on loading failure:
        // - If block loading failed for `MAX_ALLOWED_REPEATS` times, it restarts reading the file.
        // - If file loading failed for `MAX_ALLOWED_FILE_REPEATS` times, which means it
        //   met some permanent problem (maybe data corruption), there are 2 options for
        //   the next move:
        //   1. skip this file, abandon the data, can be adopted by who allows minor data lost.
        //   2. fail-slow, retry reading this file until human interference.
        _err_block_repeats_num++;
        if (_err_block_repeats_num >= MAX_ALLOWED_BLOCK_REPEATS) {
            derror_replica(
                "loading mutation logs failed for {} times: [err: {}, file: {}, start_offset: {}]",
                _err_block_repeats_num,
                err,
                _current->path(),
                _start_offset);
            _counter_dup_load_file_failed_count->increment();
            _err_file_repeats_num++;
            if (dsn_unlikely(will_fail_skip())) {
                // skip this file
                derror_replica("failed loading for {} times, abandon file {} and try next",
                               _err_file_repeats_num,
                               _current->path());
                _err_file_repeats_num = 0;

                auto prev_offset = _current_global_end_offset;
                if (switch_to_next_log_file()) {
                    // successfully skip to next file
                    auto skipped_bytes = _current_global_end_offset - prev_offset;
                    _counter_dup_load_skipped_bytes_count->add(skipped_bytes);
                    repeat(_repeat_delay);
                    return;
                }
            } else if (dsn_unlikely(will_fail_fast())) {
                dassert_replica(
                    false,
                    "unable to load file {}, fail fast. please check if the file is corrupted",
                    _current->path());
            }
            // retry from file start
            find_log_file_to_start();
        }
        repeat(_repeat_delay);
        return;
    }

    _start_offset = static_cast<size_t>(_current_global_end_offset - _current->start_offset());

    // update last_decree even for empty batch.
    step_down_next_stage(_mutation_batch.last_decree(), _mutation_batch.move_all_mutations());
}

load_from_private_log::load_from_private_log(replica *r, replica_duplicator *dup)
    : replica_base(r),
      _private_log(r->private_log()),
      _duplicator(dup),
      _stub(r->get_replica_stub()),
      _mutation_batch(dup)
{
    _counter_dup_log_read_bytes_rate.init_app_counter("eon.replica_stub",
                                                      "dup.log_read_bytes_rate",
                                                      COUNTER_TYPE_RATE,
                                                      "reading rate of private log in bytes");
    _counter_dup_log_read_mutations_rate.init_app_counter(
        "eon.replica_stub",
        "dup.log_read_mutations_rate",
        COUNTER_TYPE_RATE,
        "reading rate of mutations from private log");
    _counter_dup_load_file_failed_count.init_app_counter(
        "eon.replica_stub",
        "dup.load_file_failed_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "the number of failures loading a private log file during duplication");
    _counter_dup_load_skipped_bytes_count.init_app_counter(
        "eon.replica_stub",
        "dup.load_skipped_bytes_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "bytes of mutations that were skipped because of failure during duplication");
}

void load_from_private_log::set_start_decree(decree start_decree)
{
    _start_decree = start_decree;
    _mutation_batch.set_start_decree(start_decree);
}

void load_from_private_log::start_from_log_file(log_file_ptr f)
{
    ddebug_replica("start loading from log file {}", f->path());

    _current = std::move(f);
    _start_offset = 0;
    _current_global_end_offset = _current->start_offset();
    _err_block_repeats_num = 0;
}

} // namespace replication
} // namespace dsn
