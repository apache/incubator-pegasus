// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iterator>
#include <map>
#include <string>
#include <utility>

#include <string_view>
#include "common/duplication_common.h"
#include "duplication_types.h"
#include "load_from_private_log.h"
#include "replica/duplication/mutation_batch.h"
#include "replica/mutation.h"
#include "replica/mutation_log_utils.h"
#include "replica/replica.h"
#include "replica_duplicator.h"
#include "utils/autoref_ptr.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

METRIC_DEFINE_counter(replica,
                      dup_log_file_load_failed_count,
                      dsn::metric_unit::kFileLoads,
                      "The number of times private log files have failed to be loaded during dup");

METRIC_DEFINE_counter(replica,
                      dup_log_file_load_skipped_bytes,
                      dsn::metric_unit::kBytes,
                      "The bytes of mutations that have been skipped due to failed loadings of "
                      "private log files during dup");

METRIC_DEFINE_counter(replica,
                      dup_log_read_bytes,
                      dsn::metric_unit::kBytes,
                      "The size read from private log for dup");

METRIC_DEFINE_counter(replica,
                      dup_log_read_mutations,
                      dsn::metric_unit::kMutations,
                      "The number of mutations read from private log for dup");

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
    const auto &file_map = _private_log->get_log_file_map();
    const auto &next_file_it = file_map.find(_current->index() + 1);
    if (next_file_it != file_map.end()) {
        log_file_ptr file;
        const auto &es = log_utils::open_read(next_file_it->second->path(), file);
        if (!es.is_ok()) {
            LOG_ERROR_PREFIX("{}", es);
            _current = nullptr;
            return false;
        }
        start_from_log_file(file);
        return true;
    } else {
        LOG_INFO("no next log file (log.{}) is found", _current->index() + 1);
        _current = nullptr;
        return false;
    }
}

void load_from_private_log::run()
{
    CHECK_NE_PREFIX(_start_decree, invalid_decree);
    _duplicator->verify_start_decree(_start_decree);

    // last_decree() == invalid_decree is the init status of mutation_buffer when create
    // _mutation_batch, which means the duplication sync hasn't been completed, so need wait sync
    // complete and the  confirmed_decree  != invalid_decree, and then reset mutation_buffer to
    // valid status
    if (_mutation_batch.last_decree() == invalid_decree) {
        if (_duplicator->progress().confirmed_decree == invalid_decree) {
            LOG_WARNING_PREFIX(
                "duplication status hasn't been sync completed, try next for delay 1s, "
                "last_commit_decree={}, "
                "confirmed_decree={}",
                _duplicator->progress().last_decree,
                _duplicator->progress().confirmed_decree);
            repeat(1_s);

            FAIL_POINT_INJECT_NOT_RETURN_F(
                "duplication_sync_complete", [&](std::string_view s) -> void {
                    if (_duplicator->progress().confirmed_decree == invalid_decree) {
                        // set_confirmed_decree(9), the value must be equal (decree_start of
                        // `test_start_duplication` in `load_from_private_log_test.cpp`) -1
                        _duplicator->update_progress(
                            _duplicator->progress().set_confirmed_decree(9));
                    }
                });
            return;
        } else {
            _mutation_batch.reset_mutation_buffer(_duplicator->progress().confirmed_decree);
        }
    }

    if (_current == nullptr) {
        find_log_file_to_start();
        if (_current == nullptr) {
            LOG_INFO_PREFIX("no private log file is currently available");
            repeat(_repeat_delay);
            return;
        }
    }

    replay_log_block();
}

void load_from_private_log::find_log_file_to_start()
{
    _duplicator->set_duplication_plog_checking(true);
    auto cleanup = dsn::defer([this]() { _duplicator->set_duplication_plog_checking(false); });

    // `file_map` has already excluded the useless log files during replica init.
    const auto &file_map = _private_log->get_log_file_map();

    // Reopen the files. Because the internal file handle of `file_map`
    // is cleared once WAL replay finished. They are unable to read.
    mutation_log::log_file_map_by_index new_file_map;
    for (const auto &pr : file_map) {
        log_file_ptr file;
        error_s es = log_utils::open_read(pr.second->path(), file);
        if (!es.is_ok()) {
            LOG_ERROR_PREFIX("{}", es);
            return;
        }
        new_file_map.emplace(pr.first, file);
    }

    find_log_file_to_start(std::move(new_file_map));
}

void load_from_private_log::find_log_file_to_start(
    const mutation_log::log_file_map_by_index &log_file_map)
{
    _current = nullptr;
    if (dsn_unlikely(log_file_map.empty())) {
        LOG_ERROR_PREFIX("unable to start duplication since no log file is available");
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
    error_s err = mutation_log::replay_block(
        _current,
        [this](int log_bytes_length, mutation_ptr &mu) -> bool {
            auto es = _mutation_batch.add(std::move(mu));
            CHECK_PREFIX_MSG(es.is_ok(), es.description());
            METRIC_VAR_INCREMENT_BY(dup_log_read_bytes, log_bytes_length);
            METRIC_VAR_INCREMENT(dup_log_read_mutations);
            return true;
        },
        _start_offset,
        _current_global_end_offset);
    if (!err.is_ok() && err.code() != ERR_HANDLE_EOF) {
        // Error handling on loading failure:
        // - If block loading failed for `MAX_ALLOWED_REPEATS` times, it restarts reading the file.
        // - If file loading failed for `MAX_ALLOWED_FILE_REPEATS` times, which means it
        //   met some permanent problem (maybe data corruption), there are 2 options for
        //   the next move:
        //   1. skip this file, abandon the data, can be adopted by who allows minor data lost.
        //   2. fail-slow, retry reading this file until human interference.
        _err_block_repeats_num++;
        if (_err_block_repeats_num >= MAX_ALLOWED_BLOCK_REPEATS) {
            LOG_ERROR_PREFIX(
                "loading mutation logs failed for {} times: [err: {}, file: {}, start_offset: {}]",
                _err_block_repeats_num,
                err,
                _current->path(),
                _start_offset);
            METRIC_VAR_INCREMENT(dup_log_file_load_failed_count);
            _err_file_repeats_num++;
            if (dsn_unlikely(will_fail_skip())) {
                // skip this file
                LOG_ERROR_PREFIX("failed loading for {} times, abandon file {} and try next",
                                 _err_file_repeats_num,
                                 _current->path());
                _err_file_repeats_num = 0;

                auto prev_offset = _current_global_end_offset;
                if (switch_to_next_log_file()) {
                    // successfully skip to next file
                    auto skipped_bytes = _current_global_end_offset - prev_offset;
                    METRIC_VAR_INCREMENT_BY(dup_log_file_load_skipped_bytes, skipped_bytes);
                    repeat(_repeat_delay);
                    return;
                }
            } else {
                CHECK_PREFIX_MSG(
                    !will_fail_fast(),
                    "unable to load file {}, fail fast. please check if the file is corrupted",
                    _current->path());
            }
            // retry from file start
            find_log_file_to_start();
        }
        repeat(_repeat_delay);
        return;
    }

    if (err.is_ok()) {
        _start_offset = static_cast<size_t>(_current_global_end_offset - _current->start_offset());
        if (_mutation_batch.bytes() < FLAGS_duplicate_log_batch_bytes) {
            repeat();
            return;
        }
    } else if (switch_to_next_log_file()) {
        // !err.is_ok() means that err.code() == ERR_HANDLE_EOF, the current file read completed and
        // try next file
        repeat();
        return;
    }
    // update last_decree even for empty batch.
    // case1: err.is_ok(err.code() != ERR_HANDLE_EOF), but _mutation_batch.bytes() >=
    // FLAGS_duplicate_log_batch_bytes
    // case2: !err.is_ok(err.code() == ERR_HANDLE_EOF) and no next file, need commit the last
    // mutations()
    step_down_next_stage(_mutation_batch.last_decree(), _mutation_batch.move_all_mutations());
}

load_from_private_log::load_from_private_log(replica *r, replica_duplicator *dup)
    : replica_base(r),
      _private_log(r->private_log()),
      _duplicator(dup),
      _stub(r->get_replica_stub()),
      _mutation_batch(dup),
      METRIC_VAR_INIT_replica(dup_log_file_load_failed_count),
      METRIC_VAR_INIT_replica(dup_log_file_load_skipped_bytes),
      METRIC_VAR_INIT_replica(dup_log_read_bytes),
      METRIC_VAR_INIT_replica(dup_log_read_mutations)
{
}

void load_from_private_log::set_start_decree(decree start_decree)
{
    _start_decree = start_decree;
    _mutation_batch.set_start_decree(start_decree);
}

void load_from_private_log::start_from_log_file(log_file_ptr f)
{
    LOG_INFO_PREFIX("start loading from log file {}", f->path());

    _current = std::move(f);
    _start_offset = 0;
    _current_global_end_offset = _current->start_offset();
    _err_block_repeats_num = 0;
}

} // namespace replication
} // namespace dsn
