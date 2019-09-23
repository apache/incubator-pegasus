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

static constexpr int MAX_ALLOWED_REPEATS = 3;

// Fast path to next file. If next file (_current->index + 1) is invalid,
// we try to list all files and select a new one to start (find_log_file_to_start).
bool load_from_private_log::switch_to_next_log_file()
{
    std::string new_path = fmt::format(
        "{}/log.{}.{}", _private_log->dir(), _current->index() + 1, _current_global_end_offset);

    if (utils::filesystem::file_exists(new_path)) {
        log_file_ptr file;
        error_s es = log_utils::open_read(new_path, file);
        if (!es.is_ok()) {
            derror_replica("{}", es);
            _current = nullptr;
            return false;
        }
        start_from_log_file(file);
        return true;
    } else {
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
    if (dsn_unlikely(log_file_map.empty())) {
        derror_replica("unable to start duplication since no log file is available");
        return;
    }

    for (auto it = log_file_map.begin(); it != log_file_map.end(); it++) {
        auto next_it = std::next(it);
        if (next_it == log_file_map.end()) {
            // use the last file if no file to read
            if (!_current) {
                start_from_log_file(it->second);
            }
            return;
        }
        if (it->second->previous_log_max_decree(get_gpid()) < _start_decree &&
            _start_decree <= next_it->second->previous_log_max_decree(get_gpid())) {
            // `start_decree` is within the range
            start_from_log_file(it->second);
            // find the latest file that matches the condition
        }
    }
}

void load_from_private_log::replay_log_block()
{
    error_s err =
        mutation_log::replay_block(_current,
                                   [this](int log_bytes_length, mutation_ptr &mu) -> bool {
                                       auto es = _mutation_batch.add(std::move(mu));
                                       dassert_replica(es.is_ok(), es.description());
                                       return true;
                                   },
                                   _start_offset,
                                   _current_global_end_offset);
    if (!err.is_ok()) {
        if (err.code() == ERR_HANDLE_EOF && switch_to_next_log_file()) {
            repeat();
            return;
        }

        _err_repeats_num++;
        if (_err_repeats_num > MAX_ALLOWED_REPEATS) {
            derror_replica("loading mutation logs failed for {} times: [err: {}, file: {}, "
                           "start_offset: {}], retry from start",
                           MAX_ALLOWED_REPEATS,
                           err,
                           _current->path(),
                           _start_offset);
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
    _err_repeats_num = 0;
}

} // namespace replication
} // namespace dsn
