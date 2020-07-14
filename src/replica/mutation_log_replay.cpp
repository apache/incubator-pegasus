// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "mutation_log.h"
#include "mutation_log_utils.h"
#include <dsn/utility/fail_point.h>
#include <dsn/utility/errors.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace replication {

/*static*/ error_code mutation_log::replay(log_file_ptr log,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    end_offset = log->start_offset();
    ddebug("start to replay mutation log %s, offset = [%" PRId64 ", %" PRId64 "), size = %" PRId64,
           log->path().c_str(),
           log->start_offset(),
           log->end_offset(),
           log->end_offset() - log->start_offset());

    ::dsn::blob bb;
    log->reset_stream();
    error_s err;
    size_t start_offset = 0;
    while (true) {
        err = replay_block(log, callback, start_offset, end_offset);
        if (!err.is_ok()) {
            // Stop immediately if failed
            break;
        }

        start_offset = static_cast<size_t>(end_offset - log->start_offset());
    }

    ddebug("finish to replay mutation log (%s) [err: %s]",
           log->path().c_str(),
           err.description().c_str());
    return err.code();
}

/*static*/ error_s mutation_log::replay_block(log_file_ptr &log,
                                              replay_callback &callback,
                                              size_t start_offset,
                                              int64_t &end_offset)
{
    FAIL_POINT_INJECT_F("mutation_log_replay_block", [](string_view) -> error_s {
        return error_s::make(ERR_INCOMPLETE_DATA, "mutation_log_replay_block");
    });

    blob bb;
    std::unique_ptr<binary_reader> reader;

    log->reset_stream(start_offset); // start reading from given offset
    int64_t global_start_offset = start_offset + log->start_offset();
    end_offset = global_start_offset; // reset end_offset to the start.

    // reads the entire block into memory
    error_code err = log->read_next_log_block(bb);
    if (err != ERR_OK) {
        return error_s::make(err, "failed to read log block");
    }

    reader = dsn::make_unique<binary_reader>(bb);
    end_offset += sizeof(log_block_header);

    // The first block is log_file_header.
    if (global_start_offset == log->start_offset()) {
        end_offset += log->read_file_header(*reader);
        if (!log->is_right_header()) {
            return error_s::make(ERR_INVALID_DATA, "failed to read log file header");
        }
        // continue to parsing the data block
    }

    while (!reader->is_eof()) {
        auto old_size = reader->get_remaining_size();
        mutation_ptr mu = mutation::read_from(*reader, nullptr);
        dassert(nullptr != mu, "");
        mu->set_logged();

        if (mu->data.header.log_offset != end_offset) {
            return FMT_ERR(ERR_INVALID_DATA,
                           "offset mismatch in log entry and mutation {} vs {}",
                           end_offset,
                           mu->data.header.log_offset);
        }

        int log_length = old_size - reader->get_remaining_size();

        callback(log_length, mu);

        end_offset += log_length;
    }

    return error_s::ok();
}

/*static*/ error_code mutation_log::replay(std::vector<std::string> &log_files,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    std::map<int, log_file_ptr> logs;
    for (auto &fpath : log_files) {
        error_code err;
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr) {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA ||
                err == ERR_INVALID_PARAMETERS) {
                dinfo("skip file %s during log replay", fpath.c_str());
                continue;
            } else {
                return err;
            }
        }

        dassert(
            logs.find(log->index()) == logs.end(), "invalid log_index, index = %d", log->index());
        logs[log->index()] = log;
    }

    return replay(logs, callback, end_offset);
}

/*static*/ error_code mutation_log::replay(std::map<int, log_file_ptr> &logs,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    int64_t g_start_offset = 0;
    int64_t g_end_offset = 0;
    error_code err = ERR_OK;
    log_file_ptr last;

    if (logs.size() > 0) {
        g_start_offset = logs.begin()->second->start_offset();
        g_end_offset = logs.rbegin()->second->end_offset();
    }

    error_s error = log_utils::check_log_files_continuity(logs);
    if (!error.is_ok()) {
        derror_f("check_log_files_continuity failed: {}", error);
        return error.code();
    }

    end_offset = g_start_offset;

    for (auto &kv : logs) {
        log_file_ptr &log = kv.second;

        if (log->start_offset() != end_offset) {
            derror("offset mismatch in log file offset and global offset %" PRId64 " vs %" PRId64,
                   log->start_offset(),
                   end_offset);
            return ERR_INVALID_DATA;
        }

        last = log;
        err = mutation_log::replay(log, callback, end_offset);

        log->close();

        if (err == ERR_OK || err == ERR_HANDLE_EOF) {
            // do nothing
        } else if (err == ERR_INCOMPLETE_DATA) {
            // If the file is not corrupted, it may also return the value of ERR_INCOMPLETE_DATA.
            // In this case, the correctness is relying on the check of start_offset.
            dwarn("delay handling error: %s", err.to_string());
        } else {
            // for other errors, we should break
            break;
        }
    }

    if (err == ERR_OK || err == ERR_HANDLE_EOF) {
        // the log may still be written when used for learning
        dassert(g_end_offset <= end_offset,
                "make sure the global end offset is correct: %" PRId64 " vs %" PRId64,
                g_end_offset,
                end_offset);
        err = ERR_OK;
    } else if (err == ERR_INCOMPLETE_DATA) {
        // ignore the last incomplate block
        err = ERR_OK;
    } else {
        // bad error
        derror("replay mutation log failed: %s", err.to_string());
    }

    return err;
}

} // namespace replication
} // namespace dsn
