// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "dist/replication/lib/mutation_log.h"
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

} // namespace replication
} // namespace dsn
