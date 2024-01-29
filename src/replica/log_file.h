/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "aio/aio_task.h"
#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "runtime/api_task.h"
#include "runtime/task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class binary_reader;
class binary_writer;
class blob;
class disk_file;
class task_tracker;

namespace replication {
class log_appender;
class log_block;

// each log file has a log_file_header stored at the beginning of the first block's data content
struct log_file_header
{
    int32_t magic;   // 0xdeadbeef
    int32_t version; // current 0x1
    int64_t
        start_global_offset; // start offset in the global space, equals to the file name's postfix
};

// a structure to record replica's log info
struct replica_log_info
{
    decree max_decree;
    int64_t valid_start_offset; // valid start offset in global space
    replica_log_info(decree d, int64_t o)
    {
        max_decree = d;
        valid_start_offset = o;
    }
    replica_log_info()
    {
        max_decree = 0;
        valid_start_offset = 0;
    }
    bool operator==(const replica_log_info &o) const
    {
        return max_decree == o.max_decree && valid_start_offset == o.valid_start_offset;
    }
};

typedef std::unordered_map<gpid, replica_log_info> replica_log_info_map;

class log_file;

typedef dsn::ref_ptr<log_file> log_file_ptr;

//
// the log file is structured with sequences of log_blocks,
// each block consists of the log_block_header + log_content,
// and the first block contains the log_file_header at the beginning
//
// the class is not thread safe
//
class log_file : public ref_counter
{
public:
    ~log_file();

    //
    // file operations
    //

    // open the log file for read
    // 'path' should be in format of log.{index}.{start_offset}, where:
    //   - index: the index of the log file, start from 1
    //   - start_offset: start offset in the global space
    // returns:
    //   - non-null if open succeed
    //   - null if open failed
    static log_file_ptr open_read(const char *path, /*out*/ error_code &err);

    // open the log file for write
    // the file path is '{dir}/log.{index}.{start_offset}'
    // returns:
    //   - non-null if open succeed
    //   - null if open failed
    static log_file_ptr create_write(const char *dir, int index, int64_t start_offset);

    // close the log file
    void close();

    // flush the log file
    void flush() const;

    //
    // read routines
    //

    // sync read the next log entry from the file
    // the entry data is start from the 'local_offset' of the file
    // the result is passed out by 'bb', not including the log_block_header
    // return error codes:
    //  - ERR_OK
    //  - ERR_HANDLE_EOF
    //  - ERR_INCOMPLETE_DATA
    //  - ERR_INVALID_DATA
    //  - other io errors caused by file read operator
    error_code read_next_log_block(/*out*/ ::dsn::blob &bb);

    //
    // write routines
    //

    // async write log entry into the file
    // 'block' is the date to be written
    // 'offset' is start offset of the entry in the global space
    // 'evt' is to indicate which thread pool to execute the callback
    // 'callback_host' is used to get tracer
    // 'callback' is to indicate the callback handler
    // 'hash' helps to choose which thread in the thread pool to execute the callback
    // returns:
    //   - non-null if io task is in pending
    //   - null if error
    dsn::aio_task_ptr commit_log_block(log_block &block,
                                       int64_t offset,
                                       dsn::task_code evt,
                                       dsn::task_tracker *tracker,
                                       aio_handler &&callback,
                                       int hash);
    dsn::aio_task_ptr commit_log_blocks(log_appender &pending,
                                        dsn::task_code evt,
                                        dsn::task_tracker *tracker,
                                        aio_handler &&callback,
                                        int hash);

    //
    // others
    //

    // Reset file_streamer to point to `offset`.
    // offset=0 means the start of this log file.
    void reset_stream(size_t offset = 0);
    // end offset in the global space: end_offset = start_offset + file_size
    int64_t end_offset() const { return _end_offset.load(); }
    // start offset in the global space
    int64_t start_offset() const { return _start_offset; }
    // file index
    int index() const { return _index; }
    // file path
    const std::string &path() const { return _path; }
    // previous decrees
    const replica_log_info_map &previous_log_max_decrees() const
    {
        return _previous_log_max_decrees;
    }
    // previous decree for speicified gpid
    decree previous_log_max_decree(const gpid &pid) const;
    // file header
    const log_file_header &header() const { return _header; }

    // read file header from reader, return byte count consumed
    int read_file_header(binary_reader &reader);
    // write file header to writer, return byte count written
    int write_file_header(binary_writer &writer, const replica_log_info_map &init_max_decrees);
    // get serialized size of current file header
    int get_file_header_size() const;
    // if the file header is valid
    bool is_right_header() const;

    // set & get last write time, used for gc
    void set_last_write_time(uint64_t last_write_time) { _last_write_time = last_write_time; }
    uint64_t last_write_time() const { return _last_write_time; }

    const disk_file *file_handle() const { return _handle; }

private:
    // make private, user should create log_file through open_read() or open_write()
    log_file(const char *path, disk_file *handle, int index, int64_t start_offset, bool is_read);

private:
    friend class mock_log_file;

    uint32_t _crc32;
    const int64_t _start_offset; // start offset in the global space
    std::atomic<int64_t>
        _end_offset; // end offset in the global space: end_offset = start_offset + file_size
    class file_streamer;

    std::unique_ptr<file_streamer> _stream;
    disk_file *_handle;        // file handle
    const bool _is_read;       // if opened for read or write
    const std::string _path;   // file path
    const int _index;          // file index
    log_file_header _header;   // file header
    uint64_t _last_write_time; // seconds from epoch time

    mutable zlock _write_lock;

    // this data is used for garbage collection, and is part of file header.
    // for read, the value is read from file header.
    // for write, the value is set by write_file_header().
    replica_log_info_map _previous_log_max_decrees;
};

} // namespace replication
} // namespace dsn
