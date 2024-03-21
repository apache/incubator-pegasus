

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

#include "log_file.h"
#include "common/replication.codes.h"

namespace dsn {
namespace replication {

class log_file::file_streamer
{
public:
    explicit file_streamer(disk_file *fd, size_t file_offset)
        : _file_dispatched_bytes(file_offset), _file_handle(fd)
    {
        _current_buffer = _buffers + 0;
        _next_buffer = _buffers + 1;
        fill_buffers();
    }
    ~file_streamer()
    {
        _current_buffer->wait_ongoing_task();
        _next_buffer->wait_ongoing_task();
    }
    // try to reset file_offset
    void reset(size_t file_offset)
    {
        _current_buffer->wait_ongoing_task();
        _next_buffer->wait_ongoing_task();
        // fast path if we can just move the cursor
        if (_current_buffer->_file_offset_of_buffer <= file_offset &&
            _current_buffer->_file_offset_of_buffer + _current_buffer->_end > file_offset) {
            _current_buffer->_begin = file_offset - _current_buffer->_file_offset_of_buffer;
        } else {
            _current_buffer->_begin = _current_buffer->_end = _next_buffer->_begin =
                _next_buffer->_end = 0;
            _file_dispatched_bytes = file_offset;
        }
        fill_buffers();
    }

    // TODO(wutao1): use absl::string_view instead of using blob.
    // WARNING: the resulted blob is not guaranteed to be reference counted.
    // possible error_code:
    //  ERR_OK                      result would always size as expected
    //  ERR_HANDLE_EOF              if there are not enough data in file. result would still be
    //                              filled with possible data
    //  ERR_FILE_OPERATION_FAILED   filesystem failure
    error_code read_next(size_t size, /*out*/ blob &result)
    {
        binary_writer writer(size);

#define TRY(x)                                                                                     \
    do {                                                                                           \
        auto _x = (x);                                                                             \
        if (_x != ERR_OK) {                                                                        \
            result = writer.get_current_buffer();                                                  \
            return _x;                                                                             \
        }                                                                                          \
    } while (0)

        TRY(_current_buffer->wait_ongoing_task());
        if (size < _current_buffer->length()) {
            result.assign(_current_buffer->_buffer.get(), _current_buffer->_begin, size);
            _current_buffer->_begin += size;
        } else {
            _current_buffer->drain(writer);
            // we can now assign result since writer must have allocated a buffer.
            CHECK_GT(writer.total_size(), 0);
            if (size > writer.total_size()) {
                TRY(_next_buffer->wait_ongoing_task());
                _next_buffer->consume(writer,
                                      std::min(size - writer.total_size(), _next_buffer->length()));
                // We hope that this never happens, it would deteriorate performance
                if (size > writer.total_size()) {
                    auto task =
                        file::read(_file_handle,
                                   writer.get_current_buffer().buffer().get() + writer.total_size(),
                                   size - writer.total_size(),
                                   _file_dispatched_bytes,
                                   LPC_AIO_IMMEDIATE_CALLBACK,
                                   nullptr,
                                   nullptr);
                    task->wait();
                    writer.write_empty(task->get_transferred_size());
                    _file_dispatched_bytes += task->get_transferred_size();
                    TRY(task->error());
                }
            }
            result = writer.get_current_buffer();
        }
        fill_buffers();
        return ERR_OK;
#undef TRY
    }

private:
    void fill_buffers()
    {
        while (!_current_buffer->_have_ongoing_task && _current_buffer->empty()) {
            _current_buffer->_begin = _current_buffer->_end = 0;
            _current_buffer->_file_offset_of_buffer = _file_dispatched_bytes;
            _current_buffer->_have_ongoing_task = true;
            _current_buffer->_task = file::read(_file_handle,
                                                _current_buffer->_buffer.get(),
                                                block_size_bytes,
                                                _file_dispatched_bytes,
                                                LPC_AIO_IMMEDIATE_CALLBACK,
                                                nullptr,
                                                nullptr);
            _file_dispatched_bytes += block_size_bytes;
            std::swap(_current_buffer, _next_buffer);
        }
    }

    // buffer size, in bytes
    // TODO(wutao1): call it BLOCK_BYTES_SIZE
    static const size_t block_size_bytes;
    struct buffer_t
    {
        std::unique_ptr<char[]> _buffer; // with block_size
        size_t _begin, _end;             // [buffer[begin]..buffer[end]) contains unconsumed_data
        size_t _file_offset_of_buffer;   // file offset projected to buffer[0]
        bool _have_ongoing_task;
        aio_task_ptr _task;

        buffer_t()
            : _buffer(new char[block_size_bytes]),
              _begin(0),
              _end(0),
              _file_offset_of_buffer(0),
              _have_ongoing_task(false)
        {
        }
        size_t length() const { return _end - _begin; }
        bool empty() const { return length() == 0; }
        void consume(binary_writer &dest, size_t len)
        {
            dest.write(_buffer.get() + _begin, len);
            _begin += len;
        }
        size_t drain(binary_writer &dest)
        {
            auto len = length();
            consume(dest, len);
            return len;
        }
        error_code wait_ongoing_task()
        {
            if (_have_ongoing_task) {
                _task->wait();
                _have_ongoing_task = false;
                _end += _task->get_transferred_size();
                CHECK_LE_MSG(_end, block_size_bytes, "invalid io_size");
                return _task->error();
            } else {
                return ERR_OK;
            }
        }
    } _buffers[2];
    buffer_t *_current_buffer, *_next_buffer;

    // number of bytes we have issued read operations
    size_t _file_dispatched_bytes;
    disk_file *_file_handle;
};

const size_t log_file::file_streamer::block_size_bytes = 1024 * 1024; // 1MB

} // namespace replication
} // namespace dsn
