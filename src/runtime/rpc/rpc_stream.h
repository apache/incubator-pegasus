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

#include "common/gpid.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/app_model.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/task_code.h"
#include "utils/api_utilities.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_code.h"
#include "utils/utils.h"

namespace dsn {

// rpc_read_stream is a bridge between binary_reader and rpc_message, with which you can
// easily visit rpc_message's buffer in binary_reader's manner.
class rpc_read_stream : public binary_reader
{
public:
    rpc_read_stream(message_ex *msg) { set_read_msg(msg); }

    rpc_read_stream() : _msg(nullptr) {}

    void set_read_msg(message_ex *msg)
    {
        _msg = msg;
        if (nullptr != _msg) {
            ::dsn::blob bb;
            CHECK(((::dsn::message_ex *)_msg)->read_next(bb),
                  "read msg must have one segment of buffer ready");
            init(std::move(bb));
        }
    }

    int read(char *buffer, int sz) { return inner_read(buffer, sz); }

    int read(blob &blob, int len) { return inner_read(blob, len); }

    ~rpc_read_stream()
    {
        if (_msg) {
            _msg->read_commit((size_t)(total_size() - get_remaining_size()));
        }
    }

private:
    dsn::message_ex *_msg;
};
typedef ::dsn::ref_ptr<rpc_read_stream> rpc_read_stream_ptr;

// rpc_write_stream is a bridge between binary_writer and rpc_message, with which you can
// easily store data to rpc_message's buffer in binary_writer's manner.
class rpc_write_stream : public binary_writer
{
public:
    rpc_write_stream(message_ex *msg)
        : _msg(msg), _last_write_next_committed(true), _last_write_next_total_size(0)
    {
    }

    // write buffer for rpc_write_stream is allocated from
    // a per-thread pool, and it is expected that
    // the per-thread pool cannot allocated two outstanding
    // buffers at the same time.
    // e.g., alloc1, commit1, alloc2, commit2 is ok
    // while alloc1, alloc2, commit2, commit 1 is invalid
    // TODO(zhaoliwei): remove functions and variables below, because msg is not implemented by tls
    // memory now
    void commit_buffer()
    {
        if (!_last_write_next_committed) {
            _msg->write_commit((size_t)(total_size() - _last_write_next_total_size));
            _last_write_next_committed = true;
        }
    }

    virtual ~rpc_write_stream() { flush(); }

    virtual void flush() override
    {
        binary_writer::flush();
        commit_buffer();
    }

private:
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb) override
    {
        commit_buffer();

        void *ptr;
        size_t sz;
        _msg->write_next(&ptr, &sz, size);
        DCHECK_GE_MSG(sz, size, "allocated buffer size must be not less than the required size");
        bb.assign((const char *)ptr, 0, (int)sz);

        _last_write_next_total_size = total_size();
        _last_write_next_committed = false;
    }

private:
    message_ex *_msg;
    bool _last_write_next_committed;
    int _last_write_next_total_size;
};
typedef ::dsn::ref_ptr<rpc_write_stream> rpc_write_stream_ptr;
} // namespace dsn
