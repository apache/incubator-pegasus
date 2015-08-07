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
# pragma once

# include <dsn/cpp/utils.h>
# include <dsn/service_api_c.h>

namespace dsn
{
    class msg_binary_reader : public binary_reader
    {
    public:
        msg_binary_reader(dsn_message_t msg)
            : _msg(msg)
        {
            void* ptr;
            size_t size;
            bool r = dsn_msg_read_next(msg, &ptr, &size);
            dbg_dassert(r, "read msg must have one segment of buffer ready");

            blob bb((const char*)ptr, 0, (int)size);
            init(bb);
        }

        ~msg_binary_reader()
        {
            dsn_msg_read_commit(_msg, (size_t)(total_size() - get_remaining_size()));
        }

    private:
        dsn_message_t _msg;
    };

    class msg_binary_writer : public binary_writer
    {
    public:
        msg_binary_writer(dsn_message_t msg)
            : _msg(msg)
        {
            _last_write_next_committed = true;
            _last_write_next_total_size = 0;
        }

        // write buffer for msg_binary_writer is allocated from
        // a per-thread pool, and it is expected that
        // the per-thread pool cannot allocated two outstanding
        // buffers at the same time.
        // e.g., alloc1, commit1, alloc2, commit2 is ok
        // while alloc1, alloc2, commit2, commit 1 is invalid
        void commit_buffer()
        {
            if (!_last_write_next_committed)
            {
                dsn_msg_write_commit(_msg, (size_t)(total_size() - _last_write_next_total_size));
                _last_write_next_committed = true;
            }
        }

        ~msg_binary_writer()
        {
            commit_buffer();
        }
        
    private:
        virtual void create_new_buffer(size_t size, /*out*/blob& bb) override
        {
            commit_buffer();

            void* ptr;
            size_t sz;
            dsn_msg_write_next(_msg, &ptr, &sz, size);
            dbg_dassert(sz >= size, "allocated buffer size must be not less than the required size");
            bb.assign((const char*)ptr, 0, (int)sz);

            _last_write_next_total_size = total_size();
            _last_write_next_committed = false;
        }

    private:
        dsn_message_t _msg;
        bool          _last_write_next_committed;
        int           _last_write_next_total_size;
    };
}