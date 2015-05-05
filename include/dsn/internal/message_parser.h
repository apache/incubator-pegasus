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

# include <dsn/internal/dsn_types.h>
# include <dsn/internal/rpc_message.h>

namespace dsn 
{
    class message_parser
    {
    public:
        template <typename T> static message_parser* create(int buffer_block_size)
        {
            return new T(buffer_block_size);
        }

    public:
        message_parser(int buffer_block_size);

        // before read
        void* read_buffer_ptr(int read_next);
        int read_buffer_capacity() const;

        // afer read, see if we can compose a message
        virtual message_ptr on_read(int read_length, __out_param int& read_next) = 0;

        // before write
        virtual void get_output_buffers(message_ptr& msg, __out_param std::vector<blob>& buffers) = 0;
        
    protected:
        void create_new_buffer(int sz);
        void mark_read(int read_length);

    protected:        
        blob            _read_buffer;
        int                    _read_buffer_occupied;
        int                    _buffer_block_size;
    };

    class dsn_message_parser : public message_parser
    {
    public:
        dsn_message_parser(int buffer_block_size);

        virtual message_ptr on_read(int read_length, __out_param int& read_next);

        virtual void get_output_buffers(message_ptr& msg, __out_param std::vector<blob>& buffers)
        {
            return msg->writer().get_buffers(buffers);
        }
    };
}