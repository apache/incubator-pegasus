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
# include <dsn/internal/message_parser.h>
# include <dsn/internal/logging.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "message.parser"

namespace dsn {

    message_parser::message_parser(int buffer_block_size)
        : _buffer_block_size(buffer_block_size)
    {
        create_new_buffer(buffer_block_size);
    }

    void message_parser::create_new_buffer(int sz)
    {
        std::shared_ptr<char> buffer((char*)::malloc(sz));
        _read_buffer.assign(buffer, 0, sz);
        _read_buffer_occupied = 0;
    }

    void message_parser::mark_read(int read_length)
    {
        dassert(read_length + _read_buffer_occupied <= _read_buffer.length(), "");
        _read_buffer_occupied += read_length;
    }

    // before read
    void* message_parser::read_buffer_ptr(int read_next)
    {
        if (read_next + _read_buffer_occupied >  _read_buffer.length())
        {
            // remember currently read content
            auto rb = _read_buffer.range(0, _read_buffer_occupied);
            
            // switch to next
            if (read_next + _read_buffer_occupied > _buffer_block_size)
                create_new_buffer(read_next + _read_buffer_occupied);
            else
                create_new_buffer(_buffer_block_size);

            // copy
            if (rb.length() > 0)
            {
                memcpy((void*)_read_buffer.data(), (const void*)rb.data(), rb.length());
                _read_buffer_occupied = rb.length();
            }            
            
            dassert (read_next + _read_buffer_occupied <= _read_buffer.length(), "");
        }

        return (void*)(_read_buffer.data() + _read_buffer_occupied);
    }

    int message_parser::read_buffer_capacity() const
    {
        return _read_buffer.length() - _read_buffer_occupied;
    }

    //-------------------- dsn message --------------------

    dsn_message_parser::dsn_message_parser(int buffer_block_size)
        : message_parser(buffer_block_size)
    {
    }

    message_ptr dsn_message_parser::get_message_on_receive(int read_length, __out_param int& read_next)
    {
        mark_read(read_length);

        if (_read_buffer_occupied >= MSG_HDR_SERIALIZED_SIZE)
        {            
            int msg_sz = MSG_HDR_SERIALIZED_SIZE +
                message_header::get_body_length((char*)_read_buffer.data());

            // msg done
            if (_read_buffer_occupied >= msg_sz)
            {
                auto msg_bb = _read_buffer.range(0, msg_sz);
                message_ptr msg = new message(msg_bb, true);

                dassert(msg->is_right_header() && msg->is_right_body(), "");

                _read_buffer = _read_buffer.range(msg_sz);
                _read_buffer_occupied -= msg_sz;
                read_next = MSG_HDR_SERIALIZED_SIZE;
                return msg;
            }
            else
            {
                read_next = msg_sz - _read_buffer_occupied;
                return nullptr;
            }
        }

        else
        {
            read_next = MSG_HDR_SERIALIZED_SIZE - _read_buffer_occupied;
            return nullptr;
        }
    }
}