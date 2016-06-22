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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Jun. 2016, Zuoyan Qin, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "dsn_message_parser.h"
# include <dsn/service_api_c.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "dsn.message.parser"

namespace dsn
{
    dsn_message_parser::dsn_message_parser(int buffer_block_size, bool is_write_only)
        : message_parser(buffer_block_size, is_write_only),
          _header_checked(false)
    {
    }

    message_ex* dsn_message_parser::get_message_on_receive(unsigned int read_length, /*out*/ int& read_next)
    {
        mark_read(read_length);

        if (_read_buffer_occupied >= sizeof(message_header))
        {
            if (!_header_checked)
            {
                if (!message_ex::is_right_header((char*)_read_buffer.data()))
                {
                    derror("receive message header check failed for message");

                    truncate_read();
                    read_next = -1;
                    return nullptr;
                }
                else
                {
                    _header_checked = true;
                }
            }

            unsigned int msg_sz = sizeof(message_header) + message_ex::get_body_length((char*)_read_buffer.data());

            // msg done
            if (_read_buffer_occupied >= msg_sz)
            {
                auto msg_bb = _read_buffer.range(0, msg_sz);
                message_ex* msg = message_ex::create_receive_message(msg_bb);
                if (!msg->is_right_body(false))
                {
                    message_header* header = (message_header*)_read_buffer.data();
                    derror("body check failed for message, id: %d, rpc_name: %s, from: %s",
                          header->id, header->rpc_name, header->from_address.to_string());

                    truncate_read();
                    read_next = -1;
                    return nullptr;
                }
                else
                {
                    _read_buffer = _read_buffer.range(msg_sz);
                    _read_buffer_occupied -= msg_sz;
                    _header_checked = false;

                    read_next = sizeof(message_header);
                    return msg;
                }
            }
            else
            {
                read_next = msg_sz - _read_buffer_occupied;
                return nullptr;
            }
        }

        else
        {
            read_next = sizeof(message_header) - _read_buffer_occupied;
            return nullptr;
        }
    }

    void dsn_message_parser::truncate_read()
    {
        message_parser::truncate_read();
        _header_checked = false;
    }

    int dsn_message_parser::prepare_on_send(message_ex* msg)
    {
        return (int)msg->buffers.size();
    }

    int dsn_message_parser::get_buffers_on_send(message_ex* msg, /*out*/ send_buf* buffers)
    {
        int i = 0;        
        for (auto& buf : msg->buffers)
        {
            buffers[i].buf = (void*)buf.data();
            buffers[i].sz = (size_t)buf.length();
            ++i;
        }
        return i;
    }
}
