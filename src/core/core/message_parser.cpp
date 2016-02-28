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
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/internal/message_parser.h>
# include <dsn/service_api_c.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "message.parser"

namespace dsn {

    message_parser::message_parser(int buffer_block_size, bool is_write_only)
        : _buffer_block_size(buffer_block_size)
    {
        if (!is_write_only)
        {
            create_new_buffer(buffer_block_size);
        }
    }

    void message_parser::create_new_buffer(int sz)
    {
        _read_buffer.assign(std::shared_ptr<char>(new char[sz], std::default_delete<char[]>{}), 0, sz);
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

    //-------------------- msg parser manager --------------------
    message_parser_manager::message_parser_manager()
    {
    }

    void message_parser_manager::register_factory(network_header_format fmt, message_parser::factory f, message_parser::factory2 f2, size_t sz)
    {
        if (fmt >= _factory_vec.size())
        {
            _factory_vec.resize(fmt + 1);
        }

        parser_factory_info& info = _factory_vec[fmt];
        info.fmt = fmt;
        info.factory = f;
        info.factory2 = f2;
        info.parser_size = sz;
    }

    message_parser* message_parser_manager::create_parser(network_header_format fmt, int buffer_blk_size, bool is_write_only)
    {
        parser_factory_info& info = _factory_vec[fmt];
        return info.factory(buffer_blk_size, is_write_only);
    }

    //-------------------- dsn message --------------------

    dsn_message_parser::dsn_message_parser(int buffer_block_size, bool is_write_only)
        : message_parser(buffer_block_size, is_write_only)
    {
    }
    
    message_ex* dsn_message_parser::get_message_on_receive(int read_length, /*out*/ int& read_next)
    {
        mark_read(read_length);

        if (_read_buffer_occupied >= sizeof(message_header))
        {            
            int msg_sz = sizeof(message_header) +
                message_ex::get_body_length((char*)_read_buffer.data());

            // msg done
            if (_read_buffer_occupied >= msg_sz)
            {
                auto msg_bb = _read_buffer.range(0, msg_sz);
                message_ex* msg = message_ex::create_receive_message(msg_bb);

                dassert(msg->is_right_header() && msg->is_right_body(false), "");

                _read_buffer = _read_buffer.range(msg_sz);
                _read_buffer_occupied -= msg_sz;
                read_next = sizeof(message_header);
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
            read_next = sizeof(message_header) - _read_buffer_occupied;
            return nullptr;
        }
    }

    int dsn_message_parser::prepare_buffers_on_send(message_ex* msg, int offset, /*out*/ send_buf* buffers)
    {
        int i = 0;        
        for (auto& buf : msg->buffers)
        {
            if (offset >= buf.length())
            {
                offset -= buf.length();
                continue;
            }
         
            buffers[i].buf = (void*)(buf.data() + offset);
            buffers[i].sz = (uint32_t)(buf.length() - offset);
            offset = 0;
            ++i;
        }

        return i;
    }

    int dsn_message_parser::get_send_buffers_count_and_total_length(message_ex* msg, int* total_length)
    {
        *total_length = (int)msg->body_size() + sizeof(message_header);
        return (int)msg->buffers.size();
    }

}
