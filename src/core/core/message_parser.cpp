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

    //-------------------- msg reader --------------------
    char* message_reader::read_buffer_ptr(unsigned int read_next)
    {
        if (read_next + _buffer_occupied > _buffer.length())
        {
            // remember currently read content
            blob rb;
            if (_buffer_occupied > 0)
                rb = _buffer.range(0, _buffer_occupied);
            
            // switch to next
            unsigned int sz = (read_next + _buffer_occupied > _buffer_block_size ?
                        read_next + _buffer_occupied : _buffer_block_size);
            _buffer.assign(dsn::make_shared_array<char>(sz), 0, sz);
            _buffer_occupied = 0;

            // copy
            if (rb.length() > 0)
            {
                memcpy((void*)_buffer.data(), (const void*)rb.data(), rb.length());
                _buffer_occupied = rb.length();
            }
            
            dassert (read_next + _buffer_occupied <= _buffer.length(), "");
        }

        return (char*)(_buffer.data() + _buffer_occupied);
    }

    //-------------------- msg parser manager --------------------
    message_parser_manager::message_parser_manager()
    {
    }

    void message_parser_manager::register_factory(network_header_format fmt, message_parser::factory f, message_parser::factory2 f2, size_t sz)
    {
        if (static_cast<unsigned int>(fmt) >= _factory_vec.size())
        {
            _factory_vec.resize(fmt + 1);
        }

        parser_factory_info& info = _factory_vec[fmt];
        info.fmt = fmt;
        info.factory = f;
        info.factory2 = f2;
        info.parser_size = sz;
    }

    message_parser* message_parser_manager::create_parser(network_header_format fmt)
    {
        parser_factory_info& info = _factory_vec[fmt];
        if (info.factory)
            return info.factory();
        else
            return nullptr;
    }

}
