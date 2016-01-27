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
 *     message parser base prototype, to support different kinds
 *     of message headers (so as to interact among them)
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

# include <dsn/ports.h>
# include <dsn/internal/rpc_message.h>
# include <dsn/internal/singleton.h>
# include <vector>

namespace dsn 
{
    class message_parser
    {
    public:
        template <typename T> static message_parser* create(int buffer_block_size, bool is_write_only)
        {
            return new T(buffer_block_size, is_write_only);
        }

        template <typename T> static message_parser* create2(void* place, int buffer_block_size, bool is_write_only)
        {
            return new(place) T(buffer_block_size, is_write_only);
        }

        typedef message_parser*  (*factory)(int, bool);
        typedef message_parser*  (*factory2)(void*, int, bool);
        
    public:
        message_parser(int buffer_block_size, bool is_write_only);
        virtual ~message_parser() {}

        // before read
        void* read_buffer_ptr(int read_next);
        int read_buffer_capacity() const;

        // after read, see if we can compose a message
        virtual message_ex* get_message_on_receive(int read_length, /*out*/ int& read_next) = 0;

        // before send, prepare buffer
        // be compatible with WSABUF on windows and iovec on linux
# ifdef _WIN32
        struct send_buf
        {
            uint32_t sz;
            void*    buf;            
        };
# else
        struct send_buf
        {
            void*    buf;
            size_t   sz;
        };
# endif

        // caller must ensure buffers length is correct as get_send_buffers_count_and_total_length(...);
        // return buffer count used
        virtual int prepare_buffers_on_send(message_ex* msg, int offset, /*out*/ send_buf* buffers) = 0;

        virtual int get_send_buffers_count_and_total_length(message_ex* msg, /*out*/ int* total_length) = 0;

        // all current read-ed content are discarded
        virtual void truncate_read() { _read_buffer_occupied = 0; }
        
    protected:
        void create_new_buffer(int sz);
        void mark_read(int read_length);

    protected:        
        blob            _read_buffer;
        int             _read_buffer_occupied;
        int             _buffer_block_size;
    };

    class message_parser_manager : public utils::singleton<message_parser_manager>
    {
    public:
        struct parser_factory_info
        {
            parser_factory_info() : fmt(NET_HDR_DSN) {}

            network_header_format fmt;
            message_parser::factory factory;
            message_parser::factory2 factory2;
            size_t parser_size;
        };

    public:
        message_parser_manager();

        // called only during system init, thread-unsafe
        void register_factory(network_header_format fmt, message_parser::factory f, message_parser::factory2 f2, size_t sz);

        message_parser* create_parser(network_header_format fmt, int buffer_blk_size, bool is_write_only);
        const parser_factory_info& get(network_header_format fmt) { return _factory_vec[fmt]; }

    private:
        std::vector<parser_factory_info> _factory_vec;
    };

    class dsn_message_parser : public message_parser
    {
    public:
        dsn_message_parser(int buffer_block_size, bool is_write_only);

        virtual message_ex* get_message_on_receive(int read_length, /*out*/ int& read_next) override;

        virtual int prepare_buffers_on_send(message_ex* msg, int offset, /*out*/ send_buf* buffers) override;

        virtual int get_send_buffers_count_and_total_length(message_ex* msg, /*out*/ int* total_length) override;
    };
}
