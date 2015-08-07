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

# include <atomic>
# include <dsn/ports.h>
# include <dsn/internal/extensible_object.h>
# include <dsn/internal/task_spec.h>
# include <dsn/cpp/auto_codes.h>

namespace dsn 
{
    class rpc_client_session;
    class rpc_server_session;
    class rpc_client_matcher;

    typedef ::dsn::ref_ptr<rpc_client_session> rpc_client_session_ptr;
    typedef ::dsn::ref_ptr<rpc_server_session> rpc_server_session_ptr;
    typedef ::dsn::ref_ptr<rpc_client_matcher> rpc_client_matcher_ptr;

    typedef struct dsn_buffer_t // binary compatible with WSABUF on windows
    {
        unsigned long length;
        char          *buffer;
    } dsn_buffer_t;

    typedef struct message_header
    {
        int32_t       hdr_crc32;
        int32_t       body_crc32;
        int32_t       body_length;
        int32_t       version;
        uint64_t      id;
        uint64_t      rpc_id;
        char          rpc_name[DSN_MAX_TASK_CODE_NAME_LENGTH];

        // info from client => server
        union
        {
            struct
            {
                int32_t  timeout_ms;
                int32_t  hash;
                uint16_t port;
            } client;

            struct
            {
                int32_t  error;
            } server;
        };
    } message_header;
            
    class message_ex : 
        public ref_counter, 
        public extensible_object<message_ex, 4>
    {
    public:
        message_header         *header;
        std::vector<blob>      buffers; // header included for *send* message, 
                                        // header not included for *recieved* 

        // by rpc and network
        rpc_server_session_ptr server_session;
        dsn_address_t          from_address;
        dsn_address_t          to_address;
        uint16_t               local_rpc_code;

    public:        
        //message_ex(blob bb, bool parse_hdr = true); // read 
        ~message_ex();

        //
        // utility routines
        //
        bool is_right_header() const;
        bool is_right_body() const;
        error_code error() const { return header->server.error; }        
        static uint64_t new_id() { return ++_id; }
        static bool is_right_header(char* hdr);
        static int  get_body_length(char* hdr)
        {
            return ((message_header*)hdr)->body_length;
        }

        //
        // routines for create messages
        //
        static message_ex* create_receive_message(blob& data);
        static message_ex* create_request(dsn_task_code_t rpc_code, int timeout_milliseconds = 0, int hash = 0);
        message_ex* create_response();


        //
        // routines for buffer management
        //        
        void write_next(void** ptr, size_t* size, size_t min_size);
        void write_commit(size_t size);
        bool read_next(void** ptr, size_t* size);
        void read_commit(size_t size);
        size_t body_size() { return (size_t)header->body_length; }
        void* rw_ptr(size_t offset_begin);
        void seal(bool crc_required);

    private:
        message_ex();
        void prepare_buffer_header();

    private:        
        static std::atomic<uint64_t> _id;

    private:
        // by msg read & write
        int                    _rw_index;
        int                    _rw_offset;
        bool                   _rw_committed;
        bool                   _is_read;
    };

} // end namespace
