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

# pragma once

# include <atomic>
# include <dsn/internal/ports.h>
# include <dsn/internal/extensible_object.h>
# include <dsn/internal/task_spec.h>
# include <dsn/internal/callocator.h>
# include <dsn/cpp/auto_codes.h>
# include <dsn/cpp/address.h>
# include <dsn/internal/link.h>

namespace dsn 
{
    class rpc_session;
    typedef ::dsn::ref_ptr<rpc_session> rpc_session_ptr;

    typedef struct dsn_buffer_t // binary compatible with WSABUF on windows
    {
        unsigned long length;
        char          *buffer;
    } dsn_buffer_t;

    struct fast_rpc_name
    {
        uint32_t local_rpc_id;
        uint32_t local_hash; // same hash from two processes indicates that
                             // the mapping of rpc string and id are consistent, which
                             // we leverage for optimization (fast rpc handler lookup)
    };

    typedef struct message_header
    {
        int32_t        hdr_crc32;
        int32_t        body_crc32;
        int32_t        body_length;
        int32_t        version;
        uint64_t       id;      // sequence id, can be used to track source
        uint64_t       rpc_id;  // correlation id for connecting rpc caller, request, and response tasks
        char           rpc_name[DSN_MAX_TASK_CODE_NAME_LENGTH];
        fast_rpc_name  rpc_name_fast;
        dsn_gpid  gpid; // global partition id
        dsn_msg_context_t context;
        rpc_address       from_address; // always ipv4/v6 address,
                                        // generally, it is the from_node's primary address, except the
                                        // case described in message_ex::create_response()'s ATTENTION comment.
                                        // the from_address is always the orignal client's address, it will
                                        // not be changed in forwarding request.

        struct
        {
            int32_t  timeout_ms;
            int32_t  hash;
        } client;

        struct
        {
            int32_t  error;
        } server;
    } message_header;

    class message_ex : 
        public ref_counter, 
        public extensible_object<message_ex, 4>,
        public transient_object
    {
    public:
        message_header         *header;
        std::vector<blob>      buffers; // header included for *send* message, 
                                        // header not included for *recieved* 

        // by rpc and network
        rpc_session_ptr        io_session;     // send/recv session        
        rpc_address            to_address;     // always ipv4/v6 address, it is the to_node's net address
        rpc_address            server_address; // used by requests, and may be of uri/group address
        uint32_t               local_rpc_code;

        // by message queuing
        dlink                  dl;

    public:        
        //message_ex(blob bb, bool parse_hdr = true); // read 
        ~message_ex();

        //
        // utility routines
        //
        bool is_right_header() const;
        bool is_right_body(bool is_write_msg) const;
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
        static message_ex* create_receive_message(const blob& data);
        static message_ex* create_request(
            dsn_task_code_t rpc_code, 
            int timeout_milliseconds = 0,
            int request_hash = 0, 
            uint64_t partition_hash = 0
            );
        static message_ex* create_receive_message_with_standalone_header(const blob& data);
        message_ex* create_response();
        message_ex* copy();
        message_ex* copy_and_prepare_send();

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
        int                    _rw_index;     // current buffer index
        int                    _rw_offset;    // current buffer offset
        bool                   _rw_committed; // mark if it is in middle state of reading/writing
        bool                   _is_read;      // is for read(recv) or write(send)

    public:
        static uint32_t s_local_hash;  // used by fast_rpc_name
    };

} // end namespace
