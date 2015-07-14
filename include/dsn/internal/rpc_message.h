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
# include <dsn/internal/dsn_types.h>
# include <dsn/internal/end_point.h>
# include <dsn/internal/extensible_object.h>
# include <dsn/internal/task_code.h>
# include <dsn/internal/error_code.h>
# include <dsn/internal/memory.tools.h>

namespace dsn {

struct message_header
{    
    int32_t       hdr_crc32;
    int32_t       body_crc32;
    int32_t       body_length;
    int32_t       version;
    uint64_t      id;
    uint64_t      rpc_id;
    char          rpc_name[MAX_TASK_CODE_NAME_LENGTH + 1];

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

    // local fields - no need to be transmitted
    end_point     from_address;
    end_point     to_address;
    uint16_t      local_rpc_code;
    
    void marshall(binary_writer& writer);
    void unmarshall(binary_reader& reader);
    void new_rpc_id();
    
    static bool is_right_header(char* hdr);
    static int get_body_length(char* hdr)
    {
        return ((message_header*)hdr)->body_length;
    }
};

# define MSG_HDR_SERIALIZED_SIZE (static_cast<int>(FIELD_OFFSET(message_header, from_address)))

class rpc_server_session;
class message : public ref_object, public extensible_object<message, 4>, public ::dsn::tools::memory::tallocator_object
{
public:
    message(); // write             
    message(blob bb, bool parse_hdr = true); // read 
    virtual ~message();

    //
    // routines for request and response
    //
    static message_ptr create_request(task_code rpc_code, int timeout_milliseconds = 0, int hash = 0);
    message_ptr create_response();

    //
    // routines for reader & writer
    //
    binary_reader& reader() { return *_reader; }
    binary_writer& writer() { return *_writer; }

    //
    // meta info
    //
    void seal(bool fillCrc, bool is_placeholder = false);
    message_header& header() { return _msg_header; }
    int  total_size() const { return is_read() ? _reader->total_size() : _writer->total_size(); }
    bool is_read() const { return _reader != nullptr; }
    error_code error() const { error_code ec; ec.set(_msg_header.server.error); return ec; }
    bool is_right_header() const;
    bool is_right_body() const;
    static uint64_t new_id() { return ++_id; }
    rpc_server_session_ptr& server_session() { return _server_session; }

private:            
    void read_header();

private:
    message_header         _msg_header;    
    binary_reader          *_reader;
    binary_writer          *_writer;
    rpc_server_session_ptr _server_session;

protected:
    static std::atomic<uint64_t> _id;
};

DEFINE_REF_OBJECT(message)

} // end namespace
