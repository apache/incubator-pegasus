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
# include <dsn/internal/dsn_types.h>
# include <dsn/internal/rpc_message.h>
# include <dsn/internal/logging.h>
# include <dsn/internal/network.h>
# include "task_engine.h"
# include <dsn/service_api.h>
# include "crc.h"

using namespace dsn::utils;

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "message"
#define CRC_INVALID 0xdead0c2c

namespace dsn {

void message_header::marshall(binary_writer& writer)
{
    writer.write((const char*)this, MSG_HDR_SERIALIZED_SIZE);
}

void message_header::unmarshall(binary_reader& reader)
{
    reader.read((char*)this, MSG_HDR_SERIALIZED_SIZE);
}

void message_header::new_rpc_id()
{
    rpc_id = get_random64();
}

/*static*/ bool message_header::is_right_header(char* hdr)
{
    int32_t crc32 = *(int32_t*)hdr;
    if (crc32 != CRC_INVALID)
    {
        //dassert  (*(int32_t*)data == hdr_crc32, "HeaderCrc must be put at the beginning of the buffer");
        *(int32_t*)hdr = CRC_INVALID;
        bool r = ((uint32_t)crc32 == crc32::compute(hdr, MSG_HDR_SERIALIZED_SIZE, 0));
        *(int32_t*)hdr = crc32;
        return r;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

std::atomic<uint64_t> message::_id(0);

message::message()
{
    _reader = nullptr;
    _writer = new binary_writer();

    memset(&_msg_header, 0, MSG_HDR_SERIALIZED_SIZE);
    _msg_header.hdr_crc32 = _msg_header.body_crc32 = CRC_INVALID;
    _msg_header.local_rpc_code = 0;
    seal(false, true);
}
        
message::message(blob bb, bool parse_hdr)
{
    _reader = new binary_reader(bb);
    _writer = nullptr;

    if (parse_hdr)
    {
        read_header();
        _msg_header.local_rpc_code = 0;
    }
    else
    {
        memset(&_msg_header, 0, MSG_HDR_SERIALIZED_SIZE);
        _msg_header.hdr_crc32 = _msg_header.body_crc32 = CRC_INVALID;
        _msg_header.local_rpc_code = 0;
    }
}
                
message::~message()
{
    if (_reader != nullptr)
    {
        delete _reader;
        _reader = nullptr;
    }

    if (_writer != nullptr)
    {
        delete _writer;
        _writer = nullptr;
    }
}
                
message_ptr message::create_request(task_code rpc_code, int timeout_milliseconds, int hash)
{
    message_ptr msg(new message());
    msg->header().local_rpc_code = (uint16_t)rpc_code;
    msg->header().client.hash = hash;
    if (timeout_milliseconds == 0)
    {
        msg->header().client.timeout_ms = task_spec::get(rpc_code)->rpc_timeout_milliseconds;
    }
    else
    {
        msg->header().client.timeout_ms = timeout_milliseconds;
    }    

    strcpy(msg->header().rpc_name, rpc_code.to_string());

    msg->header().id = message::new_id();
    return msg;
}
        
message_ptr message::create_response()
{
    message_ptr msg(new message());

    msg->header().id = _msg_header.id;
    msg->header().rpc_id = _msg_header.rpc_id;
        
    msg->header().server.error = ERR_OK.get();    
    msg->header().local_rpc_code = task_spec::get(_msg_header.local_rpc_code)->rpc_paired_code;
    
    strcpy(msg->header().rpc_name, _msg_header.rpc_name);
    strcat(msg->header().rpc_name, "_ACK");

    msg->header().from_address = _msg_header.to_address;
    msg->header().to_address = _msg_header.from_address;

    msg->_server_session = _server_session;

    // join point 
    task_spec::get(_msg_header.local_rpc_code)->on_rpc_create_response.execute(this, msg.get());

    return msg;
}

void message::seal(bool fillCrc, bool is_placeholder /*= false*/)
{
    dassert  (!is_read(), "seal can only be applied to write mode messages");
    if (is_placeholder)
    {
        _writer->write_empty(MSG_HDR_SERIALIZED_SIZE);
    }
    else
    {
        header().body_length = total_size() - MSG_HDR_SERIALIZED_SIZE;

        if (fillCrc)
        {
            // compute data crc if necessary
            if (header().body_crc32 == CRC_INVALID)
            {
                std::vector<blob> buffers;
                _writer->get_buffers(buffers);

                buffers[0] = buffers[0].range(MSG_HDR_SERIALIZED_SIZE);

                uint32_t crc32 = 0;
                uint32_t len = 0;
                for (auto it = buffers.begin(); it != buffers.end(); it++)
                {
                    uint32_t lcrc = crc32::compute(it->data(), it->length(), crc32);

                    /*uintxx_t uInitialCrcAB,
                    uintxx_t uInitialCrcA,
                    uintxx_t uFinalCrcA,
                    uint64_t uSizeA,
                    uintxx_t uInitialCrcB,
                    uintxx_t uFinalCrcB,
                    uint64_t uSizeB*/
                    crc32 = crc32::concatenate(
                        0, 
                        0, crc32, len, 
                        crc32, lcrc, it->length()
                        );

                    len += it->length();
                }

                dassert  (len == (uint32_t)header().body_length, "data length is wrong");
                header().body_crc32 = crc32;
            }

            blob bb = _writer->get_first_buffer();
            dassert  (bb.length() >= MSG_HDR_SERIALIZED_SIZE, "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
            header().hdr_crc32 = CRC_INVALID;
            binary_writer writer(bb);
            _msg_header.marshall(writer);

            header().hdr_crc32 = crc32::compute(bb.data(), MSG_HDR_SERIALIZED_SIZE, 0);
            *(uint32_t*)bb.data() = header().hdr_crc32;
        }

        // crc is not enabled
        else
        {
            blob bb = _writer->get_first_buffer();
            dassert  (bb.length() >= MSG_HDR_SERIALIZED_SIZE, "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
            binary_writer writer(bb);
            _msg_header.marshall(writer);
        }
    }
}

bool message::is_right_header() const
{
    dassert  (is_read(), "message must be of read mode");
    if (_msg_header.hdr_crc32 != CRC_INVALID)
    {
        blob bb = _reader->get_buffer();
        return _msg_header.is_right_header((char*)bb.data());
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

bool message::is_right_body() const
{
    dassert  (is_read(), "message must be of read mode");
    if (_msg_header.body_crc32 != CRC_INVALID)
    {
        blob bb = _reader->get_buffer();
        return (uint32_t)_msg_header.body_crc32 == crc32::compute((char*)bb.data() + MSG_HDR_SERIALIZED_SIZE, _msg_header.body_length, 0);
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

void message::read_header()
{
    dassert  (is_read(), "message must be of read mode");
    _msg_header.unmarshall(*_reader);
}

} // end namespace dsn
