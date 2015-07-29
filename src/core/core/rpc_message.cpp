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
# include <dsn/internal/network.h>
# include "task_engine.h"
# include "transient_memory.h"

using namespace dsn::utils;

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "message"
#define CRC_INVALID 0xdead0c2c

DSN_API dsn_message_t dsn_msg_create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int hash)
{
    auto msg = ::dsn::message_ex::create_request(rpc_code, timeout_milliseconds, hash);
    return msg;
}

DSN_API void dsn_msg_update_request(dsn_message_t msg, int timeout_milliseconds, int hash)
{
    auto msg2 = (::dsn::message_ex*)msg;
    if (0 != timeout_milliseconds) msg2->header->client.timeout_ms = timeout_milliseconds;
    if (DSN_INVALID_HASH != hash) msg2->header->client.hash = hash;
}

DSN_API void dsn_msg_query_request(dsn_message_t msg, int* ptimeout_milliseconds, int* phash)
{
    auto msg2 = (::dsn::message_ex*)msg;
    if (ptimeout_milliseconds) *ptimeout_milliseconds = msg2->header->client.timeout_ms;
    if (phash) *phash = msg2->header->client.timeout_ms;
}

DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request)
{
    auto msg = ((::dsn::message_ex*)request)->create_response();
    return msg;
}

DSN_API void dsn_msg_write_next(dsn_message_t msg, void** ptr, size_t* size, size_t min_size)
{
    ((::dsn::message_ex*)msg)->write_next(ptr, size, min_size);
}

DSN_API void dsn_msg_write_commit(dsn_message_t msg, size_t size)
{
    ((::dsn::message_ex*)msg)->write_commit(size);
}

DSN_API bool dsn_msg_read_next(dsn_message_t msg, void** ptr, size_t* size)
{
    return ((::dsn::message_ex*)msg)->read_next(ptr, size);
}

DSN_API void dsn_msg_read_commit(dsn_message_t msg, size_t size)
{
    ((::dsn::message_ex*)msg)->read_commit(size);
}

DSN_API size_t dsn_msg_body_size(dsn_message_t msg)
{
    return ((::dsn::message_ex*)msg)->body_size();
}

DSN_API void* dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin)
{
    return ((::dsn::message_ex*)msg)->rw_ptr(offset_begin);
}

DSN_API void dsn_msg_add_ref(dsn_message_t msg)
{
    ((::dsn::message_ex*)msg)->add_ref();
}

DSN_API void dsn_msg_release_ref(dsn_message_t msg)
{
    ((::dsn::message_ex*)msg)->release_ref();
}

DSN_API void dsn_msg_from_address(dsn_message_t msg, /*out*/ dsn_address_t* ep)
{
    *ep = ((::dsn::message_ex*)msg)->from_address;
}

DSN_API void dsn_msg_to_address(dsn_message_t msg, /*out*/ dsn_address_t* ep)
{
    *ep = ((::dsn::message_ex*)msg)->to_address;
}

namespace dsn {

std::atomic<uint64_t> message_ex::_id(0);

message_ex::message_ex()
{
    _rw_committed = true;
    _rw_index = -1;
    _rw_offset = 0;
    header = nullptr;
    _is_read = false;
}

message_ex::~message_ex()
{
    if (!_is_read)
    {
        dassert(_rw_committed, "message write is not committed");
    }
}

void message_ex::seal(bool fill_crc)
{
    dassert  (!_is_read && _rw_committed, "seal can only be applied to write mode messages");
    dbg_dassert(header->body_length > 0, "message %s is empty!", header->rpc_name);

    if (fill_crc)
    {
        // compute data crc if necessary
        if (header->body_crc32 == CRC_INVALID)
        {
            int i_max = (int)buffers.size() - 1;
            uint32_t crc32 = 0;
            size_t len = 0;
            for (int i = 0; i <= i_max; i++)
            {
                uint32_t lcrc;
                const void* ptr;
                size_t sz;

                if (i == 0)
                {
                    ptr = (const void*)(buffers[i].data() + sizeof(message_header));
                    sz = (size_t)buffers[i].length() - sizeof(message_header);
                }
                else
                {
                    ptr = (const void*)buffers[i].data();
                    sz = (size_t)buffers[i].length();
                }

                lcrc = dsn_crc32_compute(ptr, sz);
                crc32 = dsn_crc32_concatenate(
                    0, crc32, len, 
                    crc32, lcrc, sz
                    );

                len += sz;
            }

            dassert  (len == (size_t)header->body_length, "data length is wrong");
            header->body_crc32 = crc32;
        }

        header->hdr_crc32 = CRC_INVALID;
        header->hdr_crc32 = dsn_crc32_compute(header, sizeof(message_header));
    }
    else
    {
#ifdef _DEBUG
        int i_max = (int)buffers.size() - 1;
        size_t len = 0;
        for (int i = 0; i <= i_max; i++)
        {
            len += (size_t)buffers[i].length();
        }
        dassert(len == (size_t)header->body_length + sizeof(message_header), 
            "data length is wrong");
#endif
    }
}

bool message_ex::is_right_header() const
{
    if (header->hdr_crc32 != CRC_INVALID)
    {
        return is_right_header((char*)header);
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

/*static*/ bool message_ex::is_right_header(char* hdr)
{
    int32_t crc32 = *(int32_t*)hdr;
    if (crc32 != CRC_INVALID)
    {
        //dassert  (*(int32_t*)data == hdr_crc32, "HeaderCrc must be put at the beginning of the buffer");
        *(int32_t*)hdr = CRC_INVALID;
        bool r = ((uint32_t)crc32 == dsn_crc32_compute(hdr, sizeof(message_header)));
        *(int32_t*)hdr = crc32;
        return r;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

bool message_ex::is_right_body() const
{
    if (header->body_crc32 != CRC_INVALID)
    {
        //return (uint32_t)header->body_crc32 == crc32::compute((char*)bb.data() + sizeof(message_header), _msg.hdr.body_length, 0);
        dassert(false, "TODO");
        return true;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

message_ex* message_ex::create_receive_message(blob& data)
{
    message_ex* msg = new message_ex();
    msg->header = (message_header*)data.data();
    msg->_is_read = true;
    data = data.range((int)sizeof(message_header));
    msg->buffers.push_back(data);

    dbg_dassert(msg->header->body_length > 0, "message %s is empty!", msg->header->rpc_name);
    return msg;
}

message_ex* message_ex::create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int hash)
{
    message_ex* msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto& hdr = *msg->header;
    memset(&hdr, 0, sizeof(hdr));
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;    
    
    if (DSN_INVALID_HASH != hash) 
        hdr.client.hash = hash;

    if (0 == timeout_milliseconds)
    {
        hdr.client.timeout_ms = task_spec::get(rpc_code)->rpc_timeout_milliseconds;
    }
    else
    {
        hdr.client.timeout_ms = timeout_milliseconds;
    }

    strncpy(hdr.rpc_name, dsn_task_code_to_string(rpc_code), sizeof(hdr.rpc_name));

    hdr.id = new_id();

    msg->local_rpc_code = (uint16_t)rpc_code;
    msg->from_address.name[0] = '\0';
    msg->from_address.port = 0;
    msg->to_address.name[0] = '\0';
    msg->to_address.port = 0;

    return msg;
}

message_ex* message_ex::create_response()
{
    message_ex* msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto& hdr = *msg->header;
    memset(&hdr, 0, sizeof(hdr));
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;
    hdr.id = header->id;
    hdr.rpc_id = header->rpc_id;
    strncpy(hdr.rpc_name, header->rpc_name, sizeof(hdr.rpc_name));
    strncat(hdr.rpc_name, "_ACK", sizeof(hdr.rpc_name));

    msg->local_rpc_code = task_spec::get(local_rpc_code)->rpc_paired_code;
    msg->from_address = to_address;
    msg->to_address = from_address;
    msg->server_session = server_session;

    // join point 
    task_spec::get(local_rpc_code)->on_rpc_create_response.execute(this, msg);

    return msg;
}

void message_ex::prepare_buffer_header()
{
    void* ptr;
    size_t size;
    ::dsn::tls_trans_mem_next(&ptr, &size, sizeof(message_header));

    ::dsn::blob buffer(
        (*::dsn::tls_trans_memory.block),
        (int)((char*)(ptr) - ::dsn::tls_trans_memory.block->get()),
        (int)sizeof(message_header)
        );
    this->_rw_index = 0;
    this->_rw_offset = (int)sizeof(message_header);
    this->buffers.push_back(buffer);

    ::dsn::tls_trans_mem_commit(sizeof(message_header));
    
    header = (message_header*)ptr;
}

void message_ex::write_next(void** ptr, size_t* size, size_t min_size)
{
    printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_is_read && this->_rw_committed, "there are pending msg write not committed"
        ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");
    ::dsn::tls_trans_mem_next(ptr, size, min_size);
    this->_rw_committed = false;

    // optimization
    if (this->_rw_index >= 0)
    {
        ::dsn::blob& lbb = *this->buffers.rbegin();

        // if the current allocation is within the same buffer with the previous one
        if (*ptr == lbb.data() + lbb.length()
            && ::dsn::tls_trans_memory.block->get() == lbb.buffer_ptr())
        {
            lbb.assign(
                *::dsn::tls_trans_memory.block,
                (int)((char*)(*ptr) - ::dsn::tls_trans_memory.block->get() - lbb.length()),
                (int)(lbb.length() + *size)
                );

            return;
        }
    }

    ::dsn::blob buffer(
        (*::dsn::tls_trans_memory.block),
        (int)((char*)(*ptr) - ::dsn::tls_trans_memory.block->get()),
        (int)(*size)
        );
    this->_rw_index++;
    this->_rw_offset = 0;
    this->buffers.push_back(buffer);

    dassert(this->_rw_index + 1 == (int)this->buffers.size(), "message write buffer count is not right");
}

void message_ex::write_commit(size_t size)
{
    printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_rw_committed, "there are no pending msg write to be committed"
        ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");

    ::dsn::tls_trans_mem_commit(size);

    this->_rw_offset += (int)size;
    *this->buffers.rbegin() = this->buffers.rbegin()->range(0, (int)this->_rw_offset);
    this->_rw_committed = true;
    this->header->body_length += (int)size;
}

bool message_ex::read_next(void** ptr, size_t* size)
{
    printf("%p %s %d\n", this, __FUNCTION__, utils::get_current_tid());
    dassert(this->_is_read && this->_rw_committed, "there are pending msg read not committed"
        ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    int idx = this->_rw_index;
    if (-1 == idx ||
        this->_rw_offset == this->buffers[idx].length())
    {
        idx = ++this->_rw_index;
        this->_rw_offset = 0;
    }

    if (idx < (int)this->buffers.size())
    {
        this->_rw_committed = false;
        *ptr = (void*)(this->buffers[idx].data() + this->_rw_offset);
        *size = (size_t)this->buffers[idx].length() - this->_rw_offset;
        return true;
    }
    else
        return false;
}

void message_ex::read_commit(size_t size)
{
    printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_rw_committed, "there are no pending msg read to be committed"
        ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    dassert(-1 != this->_rw_index, "no buffer in curent msg is under read");
    this->_rw_offset += (int)size;
    this->_rw_committed = true;
}

void* message_ex::rw_ptr(size_t offset_begin)
{
    printf("%p %s\n", this, __FUNCTION__);
    int i_max = (int)this->buffers.size();

    if (!_is_read)
        offset_begin += sizeof(message_header);

    for (int i = 0; i < i_max; i++)
    {
        size_t c_length = (size_t)(this->buffers[i].length());
        if (offset_begin <= c_length)
        {
            return (void*)(this->buffers[i].data() + offset_begin);
        }
        else
        {
            offset_begin -= c_length;
        }
    }
    return nullptr;
}

/////////////////////////////////////////////////

//void dsn_message_header_helper::marshall(message_header* hdr, binary_writer& writer)
//{
//    writer.write((const char*)hdr, sizeof(message_header));
//}
//
//void dsn_message_header_helper::unmarshall(message_header* hdr, binary_reader& reader)
//{
//    reader.read((char*)hdr, sizeof(message_header));
//}


//
//std::atomic<uint64_t> message::_id(0);
//
//message::message()
//{
//    _reader = nullptr;
//    _writer = new binary_writer();
//
//    memset(&_msg, 0, sizeof(_msg));
//    _msg.hdr.hdr_crc32 = _msg.hdr.body_crc32 = CRC_INVALID;
//    seal(false, true);
//}
//        
//message::message(blob bb, bool parse_hdr)
//{
//    _reader = new binary_reader(bb);
//    _writer = nullptr;
//
//    if (parse_hdr)
//    {
//        read_header();
//        _msg.hdr.local_rpc_code = 0;
//    }
//    else
//    {
//        memset(&_msg, 0, sizeof(_msg));
//        _msg.hdr.hdr_crc32 = _msg.hdr.body_crc32 = CRC_INVALID;
//    }
//}
//                
//message::~message()
//{
//    if (_reader != nullptr)
//    {
//        delete _reader;
//        _reader = nullptr;
//    }
//
//    if (_writer != nullptr)
//    {
//        delete _writer;
//        _writer = nullptr;
//    }
//}
//                
//dsn_message_t dsn_msg_create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int hash)
//{
//    dsn_message_t msg(new message());
//    auto& hdr = msg->header();
//    hdr.local_rpc_code = (uint16_t)rpc_code;
//    hdr.client.hash = hash;
//    if (timeout_milliseconds == 0)
//    {
//        hdr.client.timeout_ms = task_spec::get(rpc_code)->rpc_timeout_milliseconds;
//    }
//    else
//    {
//        hdr.client.timeout_ms = timeout_milliseconds;
//    }    
//
//    strncpy(hdr.rpc_name, dsn_task_code_to_string(rpc_code), sizeof(hdr.rpc_name));
//
//    hdr.id = message::new_id();
//    
//    hdr.from_address.name[0] = '\0';
//    hdr.from_address.port = 0; 
//    hdr.to_address.name[0] = '\0';    
//    hdr.to_address.port = 0;
//
//    return msg;
//}
//        
//dsn_message_t message::create_response()
//{
//    dsn_message_t msg(new message());
//    auto& hdr = msg->header();
//
//    hdr.id = _msg.hdr.id;
//    hdr.rpc_id = _msg.hdr.rpc_id;
//        
//    hdr.server.error = ERR_OK.get();
//    hdr.local_rpc_code = task_spec::get(_msg.hdr.local_rpc_code)->rpc_paired_code;
//    
//    strncpy(hdr.rpc_name, _msg.hdr.rpc_name, sizeof(hdr.rpc_name));
//    strncat(hdr.rpc_name, "_ACK", sizeof(hdr.rpc_name));
//
//    hdr.from_address = _msg.hdr.to_address;
//    hdr.to_address = _msg.hdr.from_address;
//
//    msg->_server_session = _server_session;
//
//    // join point 
//    task_spec::get(_msg.hdr.local_rpc_code)->on_rpc_create_response.execute(this, msg.get());
//
//    return msg;
//}
//
//void message::seal(bool fillCrc, bool is_placeholder /*= false*/)
//{
//    dassert  (!is_read(), "seal can only be applied to write mode messages");
//    if (is_placeholder)
//    {
//        _writer->write_empty(sizeof(message_header));
//    }
//    else
//    {
//        header->body_length = total_size() - sizeof(message_header);
//
//        if (fillCrc)
//        {
//            // compute data crc if necessary
//            if (header->body_crc32 == CRC_INVALID)
//            {
//                std::vector<blob> buffers;
//                _writer->get_buffers(buffers);
//
//                buffers[0] = buffers[0].range(sizeof(message_header));
//
//                uint32_t crc32 = 0;
//                uint32_t len = 0;
//                for (auto it = buffers.begin(); it != buffers.end(); it++)
//                {
//                    uint32_t lcrc = crc32::compute(it->data(), it->length(), crc32);
//
//                    /*uintxx_t uInitialCrcAB,
//                    uintxx_t uInitialCrcA,
//                    uintxx_t uFinalCrcA,
//                    uint64_t uSizeA,
//                    uintxx_t uInitialCrcB,
//                    uintxx_t uFinalCrcB,
//                    uint64_t uSizeB*/
//                    crc32 = crc32::concatenate(
//                        0, 
//                        0, crc32, len, 
//                        crc32, lcrc, it->length()
//                        );
//
//                    len += it->length();
//                }
//
//                dassert  (len == (uint32_t)header->body_length, "data length is wrong");
//                header->body_crc32 = crc32;
//            }
//
//            blob bb = _writer->get_first_buffer();
//            dassert  (bb.length() >= sizeof(message_header), "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
//            header->hdr_crc32 = CRC_INVALID;
//            binary_writer writer(bb);
//            dsn_message_header_helper::marshall(&_msg.hdr, writer);
//
//            header->hdr_crc32 = crc32::compute(bb.data(), sizeof(message_header), 0);
//            *(uint32_t*)bb.data() = header->hdr_crc32;
//        }
//
//        // crc is not enabled
//        else
//        {
//            blob bb = _writer->get_first_buffer();
//            dassert  (bb.length() >= sizeof(message_header), "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
//            binary_writer writer(bb);
//            dsn_message_header_helper::marshall(&_msg.hdr, writer);
//        }
//    }
//}
//
//bool message::is_right_header() const
//{
//    dassert  (is_read(), "message must be of read mode");
//    if (_msg.hdr.hdr_crc32 != CRC_INVALID)
//    {
//        blob bb = _reader->get_buffer();
//        return dsn_message_header_helper::is_right_header((char*)bb.data());
//    }
//
//    // crc is not enabled
//    else
//    {
//        return true;
//    }
//}
//
//bool message::is_right_body() const
//{
//    dassert  (is_read(), "message must be of read mode");
//    if (_msg.hdr.body_crc32 != CRC_INVALID)
//    {
//        blob bb = _reader->get_buffer();
//        return (uint32_t)_msg.hdr.body_crc32 == crc32::compute((char*)bb.data() + sizeof(message_header), _msg.hdr.body_length, 0);
//    }
//
//    // crc is not enabled
//    else
//    {
//        return true;
//    }
//}
//
//void message::read_header()
//{
//    dassert  (is_read(), "message must be of read mode");
//    dsn_message_header_helper::unmarshall(&_msg.hdr, *_reader);
//}

} // end namespace dsn
