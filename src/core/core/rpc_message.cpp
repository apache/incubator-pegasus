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

# include <dsn/internal/ports.h>
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

DSN_API dsn_message_t dsn_msg_create_request(
    dsn_task_code_t rpc_code, 
    int timeout_milliseconds, 
    int request_hash, 
    uint64_t partition_hash
    )
{
    return ::dsn::message_ex::create_request(rpc_code, timeout_milliseconds, request_hash, partition_hash);
}

DSN_API dsn_message_t dsn_msg_copy(dsn_message_t msg)
{
    return msg ? ((::dsn::message_ex*)msg)->copy() : nullptr;
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

DSN_API dsn_address_t dsn_msg_from_address(dsn_message_t msg)
{
    return ((::dsn::message_ex*)msg)->header->from_address.c_addr();
}

DSN_API dsn_address_t dsn_msg_to_address(dsn_message_t msg)
{
    return ((::dsn::message_ex*)msg)->to_address.c_addr();
}

DSN_API uint64_t dsn_msg_rpc_id(dsn_message_t msg)
{
    return ((::dsn::message_ex*)msg)->header->rpc_id;
}

DSN_API dsn_task_code_t dsn_msg_task_code(dsn_message_t msg)
{
    auto msg2 = ((::dsn::message_ex*)msg);
    if (msg2->local_rpc_code != (uint32_t)(-1))
    {
        return msg2->local_rpc_code;
    }
    else
    {
        uint32_t code = 0;
        auto binary_hash = msg2->header->rpc_name_fast.local_hash;
        if (binary_hash == ::dsn::message_ex::s_local_hash && binary_hash != 0)
        {
            code = msg2->header->rpc_name_fast.local_rpc_id;
        }
        else
        {
            code = dsn_task_code_from_string(msg2->header->rpc_name, ::dsn::TASK_CODE_INVALID);
        }

        msg2->local_rpc_code = code;
        return code;
    }
}

DSN_API void dsn_msg_set_options(
    dsn_message_t msg,
    dsn_msg_options_t *opts,
    uint32_t mask // set opt bits using DSN_MSGM_XXX
    )
{
    auto hdr = ((::dsn::message_ex*)msg)->header;

    if (mask & DSN_MSGM_TIMEOUT)
    {
        hdr->client.timeout_ms = opts->timeout_ms;
    }
    
    if (mask & DSN_MSGM_HASH)
    {
        hdr->client.hash = opts->request_hash;
    }
    
    if (mask & DSN_MSGM_VNID)
    {
        hdr->gpid = opts->gpid;
    }

    if (mask & DSN_MSGM_CONTEXT)
    {
        hdr->context = opts->context;
    }
}

DSN_API void dsn_msg_get_options(
    dsn_message_t msg,
    /*out*/ dsn_msg_options_t* opts
    )
{
    auto hdr = ((::dsn::message_ex*)msg)->header;
    opts->context = hdr->context;
    opts->request_hash = hdr->client.hash;
    opts->timeout_ms = hdr->client.timeout_ms;
    opts->gpid = hdr->gpid;
}

namespace dsn {

std::atomic<uint64_t> message_ex::_id(0);
uint32_t message_ex::s_local_hash = 0;

message_ex::message_ex()
{
    _rw_committed = true;
    _rw_index = -1;
    _rw_offset = 0;
    header = nullptr;
    _is_read = false;
    local_rpc_code = ::dsn::TASK_CODE_INVALID;
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

                lcrc = dsn_crc32_compute(ptr, sz, crc32);
                crc32 = dsn_crc32_concatenate(
                    0,
                    0, crc32, len, 
                    crc32, lcrc, sz
                    );

                len += sz;
            }

            dassert  (len == (size_t)header->body_length, "data length is wrong");
            header->body_crc32 = crc32;
        }

        header->hdr_crc32 = CRC_INVALID;
        header->hdr_crc32 = dsn_crc32_compute(header, sizeof(message_header), 0);
    }
    else
    {
#ifndef NDEBUG
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
        bool r = ((uint32_t)crc32 == dsn_crc32_compute(hdr, sizeof(message_header), 0));
        *(int32_t*)hdr = crc32;
        return r;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

bool message_ex::is_right_body(bool is_write_msg) const
{
    if (header->body_crc32 != CRC_INVALID)
    {
        int i_max = (int)buffers.size() - 1;
        uint32_t crc32 = 0;
        size_t len = 0;
        for (int i = 0; i <= i_max; i++)
        {
            uint32_t lcrc;
            const void* ptr;
            size_t sz;

            if (i == 0 && is_write_msg)
            {
                ptr = (const void*)(buffers[i].data() + sizeof(message_header));
                sz = (size_t)buffers[i].length() - sizeof(message_header);
            }
            else
            {
                ptr = (const void*)buffers[i].data();
                sz = (size_t)buffers[i].length();
            }

            lcrc = dsn_crc32_compute(ptr, sz, crc32);
            crc32 = dsn_crc32_concatenate(
                0,
                0, crc32, len,
                crc32, lcrc, sz
                );

            len += sz;
        }

        dassert(len == (size_t)header->body_length, "data length is wrong");
        return header->body_crc32 == crc32;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

message_ex* message_ex::create_receive_message(const blob& data)
{
    message_ex* msg = new message_ex();
    msg->header = (message_header*)data.data();
    msg->_is_read = true;
    // the message_header is hidden ahead of the buffer
    auto data2 = data.range((int)sizeof(message_header));
    msg->buffers.push_back(data2);

    dbg_dassert(msg->header->body_length > 0, "message %s is empty!", msg->header->rpc_name);
    return msg;
}

message_ex* message_ex::create_receive_message_with_standalone_header(const blob& data)
{
    message_ex* msg = new message_ex();
    msg->buffers.push_back(data);
    std::shared_ptr<char> header_holder(static_cast<char*>(dsn_transient_malloc(sizeof(message_header))), [](char* c) {dsn_transient_free(c);});
    msg->header = reinterpret_cast<message_header*>(header_holder.get());
    memset(msg->header, 0, sizeof(message_header));
    msg->buffers.emplace_back(blob(std::move(header_holder), sizeof(message_header)));
    msg->_is_read = true;
    return msg;
}

message_ex* message_ex::copy()
{
    dassert(this->_rw_committed, "should not copy the message when read/write is not committed");

    // ATTENTION:
    // - if this message is a send message, set copied message's write pointer to the end, then you
    //   can continue to append data to the copied message.
    // - if this message is a received message, set copied message's read pointer to the beginning,
    //   then you can read data from the beginning.

    message_ex* msg = new message_ex();
    msg->header = header; // header is within the buffer
    msg->buffers = buffers;
    // TODO(qinzuoyan): should io_session also be copied ?
    msg->to_address = to_address;
    msg->local_rpc_code = local_rpc_code;
    msg->_is_read = _is_read;

    // received message
    if (this->_is_read)
    {
        // leave _rw_index and _rw_offset as initial state, pointing to the beginning of the buffer
    }
    // send message
    else
    {
        msg->server_address = server_address;
        // copy the orignal value, pointing to the end of the buffer
        msg->_rw_index = _rw_index;
        msg->_rw_offset = _rw_offset;
    }

    return msg;
}

message_ex* message_ex::copy_and_prepare_send()
{
    auto copy = this->copy();

    if (_is_read)
    {
        // the message_header is hidden ahead of the buffer, expose it to buffer
        dassert(buffers.size() == 1, "there must be only one buffer for read msg");
        dassert((char*)header + sizeof(message_header) == (char*)buffers[0].data(), "header and content must be contigous");

        copy->buffers[0] = copy->buffers[0].range(-(int)sizeof(message_header));

        // switch the flag
        copy->_is_read = false;
    }

    return copy;
}

message_ex* message_ex::create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int request_hash, uint64_t partition_hash)
{
    message_ex* msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto& hdr = *msg->header;
    memset(&hdr, 0, sizeof(hdr));
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;

    hdr.client.hash = request_hash;

    if (0 == timeout_milliseconds)
    {
        hdr.client.timeout_ms = task_spec::get(rpc_code)->rpc_timeout_milliseconds;
    }
    else
    {
        hdr.client.timeout_ms = timeout_milliseconds;
    }

    task_spec* sp = task_spec::get(rpc_code);
    strncpy(hdr.rpc_name, sp->name.c_str(), sizeof(hdr.rpc_name));
    hdr.rpc_name_fast.local_rpc_id = (uint32_t)rpc_code;
    hdr.rpc_name_fast.local_hash = s_local_hash;

    hdr.id = new_id();

    hdr.context.u.is_request = true;
    if (0 != partition_hash)
    {
        hdr.context.u.parameter_type = MSG_PARAM_PARTITION_HASH;
        hdr.context.u.parameter = partition_hash;
    }

    msg->local_rpc_code = (uint32_t)rpc_code;
    return msg;
}

message_ex* message_ex::create_response()
{
    message_ex* msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto& hdr = *msg->header;
    hdr = *header; // copy request header
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;
    hdr.body_length = 0;
    strncat(hdr.rpc_name, "_ACK", sizeof(hdr.rpc_name));
    hdr.context.u.is_request = false;

    msg->local_rpc_code = task_spec::get(local_rpc_code)->rpc_paired_code;
    hdr.rpc_name_fast.local_rpc_id = msg->local_rpc_code;
    hdr.rpc_name_fast.local_hash = s_local_hash;

    // ATTENTION: the from_address may not be the primary address of this node
    // if there are more than one ports listened and the to_address is not equal to
    // the primary address.
    msg->header->from_address = to_address;
    msg->to_address = header->from_address;
    msg->io_session = io_session;

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
    // printf("%p %s\n", this, __FUNCTION__);
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
    // printf("%p %s\n", this, __FUNCTION__);
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
    // printf("%p %s %d\n", this, __FUNCTION__, utils::get_current_tid());
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
    {
        *ptr = nullptr;
        *size = 0;
        return false;
    }   
}

void message_ex::read_commit(size_t size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_rw_committed, "there are no pending msg read to be committed"
        ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    dassert(-1 != this->_rw_index, "no buffer in curent msg is under read");
    this->_rw_offset += (int)size;
    this->_rw_committed = true;
}

void* message_ex::rw_ptr(size_t offset_begin)
{
    // printf("%p %s\n", this, __FUNCTION__);
    int i_max = (int)this->buffers.size();

    if (!_is_read)
        offset_begin += sizeof(message_header);

    for (int i = 0; i < i_max; i++)
    {
        size_t c_length = (size_t)(this->buffers[i].length());
        if (offset_begin < c_length)
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

} // end namespace dsn
