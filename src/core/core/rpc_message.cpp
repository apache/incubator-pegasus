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

#include <dsn/utility/ports.h>
#include <dsn/utility/crc.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/tool-api/network.h>
#include <dsn/tool-api/message_parser.h>
#include <cctype>

#include "task_engine.h"
#include "transient_memory.h"

using namespace dsn::utils;

DSN_API dsn_message_t dsn_msg_create_request(dsn::task_code rpc_code,
                                             int timeout_milliseconds,
                                             int thread_hash,
                                             uint64_t partition_hash)
{
    return ::dsn::message_ex::create_request(
        rpc_code, timeout_milliseconds, thread_hash, partition_hash);
}

DSN_API dsn_message_t dsn_msg_create_received_request(dsn::task_code rpc_code,
                                                      dsn_msg_serialize_format serialization_type,
                                                      void *buffer,
                                                      int size,
                                                      int thread_hash,
                                                      uint64_t partition_hash)
{
    ::dsn::blob bb((const char *)buffer, 0, size);
    auto msg = ::dsn::message_ex::create_receive_message_with_standalone_header(bb);
    msg->local_rpc_code = rpc_code;
    const char *name = rpc_code.to_string();
    strncpy(msg->header->rpc_name, name, strlen(name));

    msg->header->client.thread_hash = thread_hash;
    msg->header->client.partition_hash = partition_hash;
    msg->header->context.u.serialize_format = serialization_type;
    msg->add_ref(); // released by callers explicitly using dsn_msg_release
    return msg;
}

DSN_API dsn_message_t dsn_msg_copy(dsn_message_t msg, bool clone_content, bool copy_for_receive)
{
    return msg ? ((::dsn::message_ex *)msg)->copy(clone_content, copy_for_receive) : nullptr;
}

DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request)
{
    auto msg = ((::dsn::message_ex *)request)->create_response();
    return msg;
}

DSN_API void dsn_msg_write_next(dsn_message_t msg, void **ptr, size_t *size, size_t min_size)
{
    ((::dsn::message_ex *)msg)->write_next(ptr, size, min_size);
}

DSN_API void dsn_msg_write_commit(dsn_message_t msg, size_t size)
{
    ((::dsn::message_ex *)msg)->write_commit(size);
}

DSN_API bool dsn_msg_read_next(dsn_message_t msg, void **ptr, size_t *size)
{
    return ((::dsn::message_ex *)msg)->read_next(ptr, size);
}

DSN_API void dsn_msg_read_commit(dsn_message_t msg, size_t size)
{
    ((::dsn::message_ex *)msg)->read_commit(size);
}

DSN_API size_t dsn_msg_body_size(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->body_size();
}

DSN_API void *dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin)
{
    return ((::dsn::message_ex *)msg)->rw_ptr(offset_begin);
}

DSN_API void dsn_msg_add_ref(dsn_message_t msg) { ((::dsn::message_ex *)msg)->add_ref(); }

DSN_API void dsn_msg_release_ref(dsn_message_t msg) { ((::dsn::message_ex *)msg)->release_ref(); }

DSN_API dsn_address_t dsn_msg_from_address(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->header->from_address.c_addr();
}

DSN_API dsn_address_t dsn_msg_to_address(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->to_address.c_addr();
}

DSN_API uint64_t dsn_msg_trace_id(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->header->trace_id;
}

DSN_API dsn::task_code dsn_msg_task_code(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->rpc_code();
}

DSN_API const char *dsn_msg_rpc_name(dsn_message_t msg)
{
    return ((::dsn::message_ex *)msg)->header->rpc_name;
}

DSN_API void dsn_msg_set_options(dsn_message_t msg,
                                 dsn_msg_options_t *opts,
                                 uint32_t mask // set opt bits using DSN_MSGM_XXX
                                 )
{
    auto hdr = ((::dsn::message_ex *)msg)->header;

    if (mask & DSN_MSGM_TIMEOUT) {
        hdr->client.timeout_ms = opts->timeout_ms;
    }

    if (mask & DSN_MSGM_THREAD_HASH) {
        hdr->client.thread_hash = opts->thread_hash;
    }

    if (mask & DSN_MSGM_PARTITION_HASH) {
        hdr->client.partition_hash = opts->partition_hash;
    }

    if (mask & DSN_MSGM_VNID) {
        hdr->gpid = opts->gpid;
    }

    if (mask & DSN_MSGM_CONTEXT) {
        hdr->context = opts->context;
    }
}

DSN_API dsn_msg_serialize_format dsn_msg_get_serialize_format(dsn_message_t msg)
{
    auto hdr = ((::dsn::message_ex *)msg)->header;
    return static_cast<dsn_msg_serialize_format>(hdr->context.u.serialize_format);
}

DSN_API void dsn_msg_set_serailize_format(dsn_message_t msg, dsn_msg_serialize_format fmt)
{
    auto hdr = ((::dsn::message_ex *)msg)->header;
    hdr->context.u.serialize_format = fmt;
}

DSN_API void dsn_msg_get_options(dsn_message_t msg,
                                 /*out*/ dsn_msg_options_t *opts)
{
    auto hdr = ((::dsn::message_ex *)msg)->header;
    opts->timeout_ms = hdr->client.timeout_ms;
    opts->thread_hash = hdr->client.thread_hash;
    opts->partition_hash = hdr->client.partition_hash;
    opts->gpid = hdr->gpid;
    opts->context = hdr->context;
}

namespace dsn {

std::atomic<uint64_t> message_ex::_id(0);
uint32_t message_ex::s_local_hash = 0;

message_ex::message_ex()
    : header(nullptr),
      local_rpc_code(::dsn::TASK_CODE_INVALID),
      hdr_format(NET_HDR_INVALID),
      send_retry_count(0),
      _rw_index(-1),
      _rw_offset(0),
      _rw_committed(true),
      _is_read(false)
{
}

message_ex::~message_ex()
{
    if (!_is_read) {
        dassert(_rw_committed, "message write is not committed");
    }
}

error_code message_ex::error()
{
    dsn::error_code code;
    auto binary_hash = header->server.error_code.local_hash;
    if (binary_hash != 0 && binary_hash == ::dsn::message_ex::s_local_hash) {
        code = dsn::error_code(header->server.error_code.local_code);
    } else {
        code = error_code::try_get(header->server.error_name, dsn::ERR_UNKNOWN);
        header->server.error_code.local_hash = ::dsn::message_ex::s_local_hash;
        header->server.error_code.local_code = code;
    }
    return code;
}

task_code message_ex::rpc_code()
{
    if (local_rpc_code != ::dsn::TASK_CODE_INVALID) {
        return local_rpc_code;
    }

    auto binary_hash = header->rpc_code.local_hash;
    if (binary_hash != 0 && binary_hash == ::dsn::message_ex::s_local_hash) {
        local_rpc_code = dsn::task_code(header->rpc_code.local_code);
    } else {
        local_rpc_code = dsn::task_code::try_get(header->rpc_name, ::dsn::TASK_CODE_INVALID);
        header->rpc_code.local_hash = ::dsn::message_ex::s_local_hash;
        header->rpc_code.local_code = local_rpc_code.code();
    }

    return local_rpc_code;
}

message_ex *message_ex::create_receive_message(const blob &data)
{
    message_ex *msg = new message_ex();
    msg->header = (message_header *)data.data();
    msg->_is_read = true;
    // the message_header is hidden ahead of the buffer
    auto data2 = data.range((int)sizeof(message_header));
    msg->buffers.push_back(data2);

    // dbg_dassert(msg->header->body_length > 0, "message %s is empty!", msg->header->rpc_name);
    return msg;
}

message_ex *message_ex::create_receive_message_with_standalone_header(const blob &data)
{
    message_ex *msg = new message_ex();
    std::shared_ptr<char> header_holder(
        static_cast<char *>(dsn_transient_malloc(sizeof(message_header))),
        [](char *c) { dsn_transient_free(c); });
    msg->header = reinterpret_cast<message_header *>(header_holder.get());
    memset(msg->header, 0, sizeof(message_header));
    msg->buffers.emplace_back(blob(std::move(header_holder), sizeof(message_header)));
    msg->buffers.push_back(data);

    msg->header->body_length = data.length();
    msg->_is_read = true;
    // we skip the message header
    msg->_rw_index = 1;

    return msg;
}

message_ex *message_ex::copy(bool clone_content, bool copy_for_receive)
{
    dassert(this->_rw_committed, "should not copy the message when read/write is not committed");

    // ATTENTION:
    // - if this message is a written message, set copied message's write pointer to the end, then
    // you
    //   can continue to append data to the copied message.
    // - if this message is a read message, set copied message's read pointer to the beginning,
    //   then you can read data from the beginning.
    // - if copy_for_receive is set, it means that we want to make a receiving message from a
    // sending message.
    //   which is usually useful when you want to write mock for modules which use rpc.

    message_ex *msg = new message_ex();
    msg->to_address = to_address;
    msg->local_rpc_code = local_rpc_code;
    msg->hdr_format = hdr_format;

    if (!copy_for_receive)
        msg->_is_read = _is_read;
    else
        msg->_is_read = true;

    // received message
    if (msg->_is_read) {
        // leave _rw_index and _rw_offset as initial state, pointing to the beginning of the buffer
    }
    // send message
    else {
        msg->server_address = server_address;
        // copy the orignal value, pointing to the end of the buffer
        msg->_rw_index = _rw_index;
        msg->_rw_offset = _rw_offset;
    }

    if (!clone_content) {
        msg->header = header; // header is within the buffer
        msg->buffers = buffers;
    } else {
        int total_length = body_size() + sizeof(dsn::message_header);
        std::shared_ptr<char> recv_buffer(dsn::utils::make_shared_array<char>(total_length));
        char *ptr = recv_buffer.get();
        int i = 0;

        if ((const char *)header != buffers[0].data()) {
            memcpy(ptr, (const void *)header, sizeof(message_header));
            ptr += sizeof(message_header);
        }

        for (dsn::blob &bb : buffers) {
            memcpy(ptr, bb.data(), bb.length());
            i += bb.length();
            ptr += bb.length();
        }
        dassert(
            i == total_length, "%d VS %d, rpc_name = %s", i, total_length, msg->header->rpc_name);

        auto data = dsn::blob(recv_buffer, total_length);

        msg->header = (message_header *)data.data();
        if (msg->_is_read)
            msg->buffers.push_back(data.range((int)sizeof(message_header)));
        else
            msg->buffers.push_back(data);
    }
    return msg;
}

message_ex *message_ex::copy_and_prepare_send(bool clone_content)
{
    auto copy = this->copy(clone_content, false);

    if (_is_read) {
        // the message_header is hidden ahead of the buffer, expose it to buffer
        dassert(buffers.size() == 1, "there must be only one buffer for read msg");
        dassert((char *)header + sizeof(message_header) == (char *)buffers[0].data(),
                "header and content must be contigous");

        copy->buffers[0] = copy->buffers[0].range(-(int)sizeof(message_header));

        // switch the flag
        copy->_is_read = false;
    }

    return copy;
}

message_ex *message_ex::create_request(dsn::task_code rpc_code,
                                       int timeout_milliseconds,
                                       int thread_hash,
                                       uint64_t partition_hash)
{
    message_ex *msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto &hdr = *msg->header;
    memset(&hdr, 0, sizeof(hdr));
    hdr.hdr_type = *(uint32_t *)"RDSN";
    hdr.hdr_length = sizeof(message_header);
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;

    // if thread_hash == 0 && partition_hash != 0,
    // thread_hash is computed from partition_hash in rpc_engine
    hdr.client.thread_hash = thread_hash;
    hdr.client.partition_hash = partition_hash;

    task_spec *sp = task_spec::get(rpc_code);
    if (0 == timeout_milliseconds) {
        hdr.client.timeout_ms = sp->rpc_timeout_milliseconds;
    } else {
        hdr.client.timeout_ms = timeout_milliseconds;
    }

    msg->local_rpc_code = rpc_code;
    strncpy(hdr.rpc_name, sp->name.c_str(), sizeof(hdr.rpc_name));
    hdr.rpc_code.local_code = (uint32_t)rpc_code;
    hdr.rpc_code.local_hash = s_local_hash;

    hdr.id = new_id();

    hdr.context.u.is_request = true;
    hdr.context.u.serialize_format = sp->rpc_msg_payload_serialize_default_format;
    hdr.context.u.is_forward_supported = true;

    msg->hdr_format = sp->rpc_call_header_format;

    return msg;
}

message_ex *message_ex::create_response()
{
    message_ex *msg = new message_ex();
    msg->_is_read = false;
    msg->prepare_buffer_header();

    // init header
    auto &hdr = *msg->header;
    hdr = *header; // copy request header
    hdr.hdr_crc32 = hdr.body_crc32 = CRC_INVALID;
    hdr.body_length = 0;
    hdr.context.u.is_request = false;

    task_spec *request_sp = task_spec::get(local_rpc_code);
    task_spec *response_sp = task_spec::get(request_sp->rpc_paired_code);
    msg->local_rpc_code = response_sp->code;
    strncpy(hdr.rpc_name, response_sp->name.c_str(), sizeof(hdr.rpc_name));
    hdr.rpc_code.local_code = msg->local_rpc_code;
    hdr.rpc_code.local_hash = s_local_hash;

    // ATTENTION: the from_address may not be the primary address of this node
    // if there are more than one ports listened and the to_address is not equal to
    // the primary address.
    msg->header->from_address = to_address;
    msg->to_address = header->from_address;
    msg->io_session = io_session;
    msg->hdr_format = hdr_format;

    // join point
    request_sp->on_rpc_create_response.execute(this, msg);

    return msg;
}

void message_ex::prepare_buffer_header()
{
    void *ptr;
    size_t size;
    ::dsn::tls_trans_mem_next(&ptr, &size, sizeof(message_header));

    ::dsn::blob buffer((*::dsn::tls_trans_memory.block),
                       (int)((char *)(ptr) - ::dsn::tls_trans_memory.block->get()),
                       (int)sizeof(message_header));

    ::dsn::tls_trans_mem_commit(sizeof(message_header));

    this->_rw_index = 0;
    this->_rw_offset = (int)sizeof(message_header);
    this->buffers.push_back(buffer);

    header = (message_header *)ptr;
}

void message_ex::write_next(void **ptr, size_t *size, size_t min_size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_is_read && this->_rw_committed,
            "there are pending msg write not committed"
            ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");
    ::dsn::tls_trans_mem_next(ptr, size, min_size);
    this->_rw_committed = false;

    // optimization
    if (this->_rw_index >= 0) {
        auto &lbb = *this->buffers.rbegin();

        // if the current allocation is within the same buffer with the previous one
        if (*ptr == lbb.data() + lbb.length() &&
            ::dsn::tls_trans_memory.block->get() == lbb.buffer_ptr()) {
            lbb.assign(*::dsn::tls_trans_memory.block,
                       (int)((char *)(*ptr) - ::dsn::tls_trans_memory.block->get() - lbb.length()),
                       (int)(lbb.length() + *size));

            return;
        }
    }

    ::dsn::blob buffer((*::dsn::tls_trans_memory.block),
                       (int)((char *)(*ptr) - ::dsn::tls_trans_memory.block->get()),
                       (int)(*size));
    this->_rw_index++;
    this->_rw_offset = 0;
    this->buffers.push_back(buffer);

    dassert(this->_rw_index + 1 == (int)this->buffers.size(),
            "message write buffer count is not right");
}

void message_ex::write_commit(size_t size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_rw_committed,
            "there are no pending msg write to be committed"
            ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");

    ::dsn::tls_trans_mem_commit(size);

    this->_rw_offset += (int)size;
    *this->buffers.rbegin() = this->buffers.rbegin()->range(0, (int)this->_rw_offset);
    this->_rw_committed = true;
    this->header->body_length += (int)size;
}

void message_ex::write_append(const blob &data)
{
    // printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_is_read && this->_rw_committed,
            "there are pending msg write not committed"
            ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");

    int size = data.length();
    if (size > 0) {
        this->_rw_index++;
        this->_rw_offset += size;
        this->buffers.push_back(data);
        this->header->body_length += size;
    }
}

bool message_ex::read_next(void **ptr, size_t *size)
{
    // printf("%p %s %d\n", this, __FUNCTION__, utils::get_current_tid());
    dassert(this->_is_read && this->_rw_committed,
            "there are pending msg read not committed"
            ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    int idx = this->_rw_index;
    if (-1 == idx || this->_rw_offset == static_cast<int>(this->buffers[idx].length())) {
        idx = ++this->_rw_index;
        this->_rw_offset = 0;
    }

    if (idx < (int)this->buffers.size()) {
        this->_rw_committed = false;
        *ptr = (void *)(this->buffers[idx].data() + this->_rw_offset);
        *size = (size_t)this->buffers[idx].length() - this->_rw_offset;
        return true;
    } else {
        *ptr = nullptr;
        *size = 0;
        return false;
    }
}

void message_ex::read_commit(size_t size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    dassert(!this->_rw_committed,
            "there are no pending msg read to be committed"
            ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    dassert(-1 != this->_rw_index, "no buffer in curent msg is under read");
    this->_rw_offset += (int)size;
    this->_rw_committed = true;
}

void *message_ex::rw_ptr(size_t offset_begin)
{
    // printf("%p %s\n", this, __FUNCTION__);
    int i_max = (int)this->buffers.size();

    if (!_is_read)
        offset_begin += sizeof(message_header);

    for (int i = 0; i < i_max; i++) {
        size_t c_length = (size_t)(this->buffers[i].length());
        if (offset_begin < c_length) {
            return (void *)(this->buffers[i].data() + offset_begin);
        } else {
            offset_begin -= c_length;
        }
    }
    return nullptr;
}

} // end namespace dsn
