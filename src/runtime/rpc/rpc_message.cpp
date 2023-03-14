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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <string.h>
#include <algorithm>
#include <list>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "network.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/crc.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/singleton.h"
#include "utils/utils.h"

using namespace dsn::utils;

namespace dsn {
// init common for all per-node providers
DSN_DEFINE_uint32(core,
                  local_hash,
                  0,
                  "a same hash value from two processes indicate the rpc codes are registered in "
                  "the same order, and therefore the mapping between rpc code string and integer "
                  "is the same, which we leverage for fast rpc handler lookup optimization");

std::atomic<uint64_t> message_ex::_id(0);

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
    // coz message_header's memory is managed by vector "buffers", so its memory will be released
    // after blobs in "buffers" are free.
    //
    // however, the message_header's object is constructed with placement new
    // in prepare_buffer_header, so the destructor won't be called automatically with the
    // "free of blobs in buffers".
    //
    // strictly speaking, we should call release_header_buffer to trigger message_header's
    // destructor, but we can't do this as the message_header may be shared with other
    // rpc_message objects if you call "copy_and_prepare_send".
    //
    // so here we simply skip the release_header_buffer. Notice this won't lead to any
    // memory leak problem as the header's destructor is trival:
    //      gpid -> we can treat it as POD type
    //      rpc_address -> only ipv4, we can treat it as POD type
    //
    // Please refer to comments on message_header's definition for details

    // release_header_buffer();
    if (!_is_read) {
        CHECK(_rw_committed, "message write is not committed");
    }
}

error_code message_ex::error()
{
    dsn::error_code code;
    auto binary_hash = header->server.error_code.local_hash;
    if (binary_hash != 0 && binary_hash == FLAGS_local_hash) {
        code = dsn::error_code(header->server.error_code.local_code);
    } else {
        code = error_code::try_get(header->server.error_name, dsn::ERR_UNKNOWN);
        header->server.error_code.local_hash = FLAGS_local_hash;
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
    if (binary_hash != 0 && binary_hash == FLAGS_local_hash) {
        local_rpc_code = dsn::task_code(header->rpc_code.local_code);
    } else {
        local_rpc_code = dsn::task_code::try_get(header->rpc_name, ::dsn::TASK_CODE_INVALID);
        header->rpc_code.local_hash = FLAGS_local_hash;
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

    return msg;
}

message_ex *message_ex::create_received_request(dsn::task_code code,
                                                dsn_msg_serialize_format format,
                                                void *buffer,
                                                int size,
                                                int thread_hash,
                                                uint64_t partition_hash)
{
    ::dsn::blob bb((const char *)buffer, 0, size);
    auto msg = ::dsn::message_ex::create_receive_message_with_standalone_header(bb);
    msg->local_rpc_code = code;
    const char *name = code.to_string();
    strncpy(msg->header->rpc_name, name, sizeof(msg->header->rpc_name) - 1);
    msg->header->rpc_name[sizeof(msg->header->rpc_name) - 1] = '\0';

    msg->header->client.thread_hash = thread_hash;
    msg->header->client.partition_hash = partition_hash;
    msg->header->context.u.serialize_format = format;
    msg->add_ref(); // released by callers explicitly using release_ref
    return msg;
}

message_ex *message_ex::create_receive_message_with_standalone_header(const blob &data)
{
    message_ex *msg = new message_ex();
    size_t header_size = sizeof(message_header);
    std::string str(header_size, '\0');
    msg->header = reinterpret_cast<message_header *>(const_cast<char *>(str.data()));

    msg->buffers.emplace_back(blob::create_from_bytes(std::move(str)));
    msg->buffers.push_back(data);

    msg->header->body_length = data.length();
    msg->_is_read = true;
    // we skip the message header
    msg->_rw_index = 1;

    return msg;
}

message_ex *message_ex::copy_message_no_reply(const message_ex &old_msg)
{
    message_ex *msg = new message_ex();
    size_t header_size = sizeof(message_header);
    std::string str(header_size, '\0');
    msg->header = reinterpret_cast<message_header *>(const_cast<char *>(str.data()));

    msg->buffers.emplace_back(blob::create_from_bytes(std::move(str)));
    if (old_msg.buffers.size() == 1) {
        // if old_msg only has header, consider its header as data
        msg->buffers.emplace_back(old_msg.buffers[0]);
    } else {
        msg->buffers.emplace_back(old_msg.buffers[1]);
    }

    msg->header->body_length = msg->buffers[1].length();
    msg->header->context.u.serialize_format = old_msg.header->context.u.serialize_format;
    msg->_is_read = true;
    msg->_rw_index = 1;
    msg->_rw_offset = old_msg._rw_offset;
    msg->local_rpc_code = old_msg.local_rpc_code;
    msg->add_ref();

    return msg;
}

message_ex *message_ex::copy(bool clone_content, bool copy_for_receive)
{
    CHECK(this->_rw_committed, "should not copy the message when read/write is not committed");

    // ATTENTION:
    // - if this message is a written message, set copied message's write pointer to the end,
    //   then you can continue to append data to the copied message.
    //
    // - if this message is a read message, set copied message's read pointer to the beginning,
    //   then you can read data from the beginning.
    //
    // - if copy_for_receive is set, it means that we want to make a receiving message from a
    //   sending message. which is usually useful when you want to
    //   write mock for modules which use rpc.

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
        CHECK_EQ_MSG(i, total_length, "rpc_name = {}", msg->header->rpc_name);

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
        CHECK_EQ_MSG(buffers.size(), 1, "there must be only one buffer for read msg");
        CHECK((char *)header + sizeof(message_header) == (char *)buffers[0].data(),
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
    memset(static_cast<void *>(&hdr), 0, sizeof(hdr));
    hdr.hdr_type = 0x4e534452; // "RDSN"
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
    strncpy(hdr.rpc_name, sp->name.c_str(), sizeof(hdr.rpc_name) - 1);
    hdr.rpc_name[sizeof(hdr.rpc_name) - 1] = '\0';
    hdr.rpc_code.local_code = (uint32_t)rpc_code;
    hdr.rpc_code.local_hash = FLAGS_local_hash;

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

    // ATTENTION: the from_address may not be the primary address of this node
    // if there are more than one ports listened and the to_address is not equal to
    // the primary address.
    msg->header->from_address = to_address;
    msg->to_address = header->from_address;
    msg->io_session = io_session;
    msg->hdr_format = hdr_format;

    if (local_rpc_code != TASK_CODE_INVALID) {
        task_spec *request_sp = task_spec::get(local_rpc_code);
        task_spec *response_sp = task_spec::get(request_sp->rpc_paired_code);
        msg->local_rpc_code = response_sp->code;
        strncpy(hdr.rpc_name, response_sp->name.c_str(), sizeof(hdr.rpc_name) - 1);
        hdr.rpc_name[sizeof(hdr.rpc_name) - 1] = '\0';
        hdr.rpc_code.local_code = msg->local_rpc_code;
        hdr.rpc_code.local_hash = FLAGS_local_hash;

        // join point
        request_sp->on_rpc_create_response.execute(this, msg);
    } else {
        msg->local_rpc_code = TASK_CODE_INVALID;
        std::string ack_rpc_name(header->rpc_name);
        ack_rpc_name += "_ACK";
        strncpy(hdr.rpc_name, ack_rpc_name.c_str(), sizeof(hdr.rpc_name) - 1);
        hdr.rpc_name[sizeof(hdr.rpc_name) - 1] = '\0';
        hdr.rpc_code.local_code = TASK_CODE_INVALID;
        hdr.rpc_code.local_hash = FLAGS_local_hash;
    }

    return msg;
}

void message_ex::prepare_buffer_header()
{
    size_t header_size = sizeof(message_header);
    auto ptr(dsn::utils::make_shared_array<char>(header_size));

    // here we should call placement new,
    // so the gpid & rpc_address can be initialized
    new (ptr.get())(message_header);
    this->header = (message_header *)ptr.get();

    ::dsn::blob buffer(std::move(ptr), header_size);
    this->buffers.push_back(buffer);
    this->_rw_index = 0;
    this->_rw_offset = header_size;
}

void message_ex::release_buffer_header()
{
    // we should call destructor explicitly
    // as the header is constructed with placement new, see@prepare_buffer_header
    header->~message_header();
}

void message_ex::write_next(void **ptr, size_t *size, size_t min_size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    CHECK(!this->_is_read && this->_rw_committed,
          "there are pending msg write not committed"
          ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");
    auto ptr_data(utils::make_shared_array<char>(min_size));
    *size = min_size;
    *ptr = ptr_data.get();
    this->_rw_committed = false;

    ::dsn::blob buffer(ptr_data, min_size);
    this->_rw_index++;
    this->_rw_offset = 0;
    this->buffers.push_back(buffer);

    CHECK_EQ_MSG(_rw_index + 1, buffers.size(), "message write buffer count is not right");
}

void message_ex::write_commit(size_t size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    CHECK(!this->_rw_committed,
          "there are no pending msg write to be committed"
          ", please invoke dsn_msg_write_next and dsn_msg_write_commit in pairs");

    this->_rw_offset += (int)size;
    *this->buffers.rbegin() = this->buffers.rbegin()->range(0, (int)this->_rw_offset);
    this->_rw_committed = true;
    this->header->body_length += (int)size;
}

bool message_ex::read_next(void **ptr, size_t *size)
{
    // printf("%p %s %d\n", this, __FUNCTION__, utils::get_current_tid());
    CHECK(this->_is_read && this->_rw_committed,
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

bool message_ex::read_next(blob &data)
{
    // printf("%p %s %d\n", this, __FUNCTION__, utils::get_current_tid());
    CHECK(this->_is_read && this->_rw_committed,
          "there are pending msg read not committed"
          ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    int idx = this->_rw_index;
    if (-1 == idx || this->_rw_offset == static_cast<int>(this->buffers[idx].length())) {
        idx = ++this->_rw_index;
        this->_rw_offset = 0;
    }

    if (idx < (int)this->buffers.size()) {
        this->_rw_committed = false;
        data = this->buffers[idx].range(this->_rw_offset);
        return true;
    } else {
        data = blob();
        return false;
    }
}

void message_ex::read_commit(size_t size)
{
    // printf("%p %s\n", this, __FUNCTION__);
    CHECK(!this->_rw_committed,
          "there are no pending msg read to be committed"
          ", please invoke dsn_msg_read_next and dsn_msg_read_commit in pairs");

    CHECK_NE_MSG(-1, this->_rw_index, "no buffer in curent msg is under read");
    this->_rw_offset += (int)size;
    this->_rw_committed = true;
}

void message_ex::restore_read()
{
    _rw_index = -1;
    _rw_committed = true;
    _rw_offset = 0;
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
