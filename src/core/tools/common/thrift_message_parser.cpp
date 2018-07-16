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
 *     Jun. 2016, Zuoyan Qin, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "thrift_message_parser.h"
#include <dsn/service_api_c.h>
#include <dsn/cpp/serialization_helper/thrift_helper.h>
#include <dsn/utility/ports.h>
#include <dsn/utility/crc.h>

namespace dsn {
void thrift_message_parser::reset() { _header_parsed = false; }

message_ex *thrift_message_parser::get_message_on_receive(message_reader *reader,
                                                          /*out*/ int &read_next)
{
    read_next = 4096;

    dsn::blob &buf = reader->_buffer;
    char *buf_ptr = (char *)buf.data();
    unsigned int buf_len = reader->_buffer_occupied;

    if (buf_len >= sizeof(thrift_message_header)) {
        if (!_header_parsed) {
            read_thrift_header(buf_ptr, _thrift_header);

            if (!check_thrift_header(_thrift_header)) {
                derror("header check failed");
                read_next = -1;
                return nullptr;
            } else {
                _header_parsed = true;
            }
        }

        unsigned int msg_sz = sizeof(thrift_message_header) + _thrift_header.body_length;

        // msg done
        if (buf_len >= msg_sz) {
            dsn::blob msg_bb = buf.range(0, msg_sz);
            message_ex *msg = parse_message(_thrift_header, msg_bb);

            reader->_buffer = buf.range(msg_sz);
            reader->_buffer_occupied -= msg_sz;
            _header_parsed = false;
            read_next = (reader->_buffer_occupied >= sizeof(thrift_message_header)
                             ? 0
                             : sizeof(thrift_message_header) - reader->_buffer_occupied);
            msg->hdr_format = NET_HDR_THRIFT;
            return msg;
        }
        // buf_len < msg_sz
        else {
            read_next = msg_sz - buf_len;
            return nullptr;
        }
    }
    // buf_len < sizeof(thrift_message_header)
    else {
        read_next = sizeof(thrift_message_header) - buf_len;
        return nullptr;
    }
}

void thrift_message_parser::prepare_on_send(message_ex *msg)
{
    auto &header = msg->header;
    auto &buffers = msg->buffers;

    dassert(!header->context.u.is_request, "only support send response");
    dassert(header->server.error_name[0], "error name should be set");
    dassert(!buffers.empty(), "buffers can not be empty");

    // write thrift response header and thrift message begin
    binary_writer header_writer;
    binary_writer_transport header_trans(header_writer);
    boost::shared_ptr<binary_writer_transport> header_trans_ptr(&header_trans,
                                                                [](binary_writer_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol header_proto(header_trans_ptr);
    // first total length, but we don't know the length, so firstly we put a placeholder
    header_proto.writeI32(0);
    // then the error_message
    header_proto.writeString(string_view(header->server.error_name));
    // then the thrift message begin
    header_proto.writeMessageBegin(
        header->rpc_name, ::apache::thrift::protocol::T_REPLY, header->id);

    // write thrift message end
    binary_writer end_writer;
    binary_writer_transport end_trans(header_writer);
    boost::shared_ptr<binary_writer_transport> end_trans_ptr(&end_trans,
                                                             [](binary_writer_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol end_proto(end_trans_ptr);
    end_proto.writeMessageEnd();

    // now let's set the total length
    blob header_bb = header_writer.get_buffer();
    blob end_bb = end_writer.get_buffer();
    int32_t *total_length = reinterpret_cast<int32_t *>((void *)header_bb.data());
    *total_length = htobe32(header_bb.length() + header->body_length + end_bb.length());

    unsigned int dsn_size = sizeof(message_header) + header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < buffers.size()) {
        blob &buf = buffers[dsn_buf_count];
        dassert(dsn_size >= buf.length(), "%u VS %u", dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;
    }
    dassert(dsn_size == 0, "dsn_size = %u", dsn_size);

    // put header_bb and end_bb at the end
    buffers.resize(dsn_buf_count);
    buffers.emplace_back(std::move(header_bb));
    buffers.emplace_back(std::move(end_bb));
}

int thrift_message_parser::get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers)
{
    auto &msg_header = msg->header;
    auto &msg_buffers = msg->buffers;

    // leave buffers[0] to header
    int i = 1;
    // we must skip the dsn message header
    unsigned int offset = sizeof(message_header);
    unsigned int dsn_size = sizeof(message_header) + msg_header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < msg_buffers.size()) {
        blob &buf = msg_buffers[dsn_buf_count];
        dassert(dsn_size >= buf.length(), "%u VS %u", dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;

        if (offset >= buf.length()) {
            offset -= buf.length();
            continue;
        }
        buffers[i].buf = (void *)(buf.data() + offset);
        buffers[i].sz = buf.length() - offset;
        offset = 0;
        ++i;
    }
    dassert(dsn_size == 0, "dsn_size = %u", dsn_size);
    dassert(dsn_buf_count + 2 == msg_buffers.size(), "must have 2 more blob at the end");

    // set header
    blob &header_bb = msg_buffers[dsn_buf_count];
    buffers[0].buf = (void *)header_bb.data();
    buffers[0].sz = header_bb.length();

    // set end if need
    blob &end_bb = msg_buffers[dsn_buf_count + 1];
    if (end_bb.length() > 0) {
        buffers[i].buf = (void *)end_bb.data();
        buffers[i].sz = end_bb.length();
        ++i;
    }

    return i;
}

void thrift_message_parser::read_thrift_header(const char *buffer,
                                               /*out*/ thrift_message_header &header)
{
    header.hdr_type = *(uint32_t *)(buffer);
    buffer += sizeof(int32_t);
    header.hdr_version = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.hdr_length = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.hdr_crc32 = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.body_length = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.body_crc32 = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.app_id = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.partition_index = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.client_timeout = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.client_thread_hash = be32toh(*(int32_t *)(buffer));
    buffer += sizeof(int32_t);
    header.client_partition_hash = be64toh(*(int64_t *)(buffer));
}

bool thrift_message_parser::check_thrift_header(const thrift_message_header &header)
{
    if (header.hdr_type != THRIFT_HDR_SIG) {
        derror("hdr_type should be %s, but %s",
               message_parser::get_debug_string("THFT").c_str(),
               message_parser::get_debug_string((const char *)&header.hdr_type).c_str());
        return false;
    }
    if (header.hdr_version != 0) {
        derror("hdr_version should be 0, but %u", header.hdr_version);
        return false;
    }
    if (header.hdr_length != sizeof(thrift_message_header)) {
        derror("hdr_length should be %u, but %u", sizeof(thrift_message_header), header.hdr_length);
        return false;
    }
    return true;
}

dsn::message_ex *thrift_message_parser::parse_message(const thrift_message_header &thrift_header,
                                                      dsn::blob &message_data)
{
    dsn::blob body_data = message_data.range(thrift_header.hdr_length);
    dsn::message_ex *msg = message_ex::create_receive_message_with_standalone_header(body_data);
    dsn::message_header *dsn_hdr = msg->header;

    dsn::rpc_read_stream stream(msg);
    ::dsn::binary_reader_transport binary_transport(stream);
    boost::shared_ptr<::dsn::binary_reader_transport> trans_ptr(
        &binary_transport, [](::dsn::binary_reader_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol iprot(trans_ptr);

    std::string fname;
    ::apache::thrift::protocol::TMessageType mtype;
    int32_t seqid;
    iprot.readMessageBegin(fname, mtype, seqid);
    dinfo("rpc name: %s, type: %d, seqid: %d", fname.c_str(), mtype, seqid);

    dsn_hdr->hdr_type = THRIFT_HDR_SIG;
    dsn_hdr->hdr_length = sizeof(message_header);
    dsn_hdr->body_length = thrift_header.body_length;
    dsn_hdr->hdr_crc32 = dsn_hdr->body_crc32 = CRC_INVALID;

    dsn_hdr->id = seqid;
    strncpy(dsn_hdr->rpc_name, fname.c_str(), DSN_MAX_TASK_CODE_NAME_LENGTH);
    dsn_hdr->rpc_name[DSN_MAX_TASK_CODE_NAME_LENGTH - 1] = '\0';
    dsn_hdr->gpid.set_app_id(thrift_header.app_id);
    dsn_hdr->gpid.set_partition_index(thrift_header.partition_index);
    dsn_hdr->client.timeout_ms = thrift_header.client_timeout;
    dsn_hdr->client.thread_hash = thrift_header.client_thread_hash;
    dsn_hdr->client.partition_hash = thrift_header.client_partition_hash;

    if (mtype == ::apache::thrift::protocol::T_CALL ||
        mtype == ::apache::thrift::protocol::T_ONEWAY)
        dsn_hdr->context.u.is_request = 1;
    dassert(dsn_hdr->context.u.is_request == 1, "only support receive request");
    dsn_hdr->context.u.serialize_format = DSF_THRIFT_BINARY; // always serialize in thrift binary

    return msg;
}
}
