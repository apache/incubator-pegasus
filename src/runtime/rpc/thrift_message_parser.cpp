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

#include "thrift_message_parser.h"

#include <string.h>
#include <string>
#include <utility>
#include <vector>

#include "boost/smart_ptr/shared_ptr.hpp"
#include "common/gpid.h"
#include "common/serialization_helper/thrift_helper.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/rpc_stream.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/protocol/TBinaryProtocol.tcc"
#include "thrift/protocol/TProtocol.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/crc.h"
#include "utils/endians.h"
#include "utils/fmt_logging.h"
#include "utils/fmt_utils.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"

namespace dsn {

//                 //
// Request Parsing //
//                 //

///
/// For version 0:
/// |<--              fixed-size request header              -->|<--request body-->|
/// |-"THFT"-|- hdr_version + hdr_length -|-  request_meta_v0  -|-      blob      -|
/// |-"THFT"-|-  uint32(0)  + uint32(48) -|-      36bytes      -|-                -|
/// |-               12bytes             -|-      36bytes      -|-                -|
///
/// For version 1:
/// |<--          fixed-size request header           -->| <--        request body        -->|
/// |-"THFT"-|- hdr_version + meta_length + body_length -|- thrift_request_meta_v1 -|- blob -|
/// |-"THFT"-|-  uint32(1)  +   uint32    +    uint32   -|-      thrift struct     -|-      -|
/// |-                      16bytes                     -|-      thrift struct     -|-      -|
///
/// TODO(wutao1): remove v0 once it has no user

// "THFT" + uint32(hdr_version) + uint32(body_length) + uint32(meta_length)
static constexpr size_t HEADER_LENGTH_V1 = 16;

// "THFT" + uint32(hdr_version)
static constexpr size_t THFT_HDR_VERSION_LENGTH = 8;

// "THFT" + uint32(hdr_version) + uint32(hdr_length) + 36bytes(request_meta_v0)
static constexpr size_t HEADER_LENGTH_V0 = 48;

static void parse_request_meta_v0(data_input &input, /*out*/ request_meta_v0 &meta)
{
    meta.hdr_crc32 = input.read_u32();
    meta.body_length = input.read_u32();
    meta.body_crc32 = input.read_u32();
    meta.app_id = input.read_u32();
    meta.partition_index = input.read_u32();
    meta.client_timeout = input.read_u32();
    meta.client_thread_hash = input.read_u32();
    meta.client_partition_hash = input.read_u64();
}

static int32_t gpid_to_thread_hash(gpid id)
{
    static const int magic_number = 7919;
    return id.get_app_id() * magic_number + id.get_partition_index();
}

// Reads the requests's name, seqid, and TMessageType from the binary data,
// and constructs a `message_ex` object.
static message_ex *create_message_from_request_blob(const blob &body_data)
{
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
    dsn_hdr->id = seqid;
    strncpy(dsn_hdr->rpc_name, fname.c_str(), sizeof(dsn_hdr->rpc_name) - 1);
    dsn_hdr->rpc_name[sizeof(dsn_hdr->rpc_name) - 1] = '\0';

    if (mtype == ::apache::thrift::protocol::T_CALL ||
        mtype == ::apache::thrift::protocol::T_ONEWAY) {
        dsn_hdr->context.u.is_request = 1;
    }
    if (dsn_hdr->context.u.is_request != 1) {
        LOG_ERROR("invalid message type: {}", mtype);
        delete msg;
        /// set set rpc_read_stream::_msg to nullptr,
        /// to avoid the dstor to call read_commit of _msg, which is deleted here.
        stream.set_read_msg(nullptr);
        return nullptr;
    }
    dsn_hdr->context.u.serialize_format = DSF_THRIFT_BINARY; // always serialize in thrift binary

    // common fields
    msg->hdr_format = NET_HDR_THRIFT;
    dsn_hdr->hdr_type = THRIFT_HDR_SIG;
    dsn_hdr->hdr_length = sizeof(message_header);
    dsn_hdr->hdr_crc32 = msg->header->body_crc32 = CRC_INVALID;
    return msg;
}

// Parses the request's fixed-size header.
//
// For version 0:
// |-"THFT"-|- hdr_version + hdr_length -|-  request_meta_v0  -|
//
// For version 1:
// |-"THFT"-|- hdr_version + meta_length + body_length -|
//
bool thrift_message_parser::parse_request_header(message_reader *reader, int &read_next)
{
    blob buf = reader->buffer();
    // make sure there is enough space for 'THFT' and header_version
    if (buf.size() < THFT_HDR_VERSION_LENGTH) {
        read_next = THFT_HDR_VERSION_LENGTH - buf.size();
        return false;
    }

    // The first 4 bytes is "THFT"
    data_input input(buf.to_string_view());
    if (!utils::mequals(buf.data(), "THFT", 4)) {
        LOG_ERROR("hdr_type mismatch {}", message_parser::get_debug_string(buf.data()));
        read_next = -1;
        return false;
    }
    input.skip(4);

    // deal with different versions
    int header_version = input.read_u32();
    if (0 == header_version) {
        if (buf.size() < HEADER_LENGTH_V0) {
            read_next = HEADER_LENGTH_V0 - buf.size();
            return false;
        }

        uint32_t hdr_length = input.read_u32();
        if (hdr_length != HEADER_LENGTH_V0) {
            LOG_ERROR("hdr_length should be {}, but {}", HEADER_LENGTH_V0, hdr_length);
            read_next = -1;
            return false;
        }

        parse_request_meta_v0(input, *_meta_v0);
        reader->consume_buffer(HEADER_LENGTH_V0);
    } else if (1 == header_version) {
        if (buf.size() < HEADER_LENGTH_V1) {
            read_next = HEADER_LENGTH_V1 - buf.size();
            return false;
        }

        _v1_specific_vars->_meta_length = input.read_u32();
        _v1_specific_vars->_body_length = input.read_u32();
        reader->consume_buffer(HEADER_LENGTH_V1);
    } else {
        LOG_ERROR("invalid hdr_version {}", _header_version);
        read_next = -1;
        return false;
    }
    _header_version = header_version;

    return true;
}

message_ex *thrift_message_parser::parse_request_body_v0(message_reader *reader, int &read_next)
{
    blob buf = reader->buffer();

    // Parses request data
    // TODO(wutao1): handle the case where body_length is too short to parse.
    if (buf.size() < _meta_v0->body_length) {
        read_next = _meta_v0->body_length - buf.size();
        return nullptr;
    }

    buf = buf.range(0, _meta_v0->body_length);
    reader->consume_buffer(_meta_v0->body_length);
    message_ex *msg = create_message_from_request_blob(buf);
    if (msg == nullptr) {
        read_next = -1;
        reset();
        return nullptr;
    }

    read_next = (reader->_buffer_occupied >= HEADER_LENGTH_V0
                     ? 0
                     : HEADER_LENGTH_V0 - reader->_buffer_occupied);

    msg->header->body_length = _meta_v0->body_length;
    CHECK_EQ(msg->header->body_length, msg->buffers[1].size());
    msg->header->gpid.set_app_id(_meta_v0->app_id);
    msg->header->gpid.set_partition_index(_meta_v0->partition_index);
    msg->header->client.timeout_ms = _meta_v0->client_timeout;
    msg->header->client.thread_hash = _meta_v0->client_thread_hash;
    msg->header->client.partition_hash = _meta_v0->client_partition_hash;
    reset();
    return msg;
}

message_ex *thrift_message_parser::parse_request_body_v1(message_reader *reader, int &read_next)
{
    // Parses request meta
    blob buf = reader->buffer();
    if (!_v1_specific_vars->_meta_parsed) {
        if (buf.size() < _v1_specific_vars->_meta_length) {
            read_next = _v1_specific_vars->_meta_length - buf.size();
            return nullptr;
        }

        binary_reader meta_reader(buf);
        ::dsn::binary_reader_transport trans(meta_reader);
        boost::shared_ptr<::dsn::binary_reader_transport> transport(
            &trans, [](::dsn::binary_reader_transport *) {});
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        _v1_specific_vars->_meta_v1->read(&proto);
        _v1_specific_vars->_meta_parsed = true;
    }
    buf = buf.range(_v1_specific_vars->_meta_length);

    // Parses request body
    if (buf.size() < _v1_specific_vars->_body_length) {
        read_next = _v1_specific_vars->_body_length - buf.size();
        return nullptr;
    }
    buf = buf.range(0, _v1_specific_vars->_body_length);
    reader->consume_buffer(_v1_specific_vars->_meta_length + _v1_specific_vars->_body_length);
    message_ex *msg = create_message_from_request_blob(buf);
    if (msg == nullptr) {
        read_next = -1;
        reset();
        return nullptr;
    }

    read_next = (reader->_buffer_occupied >= HEADER_LENGTH_V1
                     ? 0
                     : HEADER_LENGTH_V1 - reader->_buffer_occupied);

    msg->header->body_length = _v1_specific_vars->_body_length;
    CHECK_EQ(msg->header->body_length, msg->buffers[1].size());
    msg->header->gpid.set_app_id(_v1_specific_vars->_meta_v1->app_id);
    msg->header->gpid.set_partition_index(_v1_specific_vars->_meta_v1->partition_index);
    msg->header->client.timeout_ms = _v1_specific_vars->_meta_v1->client_timeout;
    msg->header->client.thread_hash = gpid_to_thread_hash(msg->header->gpid);
    msg->header->client.partition_hash = _v1_specific_vars->_meta_v1->client_partition_hash;
    msg->header->context.u.is_backup_request = _v1_specific_vars->_meta_v1->is_backup_request;
    reset();
    return msg;
}

message_ex *thrift_message_parser::get_message_on_receive(message_reader *reader,
                                                          /*out*/ int &read_next)
{
    read_next = 4096;
    // Parses request header, -1 means header has not been parsed
    if (-1 == _header_version) {
        if (!parse_request_header(reader, read_next)) {
            return nullptr;
        }
    }

    // Parses request body
    switch (_header_version) {
    case 0:
        return parse_request_body_v0(reader, read_next);
    case 1:
        return parse_request_body_v1(reader, read_next);
    default:
        CHECK(false, "invalid header version: {}", _header_version);
    }

    return nullptr;
}

void thrift_message_parser::reset()
{
    _header_version = -1;
    _meta_v0->clear();
    _v1_specific_vars->clear();
}

//                   //
// Response Encoding //
//                   //

void thrift_message_parser::prepare_on_send(message_ex *msg)
{
    auto &header = msg->header;
    auto &buffers = msg->buffers;

    CHECK(!header->context.u.is_request, "only support send response");
    CHECK(header->server.error_name[0], "error name should be set");
    CHECK(!buffers.empty(), "buffers can not be empty");

    // write thrift response header and thrift message begin
    binary_writer header_writer;
    binary_writer_transport header_trans(header_writer);
    boost::shared_ptr<binary_writer_transport> header_trans_ptr(&header_trans,
                                                                [](binary_writer_transport *) {});
    ::apache::thrift::protocol::TBinaryProtocol header_proto(header_trans_ptr);
    // first total length, but we don't know the length, so firstly we put a placeholder
    header_proto.writeI32(0);
    // then the error_message
    header_proto.writeString(absl::string_view(header->server.error_name));
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
    int32_t *total_length = reinterpret_cast<int32_t *>(const_cast<char *>(header_bb.data()));
    *total_length = endian::hton(header_bb.length() + header->body_length + end_bb.length());

    unsigned int dsn_size = sizeof(message_header) + header->body_length;
    int dsn_buf_count = 0;
    while (dsn_size > 0 && dsn_buf_count < buffers.size()) {
        blob &buf = buffers[dsn_buf_count];
        CHECK_GE(dsn_size, buf.length());
        dsn_size -= buf.length();
        ++dsn_buf_count;
    }
    CHECK_EQ(dsn_size, 0);

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
        CHECK_GE(dsn_size, buf.length());
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
    CHECK_EQ(dsn_size, 0);
    CHECK_EQ_MSG(dsn_buf_count + 2, msg_buffers.size(), "must have 2 more blob at the end");

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

thrift_message_parser::thrift_message_parser()
    : _v1_specific_vars(new v1_specific_vars), _meta_v0(new request_meta_v0)
{
}

thrift_message_parser::~thrift_message_parser() = default;

} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(apache::thrift::protocol::TMessageType);
