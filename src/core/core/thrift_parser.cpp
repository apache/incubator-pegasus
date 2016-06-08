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
 *     The thrift rpc message parser
 *
 * Revision history:
 *     2016-04-04 Weijie Sun(sunweijie[at]xiaomi.com) First version
 *     2016-04-11 Weijie Sun(sunweijie[at]xiaomi.com) add a prefix length in thrift response,
 *          which is the same with the thrift TFramedTransport, and make the client easier to optimize
 */

#ifdef DSN_ENABLE_THRIFT_RPC

#include <dsn/internal/thrift_parser.h>
#include <dsn/cpp/serialization_helper/thrift_helper.h>

namespace dsn{

__thread char thrift_header_parser::response_header_buffer[128];

void thrift_header_parser::read_thrift_header_from_buffer(/*out*/dsn_thrift_header& result, const char* buffer)
{
    result.hdr_type = header_type(buffer);
    buffer += sizeof(int32_t);
    result.hdr_crc32 = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.body_offset = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.body_length = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.request_hash = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.client_timeout = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.opt.o = be64toh( *(int64_t*)(buffer) );
    buffer += sizeof(int64_t);
    result.gpid.value = be64toh( *(int64_t*)(buffer) );
}

dsn::message_ex* thrift_header_parser::parse_dsn_message(dsn_thrift_header* header, dsn::blob& message_data)
{
    dsn::blob message_content = message_data.range(header->body_offset);
    dsn::message_ex* msg = message_ex::create_receive_message_with_standalone_header(message_content);
    dsn::message_header* dsn_hdr = msg->header;

    dsn::rpc_read_stream stream(msg);
    ::dsn::binary_reader_transport binary_transport(stream);
    boost::shared_ptr< ::dsn::binary_reader_transport > trans_ptr(&binary_transport, [](::dsn::binary_reader_transport*) {});
    ::apache::thrift::protocol::TBinaryProtocol iprot(trans_ptr);

    std::string fname;
    ::apache::thrift::protocol::TMessageType mtype;
    int32_t seqid;
    iprot.readMessageBegin(fname, mtype, seqid);

    dinfo("rpc name: %s, type: %d, seqid: %d", fname.c_str(), mtype, seqid);
    memset(dsn_hdr, 0, sizeof(*dsn_hdr));
    dsn_hdr->hdr_type = header_type::hdr_dsn_thrift;
    dsn_hdr->body_length = header->body_length;
    strncpy(dsn_hdr->rpc_name, fname.c_str(), DSN_MAX_TASK_CODE_NAME_LENGTH);

    if (mtype == ::apache::thrift::protocol::T_CALL || mtype == ::apache::thrift::protocol::T_ONEWAY)
        dsn_hdr->context.u.is_request = 1;

    dsn_hdr->id = seqid;
    dsn_hdr->gpid.value = header->gpid.value;
    dsn_hdr->context.u.is_forward_disabled = header->opt.u.is_forward_msg_disabled;
    dsn_hdr->client.hash = header->request_hash;
    dsn_hdr->client.timeout_ms = header->client_timeout;

    return msg;
}

void thrift_header_parser::add_prefix_for_thrift_response(message_ex* msg)
{
    dsn::rpc_write_stream write_stream(msg);
    ::dsn::binary_writer_transport trans(write_stream);
    boost::shared_ptr< ::dsn::binary_writer_transport> trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
    ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

    //add message begin for each thrift response, corresponding with thrift_parser::add_post_fix's writeMessageEnd
    msg_proto.writeMessageBegin(msg->header->rpc_name, ::apache::thrift::protocol::T_REPLY, msg->header->id);
}

void thrift_header_parser::add_postfix_for_thrift_response(message_ex* msg)
{
    dsn::rpc_write_stream write_stream(msg);
    ::dsn::binary_writer_transport trans(write_stream);
    boost::shared_ptr< ::dsn::binary_writer_transport> trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
    ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

    //write message end, which indicate the end of a thrift message
    msg_proto.writeMessageEnd();
}

void thrift_header_parser::adjust_thrift_response(message_ex* msg)
{
    dassert(msg->is_response_adjusted_for_custom_rpc==false, "we have adjusted this");

    msg->is_response_adjusted_for_custom_rpc = true;
    add_postfix_for_thrift_response(msg);
}

int thrift_header_parser::prepare_buffers_on_send(message_ex* msg, int offset, /*out*/message_parser::send_buf* buffers)
{
    if ( !msg->is_response_adjusted_for_custom_rpc )
        adjust_thrift_response(msg);

    dassert(!msg->buffers.empty(), "buffers is not possible to be empty");

    // write the thrift message header
    int length;
    char* ptr = response_header_buffer + sizeof(int32_t)*2;
    strncpy(ptr, error_code(msg->header->server.error).to_string(), 120);
    length = strlen(ptr);

    // first buffer is the response header
    buffers[0].buf = response_header_buffer;
    buffers[0].sz = length + sizeof(int32_t)*2;
    //then the response error code string length
    ptr -= sizeof(int32_t);
    *((int*)ptr) = htobe32(length);
    //then the total length of all message: error_code_string's memory + msg_body
    ptr -= sizeof(int32_t);
    length = length + sizeof(int32_t) + msg->header->body_length;
    *((int*)ptr) = htobe32(length);

    // we ignore the message header when response thrift
    offset += sizeof(message_header);
    int count = 1;
    for (unsigned int i=0; i!=msg->buffers.size(); ++i)
    {
        blob& bb = msg->buffers[i];
        if (offset >= bb.length())
        {
            offset -= bb.length();
            continue;
        }

        buffers[count].buf = (void*)(bb.data() + offset);
        buffers[count].sz = (uint32_t)(bb.length() - offset);
        offset = 0;
        ++count;
    }
    return count;
}

int thrift_header_parser::get_send_buffers_count(message_ex* msg)
{
    if ( !msg->is_response_adjusted_for_custom_rpc )
        adjust_thrift_response(msg);

    //the message payload & the extra thrift header
    return msg->buffers.size() + 1;
}
}
#endif
