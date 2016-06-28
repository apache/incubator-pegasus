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

# include "thrift_message_parser.h"
# include <dsn/service_api_c.h>
# include <dsn/cpp/serialization_helper/thrift_helper.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "thrift.message.parser"

namespace dsn
{
    void thrift_message_parser::reset()
    {
        _header_parsed = false;
    }

    message_ex* thrift_message_parser::get_message_on_receive(message_reader* reader, /*out*/ int& read_next)
    {
        dsn::blob& buf = reader->_buffer;
        char* buf_ptr = (char*)buf.data();
        unsigned int buf_len = reader->_buffer_occupied;

        if (buf_len >= sizeof(thrift_message_header))
        {
            if (!_header_parsed)
            {
                read_thrift_header(buf_ptr, _thrift_header);

                if (!check_thrift_header(_thrift_header))
                {
                    derror("header check failed");
                    read_next = -1;
                    return nullptr;
                }
                else
                {
                    _header_parsed = true;
                }
            }

            unsigned int msg_sz = sizeof(thrift_message_header) + _thrift_header.body_length;

            // msg done
            if (buf_len >= msg_sz)
            {
                dsn::blob msg_bb = buf.range(0, msg_sz);
                message_ex* msg = parse_message(_thrift_header, msg_bb);

                reader->_buffer = buf.range(msg_sz);
                reader->_buffer_occupied -= msg_sz;
                _header_parsed = false;
                read_next = (reader->_buffer_occupied >= sizeof(thrift_message_header) ?
                                 0 : sizeof(thrift_message_header) - reader->_buffer_occupied);
                msg->hdr_format = NET_HDR_THRIFT;
                return msg;
            }
            else // buf_len < msg_sz
            {
                read_next = msg_sz - buf_len;
                return nullptr;
            }
        }
        else // buf_len < sizeof(thrift_message_header)
        {
            read_next = sizeof(thrift_message_header) - buf_len;
            return nullptr;
        }
    }

    int thrift_message_parser::prepare_on_send(message_ex* msg)
    {
        dassert(!msg->header->context.u.is_request, "only support send response");

        dsn::rpc_write_stream write_stream(msg);
        ::dsn::binary_writer_transport trans(write_stream);
        boost::shared_ptr< ::dsn::binary_writer_transport > trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
        ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

        //write message end, which indicate the end of a thrift message
        msg_proto.writeMessageEnd();
        //
        // we must add a thrift header for the response. And let's reserve one more buffer
        //
        return (int)msg->buffers.size() + 1;
    }

    int thrift_message_parser::get_buffers_on_send(message_ex* msg, /*out*/ send_buf* buffers)
    {
        dassert(!msg->header->context.u.is_request, "only support send response");
        dassert(msg->header->server.error_name[0], "error name should be set");
        dassert(!msg->buffers.empty(), "buffers can not be empty");

        // response format:
        //     <total_len(int32)> <thrift_string> <body_data(bytes)>
        //    |-----------response header------------------------------|
        binary_writer header_writer;
        binary_writer_transport trans(header_writer);
        boost::shared_ptr<binary_writer_transport> trans_ptr(&trans, [](binary_writer_transport*) {});
        ::apache::thrift::protocol::TBinaryProtocol proto(trans_ptr);

        //Total length, but we don't know the length, so firstly we put a placeholder
        proto.writeI32(0);
        char_ptr error_msg(msg->header->server.error_name, strlen(msg->header->server.error_name));
        //then the error_message
        proto.writeString<char_ptr>(error_msg);
        //then the thrift Message Begin
        proto.writeMessageBegin(msg->header->rpc_name, ::apache::thrift::protocol::T_REPLY, msg->header->id);

        //TODO: if more than one buffers in the header_writer, copying is inevitable. Should optimize this
        blob bb = header_writer.get_buffer();
        buffers[0].buf = (void*)bb.data();
        buffers[0].sz = bb.length();

        //now let's get the total length
        int32_t* total_length = reinterpret_cast<int32_t*>(buffers[0].buf);
        *total_length = header_writer.total_size() + msg->header->body_length;

        int i=1;
        // then copy the message body, we must skip the standard message_header
        unsigned int offset = sizeof(message_header);
        for (blob& buf : msg->buffers)
        {
            if (offset >= buf.length())
            {
                offset -= buf.length();
                continue;
            }

            buffers[i].buf = (void*)(buf.data() + offset);
            buffers[i].sz = buf.length() - offset;
            offset = 0;
            ++i;
        }

        //push the prefix buffers to hold memory
        msg->buffers.push_back(bb);
        return i;
    }

    void thrift_message_parser::read_thrift_header(const char* buffer, /*out*/ thrift_message_header& header)
    {
        header.hdr_type = header_type(buffer);
        buffer += sizeof(int32_t);
        header.hdr_version = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.hdr_length = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.hdr_crc32 = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.body_length = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.body_crc32 = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.app_id = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.partition_index = be32toh( *(int32_t*)(buffer) );
        buffer += sizeof(int32_t);
        header.client_hash = be64toh( *(int64_t*)(buffer) );
        buffer += sizeof(int64_t);
        header.client_timeout = be64toh( *(int64_t*)(buffer) );
    }

    bool thrift_message_parser::check_thrift_header(const thrift_message_header& header)
    {
        if (header.hdr_type != header_type::hdr_type_thrift)
        {
            derror("hdr_type should be %s, but %s",
                   header_type::hdr_type_thrift.debug_string().c_str(),
                   header.hdr_type.debug_string().c_str());
            return false;
        }
        if (header.hdr_version != 0)
        {
            derror("hdr_version should be 0, but %u", header.hdr_version);
            return false;
        }
        if (header.hdr_length != sizeof(thrift_message_header))
        {
            derror("hdr_length should be %u, but %u", sizeof(thrift_message_header), header.hdr_length);
            return false;
        }
        return true;
    }

    dsn::message_ex* thrift_message_parser::parse_message(const thrift_message_header& thrift_header, dsn::blob& message_data)
    {
        dsn::blob body_data = message_data.range(thrift_header.hdr_length);
        dsn::message_ex* msg = message_ex::create_receive_message_with_standalone_header(body_data);
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

        dsn_hdr->hdr_type = header_type::hdr_type_thrift;
        dsn_hdr->hdr_length = sizeof(message_header);
        dsn_hdr->body_length = thrift_header.body_length;
        dsn_hdr->hdr_crc32 = dsn_hdr->body_crc32 = CRC_INVALID;

        dsn_hdr->id = seqid;
        strncpy(dsn_hdr->rpc_name, fname.c_str(), DSN_MAX_TASK_CODE_NAME_LENGTH);
        dsn_hdr->gpid.u.app_id = thrift_header.app_id;
        dsn_hdr->gpid.u.partition_index = thrift_header.partition_index;
        dsn_hdr->client.hash = thrift_header.client_hash;
        dsn_hdr->client.timeout_ms = thrift_header.client_timeout;

        if (mtype == ::apache::thrift::protocol::T_CALL || mtype == ::apache::thrift::protocol::T_ONEWAY)
            dsn_hdr->context.u.is_request = 1;
        dassert(dsn_hdr->context.u.is_request == 1, "only support receive request");
        dsn_hdr->context.u.serialize_format = DSF_THRIFT_BINARY; // always serialize in thrift binary

        return msg;
    }
}
