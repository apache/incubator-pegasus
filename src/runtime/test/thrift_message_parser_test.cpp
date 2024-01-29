// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/smart_ptr/shared_ptr.hpp>
#include <string.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TBinaryProtocol.tcc>
#include <thrift/protocol/TProtocol.h>
#include <memory>
#include <string>
#include <vector>

#include "common/gpid.h"
#include "common/serialization_helper/thrift_helper.h"
#include "gtest/gtest.h"
#include "request_meta_types.h"
#include "runtime/rpc/message_parser.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/rpc/thrift_message_parser.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/crc.h"
#include "utils/endians.h"
#include "utils/threadpool_code.h"

namespace dsn {

DEFINE_TASK_CODE_RPC(RPC_TEST_THRIFT_MESSAGE_PARSER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

class thrift_message_parser_test : public testing::Test
{
public:
    void
    mock_reader_read_data(message_reader &reader, const std::string &data, int message_count = 1)
    {
        char *buf = reader.read_buffer_ptr(data.length() * message_count);
        for (int i = 0; i < message_count; i++) {
            memcpy(buf + i * data.length(), data.c_str(), data.length());
            reader.mark_read(data.length());
        }
    }

    void test_get_message_on_receive_v0_data(message_reader &reader,
                                             apache::thrift::protocol::TMessageType messageType,
                                             bool is_request,
                                             int message_count = 1)
    {
        /// write rpc message
        size_t body_length = 0;
        message_ptr msg =
            message_ex::create_request(RPC_TEST_THRIFT_MESSAGE_PARSER, 1000, 64, 5000000000);
        rpc_write_stream stream(msg);
        binary_writer_transport binary_transport(stream);
        boost::shared_ptr<binary_writer_transport> trans_ptr(&binary_transport,
                                                             [](binary_writer_transport *) {});
        ::apache::thrift::protocol::TBinaryProtocol oprot(trans_ptr);
        body_length += oprot.writeMessageBegin("RPC_TEST_THRIFT_MESSAGE_PARSER", messageType, 999);
        body_length += oprot.writeMessageEnd();
        stream.commit_buffer();

        thrift_message_parser parser;
        std::string data;
        int read_next = 0;
        data = std::string("THFT") + std::string(44 + body_length, '\0'); // header+body_length
        data_output out(&data[4], 44);
        out.write_u32(0);           // hdr_version
        out.write_u32(48);          // hdr_length
        out.write_u32(0);           // hdr_crc32
        out.write_u32(body_length); // body_length
        out.write_u32(0);           // body_crc32
        out.write_u32(1);           // app_id
        out.write_u32(28);          // partition_index
        out.write_u32(1000);        // client_timeout
        out.write_u32(64);          // client_thread_hash
        out.write_u64(5000000000);  // client_partition_hash
        ASSERT_EQ(stream.get_buffer().size(), body_length);
        memcpy(&data[48], stream.get_buffer().data(), stream.get_buffer().size());

        mock_reader_read_data(reader, data, message_count);

        for (int i = 0; i < message_count; i++) {
            msg = parser.get_message_on_receive(&reader, read_next);

            if (is_request) {
                ASSERT_NE(msg, nullptr);
                ASSERT_EQ(msg->hdr_format, NET_HDR_THRIFT);

                ASSERT_EQ(msg->header->body_length, body_length);
                ASSERT_EQ(msg->header->gpid, gpid(1, 28));
                ASSERT_EQ(msg->header->hdr_type, THRIFT_HDR_SIG);
                ASSERT_EQ(msg->header->hdr_length, sizeof(message_header));
                ASSERT_EQ(msg->header->hdr_crc32, CRC_INVALID);
                ASSERT_EQ(msg->header->body_crc32, CRC_INVALID);
                ASSERT_EQ(msg->header->id, 999);

                ASSERT_EQ(msg->header->client.timeout_ms, 1000);
                ASSERT_EQ(msg->header->client.thread_hash, 64);
                ASSERT_EQ(msg->header->client.partition_hash, 5000000000);

                ASSERT_EQ(msg->header->context.u.is_request, true);
                ASSERT_EQ(msg->header->context.u.serialize_format, DSF_THRIFT_BINARY);

                // v0 Thrift network format doesn't support message context.
                ASSERT_EQ(msg->header->context.u.is_backup_request, false);
                ASSERT_EQ(msg->header->context.u.is_forwarded, false);
                ASSERT_EQ(msg->header->context.u.is_forward_supported, false);

                ASSERT_EQ(msg->buffers[1].size(), body_length);

                // must be reset
                ASSERT_EQ(parser._header_version, -1);
                ASSERT_EQ(parser._v1_specific_vars->_meta_parsed, false);
                ASSERT_EQ(parser._v1_specific_vars->_meta_length, 0);
                ASSERT_EQ(parser._v1_specific_vars->_body_length, 0);
            } else {
                ASSERT_EQ(msg, nullptr);
                ASSERT_EQ(read_next, -1);
            }
        }
    }

    void test_get_message_on_receive_v1_data(message_reader &reader,
                                             apache::thrift::protocol::TMessageType messageType,
                                             bool is_request,
                                             bool is_backup_request,
                                             int message_count = 1)
    {
        /// write rpc message
        size_t body_length = 0;
        message_ptr msg =
            message_ex::create_request(RPC_TEST_THRIFT_MESSAGE_PARSER, 1000, 64, 5000000000);
        rpc_write_stream body_stream(msg);
        {
            binary_writer_transport transport(body_stream);
            boost::shared_ptr<binary_writer_transport> trans_ptr(&transport,
                                                                 [](binary_writer_transport *) {});
            ::apache::thrift::protocol::TBinaryProtocol oprot(trans_ptr);
            body_length +=
                oprot.writeMessageBegin("RPC_TEST_THRIFT_MESSAGE_PARSER", messageType, 999);
            body_length += oprot.writeMessageEnd();
            body_stream.commit_buffer();
            ASSERT_EQ(body_stream.get_buffer().size(), body_length);
        }

        // write rpc meta
        size_t meta_length = 0;
        thrift_request_meta_v1 meta;
        meta.__set_is_backup_request(is_backup_request);
        meta.__set_app_id(1);
        meta.__set_partition_index(28);
        meta.__set_client_timeout(1000);
        meta.__set_client_partition_hash(5000000000);

        binary_writer meta_writer(1024);
        ::dsn::binary_writer_transport trans(meta_writer);
        boost::shared_ptr<::dsn::binary_writer_transport> transport(
            &trans, [](::dsn::binary_writer_transport *) {});
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        meta.write(&proto);

        meta_length = meta_writer.get_buffer().size();

        thrift_message_parser parser;
        std::string data;
        int read_next = 0;
        data = std::string("THFT") + std::string(12 + meta_length + body_length, '\0');
        data_output out(&data[4], 12);
        out.write_u32(1);
        out.write_u32(meta_length);
        out.write_u32(body_length);

        memcpy(&data[16], meta_writer.get_buffer().data(), meta_writer.get_buffer().size());
        memcpy(&data[16 + meta_length],
               body_stream.get_buffer().data(),
               body_stream.get_buffer().size());
        ASSERT_EQ(16 + meta_length + body_length, data.size());
        mock_reader_read_data(reader, data, message_count);
        ASSERT_EQ(reader.buffer().size(), data.size() * message_count);

        for (int i = 0; i != message_count; ++i) {
            msg = parser.get_message_on_receive(&reader, read_next);

            if (is_request) {
                ASSERT_NE(msg, nullptr);
                ASSERT_EQ(msg->hdr_format, NET_HDR_THRIFT);

                ASSERT_EQ(msg->header->body_length, body_length);
                ASSERT_EQ(msg->header->gpid, gpid(1, 28));
                ASSERT_EQ(msg->header->hdr_type, THRIFT_HDR_SIG);
                ASSERT_EQ(msg->header->hdr_length, sizeof(message_header));
                ASSERT_EQ(msg->header->hdr_crc32, CRC_INVALID);
                ASSERT_EQ(msg->header->body_crc32, CRC_INVALID);
                ASSERT_EQ(msg->header->id, 999);

                ASSERT_EQ(msg->header->client.timeout_ms, 1000);
                ASSERT_EQ(msg->header->client.thread_hash, 7947);
                ASSERT_EQ(msg->header->client.partition_hash, 5000000000);

                ASSERT_EQ(msg->header->context.u.is_request, true);
                ASSERT_EQ(msg->header->context.u.serialize_format, DSF_THRIFT_BINARY);
                ASSERT_EQ(msg->header->context.u.is_backup_request, is_backup_request);
                ASSERT_EQ(msg->header->context.u.is_forwarded, false);
                ASSERT_EQ(msg->header->context.u.is_forward_supported, false);

                // must be reset
                ASSERT_EQ(parser._header_version, -1);
                ASSERT_EQ(parser._v1_specific_vars->_meta_parsed, false);
                ASSERT_EQ(msg->buffers[1].size(), body_length);
            } else {
                ASSERT_EQ(msg, nullptr);
                ASSERT_EQ(read_next, -1);
            }
        }
    }
};

TEST_F(thrift_message_parser_test, get_message_on_receive_incomplete_second_field)
{
    for (int i = 0; i < 4; i++) {
        thrift_message_parser parser;

        std::string data;
        int read_next = 0;
        message_reader reader(64);
        data = std::string("THFT") + std::string(i, ' ');
        mock_reader_read_data(reader, data);
        ASSERT_EQ(reader._buffer_occupied, 4 + i);
        ASSERT_EQ(reader.buffer().size(), 4 + i);

        message_ex *msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(read_next, 4 - i);
        ASSERT_EQ(parser._header_version, -1);
        ASSERT_EQ(parser._v1_specific_vars->_meta_parsed, false);
        ASSERT_EQ(parser._v1_specific_vars->_meta_length, 0);

        // not consumed
        ASSERT_EQ(reader._buffer_occupied, data.length());
        ASSERT_EQ(reader.buffer().size(), data.length());
    }
}

TEST_F(thrift_message_parser_test, get_message_on_receive_incomplete_v0_hdr_len)
{
    for (int i = 4; i < 44; i++) {
        thrift_message_parser parser;

        std::string data;
        int read_next = 0;
        message_reader reader(64);
        data = std::string("THFT") + std::string(i, ' ');

        data_output out(&data[4], 8);
        out.write_u32(0);
        out.write_u32(48);

        mock_reader_read_data(reader, data);
        ASSERT_EQ(reader.buffer().size(), data.length());

        message_ex *msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(read_next, 48 - data.length()); // read remaining fields
        ASSERT_EQ(parser._header_version, -1);
    }
}

TEST_F(thrift_message_parser_test, get_message_on_receive_invalid_v0_hdr_length)
{
    for (int i = 0; i < 48; i++) {
        thrift_message_parser parser;

        std::string data;
        int read_next = 0;
        message_reader reader(64);
        data = std::string("THFT") + std::string(44, '\0'); // full 48 bytes

        // hdr_version = 0
        data_output out(&data[4], 8);
        out.write_u32(0);
        // hdr_length = i
        out.write_u32(i);

        mock_reader_read_data(reader, data);
        message_ex *msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(read_next, -1);
        ASSERT_EQ(parser._header_version, -1);
    }
}

TEST_F(thrift_message_parser_test, get_message_on_receive_valid_v0_hdr)
{
    thrift_message_parser parser;
    std::string data;
    int read_next = 0;
    message_reader reader(64);
    data = std::string("THFT") + std::string(44, '\0'); // full 48 bytes
    data_output out(&data[4], 44);
    out.write_u32(0);          // hdr_version
    out.write_u32(48);         // hdr_length
    out.write_u32(0);          // hdr_crc32
    out.write_u32(100);        // body_length
    out.write_u32(0);          // body_crc32
    out.write_u32(1);          // app_id
    out.write_u32(28);         // partition_index
    out.write_u32(1000);       // client_timeout
    out.write_u32(64);         // client_thread_hash
    out.write_u64(5000000000); // client_partition_hash

    mock_reader_read_data(reader, data);

    message_ex *msg = parser.get_message_on_receive(&reader, read_next);
    ASSERT_EQ(msg, nullptr);
    ASSERT_EQ(read_next, 100); // required to read more
    ASSERT_EQ(parser._header_version, 0);
    ASSERT_EQ(reader.buffer().size(), 0);
    ASSERT_EQ(parser._meta_v0->hdr_crc32, 0);
    ASSERT_EQ(parser._meta_v0->body_length, 100);
    ASSERT_EQ(parser._meta_v0->body_crc32, 0);
    ASSERT_EQ(parser._meta_v0->app_id, 1);
    ASSERT_EQ(parser._meta_v0->partition_index, 28);
    ASSERT_EQ(parser._meta_v0->client_timeout, 1000);
    ASSERT_EQ(parser._meta_v0->client_thread_hash, 64);
    ASSERT_EQ(parser._meta_v0->client_partition_hash, 5000000000);
}

TEST_F(thrift_message_parser_test, get_message_on_receive_valid_v0_data)
{
    message_reader reader(64);

    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v0_data(reader, apache::thrift::protocol::T_CALL, true));
    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v0_data(reader, apache::thrift::protocol::T_ONEWAY, true));
}

TEST_F(thrift_message_parser_test, get_message_on_receive_v0_not_request)
{
    message_reader reader(64);

    // ensure server won't corrupt when it receives a non-request.
    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v0_data(reader, apache::thrift::protocol::T_REPLY, false));
    // bad message should be consumed and discarded
    ASSERT_EQ(reader.buffer().size(), 0);
    ASSERT_NO_FATAL_FAILURE(test_get_message_on_receive_v0_data(
        reader, apache::thrift::protocol::TMessageType(65), false));
    ASSERT_EQ(reader.buffer().size(), 0);
}

TEST_F(thrift_message_parser_test, get_message_on_receive_incomplete_v1_hdr)
{
    for (int i = 4; i < 12; i++) {
        thrift_message_parser parser;

        std::string data;
        int read_next = 0;
        message_reader reader(64);
        data = std::string("THFT") + std::string(i, ' ');

        data_output out(&data[4], 8);
        out.write_u32(1);

        mock_reader_read_data(reader, data);
        ASSERT_EQ(reader.buffer().size(), data.length());

        message_ex *msg = parser.get_message_on_receive(&reader, read_next);
        ASSERT_EQ(msg, nullptr);
        ASSERT_EQ(read_next, 16 - data.length()); // read remaining fields
        ASSERT_EQ(parser._header_version, -1);
        ASSERT_EQ(parser._v1_specific_vars->_meta_length, 0);
        ASSERT_EQ(parser._v1_specific_vars->_body_length, 0);
    }
}

TEST_F(thrift_message_parser_test, get_message_on_receive_valid_v1_hdr)
{
    thrift_message_parser parser;
    std::string data;
    int read_next = 0;
    message_reader reader(64);
    data = std::string("THFT") + std::string(12, '\0'); // full 12 bytes
    data_output out(&data[4], 12);
    out.write_u32(1);   // header_version
    out.write_u32(100); // meta_length
    out.write_u32(200); // body_length

    mock_reader_read_data(reader, data);
    ASSERT_EQ(reader.buffer().size(), 16);

    message_ex *msg = parser.get_message_on_receive(&reader, read_next);
    ASSERT_EQ(msg, nullptr);
    ASSERT_EQ(read_next, 100); // required to read more
    ASSERT_EQ(parser._header_version, 1);
    ASSERT_EQ(parser._v1_specific_vars->_meta_length, 100);
    ASSERT_EQ(parser._v1_specific_vars->_body_length, 200);
    ASSERT_EQ(parser._v1_specific_vars->_meta_parsed, false);
    ASSERT_EQ(reader.buffer().size(), 0);
}

TEST_F(thrift_message_parser_test, get_message_on_receive_v1_data)
{
    message_reader reader(64);
    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v1_data(reader, apache::thrift::protocol::T_CALL, true, true));
    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v1_data(reader, apache::thrift::protocol::T_CALL, true, false));
    ASSERT_NO_FATAL_FAILURE(test_get_message_on_receive_v1_data(
        reader, apache::thrift::protocol::T_ONEWAY, true, true));
    ASSERT_NO_FATAL_FAILURE(test_get_message_on_receive_v1_data(
        reader, apache::thrift::protocol::T_ONEWAY, true, false));

    ASSERT_NO_FATAL_FAILURE(test_get_message_on_receive_v1_data(
        reader, apache::thrift::protocol::TMessageType(65), false, false));
    reader.truncate_read();
}

TEST_F(thrift_message_parser_test, get_message_on_large_writes_pileup_v0)
{
    message_reader reader(4096);
    ASSERT_NO_FATAL_FAILURE(
        test_get_message_on_receive_v0_data(reader, apache::thrift::protocol::T_CALL, true, 10));
}

TEST_F(thrift_message_parser_test, get_message_on_large_writes_pileup_v1)
{
    message_reader reader(4096);
    ASSERT_NO_FATAL_FAILURE(test_get_message_on_receive_v1_data(
        reader, apache::thrift::protocol::T_CALL, true, true, 10));
}

} // namespace dsn
