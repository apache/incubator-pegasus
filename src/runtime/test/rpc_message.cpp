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

#include <stdint.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "runtime/message_utils.cpp"
#include "runtime/message_utils.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/crc.h"
#include "utils/threadpool_code.h"

using namespace ::dsn;

DEFINE_TASK_CODE_RPC(RPC_CODE_FOR_TEST, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

TEST(core, message_ex)
{
    msg_context_t ctx0, ctx1;
    ctx0.context = 0;
    ctx0.u.is_request = true;
    ctx0.u.serialize_format = DSF_THRIFT_BINARY;
    ctx0.u.is_forward_supported = true;

    ctx1.context = 0;
    ctx1.u.is_request = false;
    ctx1.u.serialize_format = DSF_THRIFT_BINARY;
    ctx1.u.is_forward_supported = true;

    { // create_request
        uint64_t next_id = message_ex::new_id() + 1;
        message_ex *m = message_ex::create_request(RPC_CODE_FOR_TEST, 100, 1, 2);
        ASSERT_EQ(0, m->get_count());

        message_header &h = *m->header;
        ASSERT_EQ(0, h.hdr_version);
        ASSERT_EQ(sizeof(message_header), h.hdr_length);
        ASSERT_EQ(CRC_INVALID, h.hdr_crc32);
        ASSERT_EQ(0, h.body_length);
        ASSERT_EQ(CRC_INVALID, h.body_crc32);
        ASSERT_EQ(next_id, h.id);
        ASSERT_EQ(0, h.trace_id); ///////////////////
        ASSERT_STREQ(dsn::task_code(RPC_CODE_FOR_TEST).to_string(), h.rpc_name);
        ASSERT_EQ(0, h.gpid.value());
        ASSERT_EQ(ctx0.context, h.context.context);
        ASSERT_EQ(100, h.client.timeout_ms);
        ASSERT_EQ(1, h.client.thread_hash);
        ASSERT_EQ(2, h.client.partition_hash);
        ASSERT_EQ(0, h.from_address.port());

        ASSERT_EQ(1u, m->buffers.size());
        ASSERT_EQ((int)RPC_CODE_FOR_TEST, m->local_rpc_code);

        m->add_ref();
        ASSERT_EQ(1, m->get_count());
        m->release_ref();
    }

    { // create_response
        message_ex *request = message_ex::create_request(RPC_CODE_FOR_TEST, 0, 0);
        request->header->from_address = rpc_address("127.0.0.1", 8080);
        request->to_address = rpc_address("127.0.0.1", 9090);
        request->header->trace_id = 123456;

        message_ex *response = request->create_response();

        message_header &h = *response->header;
        ASSERT_EQ(0, h.hdr_version);
        ASSERT_EQ(sizeof(message_header), h.hdr_length);
        ASSERT_EQ(CRC_INVALID, h.hdr_crc32);
        ASSERT_EQ(0, h.body_length);
        ASSERT_EQ(CRC_INVALID, h.body_crc32);
        ASSERT_EQ(request->header->id, h.id);
        ASSERT_EQ(request->header->trace_id, h.trace_id); ///////////////////
        ASSERT_STREQ(dsn::task_code(RPC_CODE_FOR_TEST_ACK).to_string(), h.rpc_name);
        ASSERT_EQ(0, h.gpid.value());
        ASSERT_EQ(ctx1.context, h.context.context);
        ASSERT_EQ(0, h.server.error_code.local_code);

        ASSERT_EQ(1u, response->buffers.size());
        ASSERT_EQ((int)RPC_CODE_FOR_TEST_ACK, response->local_rpc_code);
        ASSERT_EQ(request->header->from_address, response->to_address);
        ASSERT_EQ(request->to_address, response->header->from_address);

        response->add_ref();
        response->release_ref();

        request->add_ref();
        request->release_ref();
    }

    { // write
        message_ex *request = message_ex::create_request(RPC_CODE_FOR_TEST, 100, 1);
        const char *data = "adaoihfeuifgggggisdosghkbvjhzxvdafdiofgeof";
        size_t data_size = strlen(data);

        void *ptr;
        size_t sz;

        request->write_next(&ptr, &sz, data_size);
        memcpy(ptr, data, data_size);
        request->write_commit(data_size);
        ASSERT_EQ(2u, request->buffers.size());
        ASSERT_EQ(ptr, request->rw_ptr(0));
        ASSERT_EQ((void *)((char *)ptr + 10), request->rw_ptr(10));
        ASSERT_EQ(nullptr, request->rw_ptr(data_size));

        request->write_next(&ptr, &sz, data_size);
        memcpy(ptr, data, data_size);
        request->write_commit(data_size);
        ASSERT_EQ(3u, request->buffers.size());
        ASSERT_EQ(ptr, request->rw_ptr(data_size));
        ASSERT_EQ((void *)((char *)ptr + 10), request->rw_ptr(data_size + 10));
        ASSERT_EQ(nullptr, request->rw_ptr(data_size + data_size));

        request->add_ref();
        request->release_ref();
    }

    { // read
        message_ex *request = message_ex::create_request(RPC_CODE_FOR_TEST, 100, 1);
        const char *data = "adaoihfeuifgggggisdosghkbvjhzxvdafdiofgeof";
        size_t data_size = strlen(data);

        void *ptr;
        size_t sz;

        request->write_next(&ptr, &sz, data_size);
        memcpy(ptr, data, data_size);
        request->write_commit(data_size);

        ASSERT_EQ(2u, request->buffers.size());

        message_ex *receive = message_ex::create_received_request(
            request->local_rpc_code,
            (dsn_msg_serialize_format)request->header->context.u.serialize_format,
            (void *)request->buffers[1].data(),
            request->buffers[1].size(),
            request->header->client.thread_hash,
            request->header->client.partition_hash);
        ASSERT_EQ(2u, receive->buffers.size());

        ASSERT_STREQ(dsn::task_code(RPC_CODE_FOR_TEST).to_string(), receive->header->rpc_name);

        ASSERT_TRUE(receive->read_next(&ptr, &sz));
        ASSERT_EQ(data_size, sz);
        ASSERT_EQ(std::string(data), std::string((const char *)ptr, sz));
        receive->read_commit(sz);

        ASSERT_FALSE(receive->read_next(&ptr, &sz));

        receive->add_ref();
        receive->release_ref();

        request->add_ref();
        request->release_ref();
    }
}

TEST(rpc_message, restore_read)
{
    using namespace dsn;
    query_cfg_request request, result;
    message_ptr msg = from_thrift_request_to_received_message(request, RPC_CODE_FOR_TEST);
    for (int i = 0; i < 10; i++) {
        unmarshall(msg, result);
        msg->restore_read();
    }
}

TEST(rpc_message, create_receive_message_with_standalone_header)
{
    auto data = blob::create_from_bytes("10086");

    message_ptr msg = message_ex::create_receive_message_with_standalone_header(data);
    ASSERT_EQ(msg->buffers.size(), 2);
    ASSERT_STREQ(msg->buffers[1].data(), data.data());
    ASSERT_EQ(msg->header->body_length, data.length());
}

TEST(rpc_message, copy_message_no_reply)
{
    auto data = blob::create_from_bytes("10086");
    message_ptr old_msg = message_ex::create_receive_message_with_standalone_header(data);
    old_msg->local_rpc_code = RPC_CODE_FOR_TEST;

    auto msg = message_ex::copy_message_no_reply(*old_msg);
    ASSERT_EQ(msg->buffers.size(), old_msg->buffers.size());
    ASSERT_STREQ(msg->buffers[1].data(), old_msg->buffers[1].data());
    ASSERT_EQ(msg->header->body_length, old_msg->header->body_length);
    ASSERT_EQ(msg->local_rpc_code, old_msg->local_rpc_code);

    // add_ref was called in message_ex::copy_message_no_reply for msg
    // so we only need to call release_ref here.
    msg->release_ref();
}
