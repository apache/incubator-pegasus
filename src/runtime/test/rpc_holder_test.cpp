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

#include "runtime/rpc/rpc_holder.h"

#include <fmt/core.h>
#include <string>

#include "common/gpid.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "runtime/message_utils.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/threadpool_code.h"

using namespace dsn;

typedef rpc_holder<query_cfg_request, query_cfg_response> t_rpc;

DEFINE_TASK_CODE_RPC(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                     TASK_PRIORITY_COMMON,
                     THREAD_POOL_DEFAULT)

TEST(rpc_holder, type_traits)
{
    ASSERT_FALSE(is_rpc_holder<bool>::value);
    ASSERT_TRUE(is_rpc_holder<t_rpc>::value);
}

TEST(rpc_holder, construct)
{
    {
        t_rpc rpc;
        ASSERT_FALSE(rpc.is_initialized());
    }

    {
        auto request = std::make_unique<query_cfg_request>();
        t_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
        ASSERT_TRUE(rpc.is_initialized());
    }

    {
        query_cfg_request request;
        request.app_name = "test";
        dsn::message_ex *msg =
            dsn::message_ex::create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
        dsn::marshall(msg, request);
        dsn::message_ex *msg2 = msg->copy(true, true);

        t_rpc rpc(msg2);
        ASSERT_TRUE(rpc.is_initialized());
        ASSERT_EQ(rpc.request().app_name, "test");
    }

    {
        auto request = std::make_unique<query_cfg_request>();
        t_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
        ASSERT_EQ(rpc.error(), ERR_OK);
        ASSERT_TRUE(rpc.is_initialized());

        rpc.error() = ERR_BUSY;
        ASSERT_EQ(rpc.error(), ERR_BUSY);

        rpc.error() = ERR_ADDRESS_ALREADY_USED;
        ASSERT_EQ(rpc.error(), ERR_ADDRESS_ALREADY_USED);
    }
}

TEST(rpc_holder, mock_rpc_call)
{
    RPC_MOCKING(t_rpc)
    {
        auto &mail_box = t_rpc::mail_box();

        for (int i = 0; i < 10; i++) {
            auto request = std::make_unique<query_cfg_request>();
            t_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
            rpc.call(rpc_address("127.0.0.1", 12321), nullptr, [](error_code) {});
        }

        ASSERT_EQ(mail_box.size(), 10);
    }

    // test in error cases
    RPC_MOCKING(t_rpc)
    {
        auto &mail_box = t_rpc::mail_box();

        for (int i = 0; i < 10; i++) {
            auto request = std::make_unique<query_cfg_request>();
            t_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
            rpc.error() = ERR_BUSY;
            rpc.call(rpc_address("127.0.0.1", 12321), nullptr, [](error_code) {});
        }

        ASSERT_EQ(mail_box.size(), 10);

        for (const auto &iter : mail_box) {
            ASSERT_EQ(iter.error(), ERR_BUSY);
        }
    }

    // instances of rpc mocking are independent
    RPC_MOCKING(t_rpc)
    {
        auto &mail_box = t_rpc::mail_box();
        ASSERT_EQ(mail_box.size(), 0);

        for (int i = 0; i < 10; i++) {
            auto request = std::make_unique<query_cfg_request>();
            t_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
            rpc.call(rpc_address("127.0.0.1", 12321), nullptr, [](error_code) {});
        }

        ASSERT_EQ(mail_box.size(), 10);
    }
}

TEST(rpc_holder, mock_rpc_reply)
{
    RPC_MOCKING(t_rpc)
    {
        auto &mail_box = t_rpc::mail_box();

        for (int i = 0; i < 10; i++) {
            query_cfg_request request;
            request.app_name = "haha";
            auto msg = from_thrift_request_to_received_message(
                request, RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
            auto rpc = t_rpc::auto_reply(msg);

            // destruct rpc and automatically reply via mail_box
        }

        ASSERT_EQ(mail_box.size(), 10);
    }
}

TEST(rpc_holder, mock_rpc_forward)
{
    RPC_MOCKING(t_rpc)
    {
        auto &mail_box = t_rpc::mail_box();
        auto &forward_mail_box = t_rpc::forward_mail_box();
        rpc_address forward_addr("127.0.0.1", 10086);

        for (int i = 0; i < 10; i++) {
            query_cfg_request request;
            auto msg = from_thrift_request_to_received_message(
                request, RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
            auto rpc = t_rpc::auto_reply(msg);
            rpc.forward(forward_addr);

            // destruct rpc and automatically reply via mail_box
        }

        ASSERT_EQ(mail_box.size(), 0);
        ASSERT_EQ(forward_mail_box.size(), 10);
        for (auto rpc : forward_mail_box) {
            ASSERT_EQ(rpc.remote_address(), forward_addr);
        }
    }
}
