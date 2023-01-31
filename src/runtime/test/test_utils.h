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

#pragma once

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/task.h"
#include "runtime/task/task_worker.h"
#include <gtest/gtest.h>
#include <iostream>

using namespace ::dsn;

#ifndef TEST_PORT_BEGIN
#define TEST_PORT_BEGIN 20201
#define TEST_PORT_END 20203
#endif

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_RPC(RPC_TEST_HASH, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

DEFINE_TASK_CODE_RPC(RPC_TEST_HASH1, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_RPC(RPC_TEST_HASH2, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_RPC(RPC_TEST_HASH3, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_RPC(RPC_TEST_HASH4, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

DEFINE_TASK_CODE_RPC(RPC_TEST_STRING_COMMAND, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

extern int g_test_count;
extern int g_test_ret;

inline void exec_tests()
{
    g_test_ret = RUN_ALL_TESTS();
    g_test_count++;
}

class test_client : public ::dsn::serverlet<test_client>, public ::dsn::service_app
{
public:
    test_client(const service_app_info *info)
        : ::dsn::serverlet<test_client>("test-server"), ::dsn::service_app(info)
    {
    }

    void on_rpc_test(const int &test_id, ::dsn::rpc_replier<std::string> &replier)
    {
        std::string r = ::dsn::task::get_current_worker()->name();
        replier(r);
    }

    void on_rpc_string_test(dsn::message_ex *message)
    {
        std::string command;
        ::dsn::unmarshall(message, command);

        if (command == "expect_talk_to_others") {
            dsn::rpc_address next_addr = dsn::service_app::primary_address();
            if (next_addr.port() != TEST_PORT_END) {
                next_addr.assign_ipv4(next_addr.ip(), next_addr.port() + 1);
                LOG_INFO("test_client_server, talk_to_others: {}", next_addr);
                dsn_rpc_forward(message, next_addr);
            } else {
                LOG_INFO("test_client_server, talk_to_me: {}", next_addr);
                reply(message, next_addr.to_std_string());
            }
        } else if (command == "expect_no_reply") {
            if (dsn::service_app::primary_address().port() == TEST_PORT_END) {
                LOG_INFO("test_client_server, talk_with_reply: {}",
                         dsn::service_app::primary_address());
                reply(message, dsn::service_app::primary_address().to_std_string());
            }
        } else if (command.substr(0, 5) == "echo ") {
            reply(message, command.substr(5));
        } else {
            LOG_ERROR("unknown command");
        }
    }

    ::dsn::error_code start(const std::vector<std::string> &args)
    {
        // server
        if (args.size() == 1) {
            register_async_rpc_handler(RPC_TEST_HASH, "rpc.test.hash", &test_client::on_rpc_test);
            // used for corrupted message test
            register_async_rpc_handler(RPC_TEST_HASH1, "rpc.test.hash1", &test_client::on_rpc_test);
            register_async_rpc_handler(RPC_TEST_HASH2, "rpc.test.hash2", &test_client::on_rpc_test);
            register_async_rpc_handler(RPC_TEST_HASH3, "rpc.test.hash3", &test_client::on_rpc_test);
            register_async_rpc_handler(RPC_TEST_HASH4, "rpc.test.hash4", &test_client::on_rpc_test);

            register_rpc_handler(RPC_TEST_STRING_COMMAND,
                                 "rpc.test.string.command",
                                 &test_client::on_rpc_string_test);
        }

        // client
        else {
            std::cout << "=========================================================== "
                      << std::endl;
            std::cout << "================== run in rDSN threads ==================== "
                      << std::endl;
            std::cout << "=========================================================== "
                      << std::endl;
            exec_tests();
        }

        return ::dsn::ERR_OK;
    }

    ::dsn::error_code stop(bool cleanup = false) { return ERR_OK; }
};
