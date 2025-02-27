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

#pragma once

#include <gtest/gtest.h>
#include <iostream>
#include "common/gpid.h"
#include "rpc/dns_resolver.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_stream.h"
#include "rpc/serialization.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "task/task_code.h"
#include "task/task.h"
#include "task/task_worker.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"

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
DEFINE_TASK_CODE_RPC(RPC_TEST_THRIFT_HOST_PORT_PARSER,
                     TASK_PRIORITY_COMMON,
                     THREAD_POOL_TEST_SERVER)

extern int g_test_count;
extern int g_test_ret;

inline void exec_tests()
{
    g_test_ret = RUN_ALL_TESTS();
    g_test_count++;
}

namespace dsn {
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
            auto next_hp = dsn::service_app::primary_host_port();
            if (next_hp.port() != TEST_PORT_END) {
                next_hp = dsn::host_port(next_hp.host(), next_hp.port() + 1);
                LOG_INFO("test_client_server, talk_to_others: {}", next_hp);
                dsn_rpc_forward(message, dsn::dns_resolver::instance().resolve_address(next_hp));
            } else {
                LOG_INFO("test_client_server, talk_to_me: {}", next_hp);
                reply(message, next_hp.to_string());
            }
        } else if (command == "expect_no_reply") {
            if (dsn::service_app::primary_host_port().port() == TEST_PORT_END) {
                LOG_INFO("test_client_server, talk_with_reply: {}",
                         dsn::service_app::primary_host_port());
                reply(message, dsn::service_app::primary_host_port().to_string());
            }
        } else if (command.substr(0, 5) == "echo ") {
            reply(message, command.substr(5));
        } else {
            LOG_ERROR("unknown command");
        }
    }

    void on_rpc_host_port_test(dsn::message_ex *message)
    {
        host_port hp;
        ::dsn::unmarshall(message, hp);
        reply(message, hp.to_string());
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
            register_rpc_handler(RPC_TEST_THRIFT_HOST_PORT_PARSER,
                                 "rpc.test.host_port",
                                 &test_client::on_rpc_host_port_test);
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
} // namespace dsn
