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

#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <functional>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "runtime/rpc/group_address.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "test_utils.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/priority_queue.h"

typedef std::function<void(error_code, dsn::message_ex *, dsn::message_ex *)> rpc_reply_handler;

static dsn::rpc_address build_group()
{
    ::dsn::rpc_address server_group;
    server_group.assign_group("server_group.test");
    dsn::rpc_group_address *g = server_group.group_address();
    for (uint16_t p = TEST_PORT_BEGIN; p <= TEST_PORT_END; ++p) {
        CHECK(g->add(dsn::rpc_address("localhost", p)), "");
    }

    g->set_leader(dsn::rpc_address("localhost", TEST_PORT_BEGIN));
    return server_group;
}

static ::dsn::rpc_address dsn_address_from_string(const std::string &str)
{
    size_t pos = str.find(":");
    if (pos != std::string::npos) {
        std::string host = str.substr(0, pos);
        uint16_t port = boost::lexical_cast<uint16_t>(str.substr(pos + 1));
        return ::dsn::rpc_address(host.c_str(), port);
    } else {
        // invalid address
        return ::dsn::rpc_address();
    }
}

TEST(core, rpc)
{
    int req = 0;
    ::dsn::rpc_address server("localhost", 20101);

    auto result = ::dsn::rpc::call_wait<std::string>(
        server, RPC_TEST_HASH, req, std::chrono::milliseconds(0), 1);
    EXPECT_TRUE(result.first == ERR_OK);

    EXPECT_TRUE(result.second.substr(0, result.second.length() - 2) ==
                "server.THREAD_POOL_TEST_SERVER");
}

TEST(core, group_address_talk_to_others)
{
    ::dsn::rpc_address addr = build_group();

    auto typed_callback = [addr](error_code err_code, const std::string &result) {
        EXPECT_EQ(ERR_OK, err_code);
        dsn::rpc_address addr_got;
        LOG_INFO("talk to others callback, result: {}", result);
        EXPECT_TRUE(addr_got.from_string_ipv4(result.c_str()));
        EXPECT_EQ(TEST_PORT_END, addr_got.port());
    };
    ::dsn::task_ptr resp = ::dsn::rpc::call(addr,
                                            RPC_TEST_STRING_COMMAND,
                                            std::string("expect_talk_to_others"),
                                            nullptr,
                                            typed_callback);
    resp->wait();
}

TEST(core, group_address_change_leader)
{
    ::dsn::rpc_address addr = build_group();

    error_code rpc_err;
    auto typed_callback = [addr, &rpc_err](error_code err_code, const std::string &result) -> void {
        rpc_err = err_code;
        if (ERR_OK == err_code) {
            ::dsn::rpc_address addr_got;
            LOG_INFO("talk to others callback, result: {}", result);
            EXPECT_TRUE(addr_got.from_string_ipv4(result.c_str()));
            EXPECT_EQ(TEST_PORT_END, addr_got.port());
        }
    };

    ::dsn::task_ptr resp_task;

    // not update leader on forwarding
    addr.group_address()->set_update_leader_automatically(false);
    addr.group_address()->set_leader(dsn::rpc_address("localhost", TEST_PORT_BEGIN));
    resp_task = ::dsn::rpc::call(addr,
                                 RPC_TEST_STRING_COMMAND,
                                 std::string("expect_talk_to_others"),
                                 nullptr,
                                 typed_callback);
    resp_task->wait();
    if (rpc_err == ERR_OK) {
        EXPECT_EQ(dsn::rpc_address("localhost", TEST_PORT_BEGIN),
                  dsn::rpc_address(addr.group_address()->leader()));
    }

    // update leader on forwarding
    addr.group_address()->set_update_leader_automatically(true);
    addr.group_address()->set_leader(dsn::rpc_address("localhost", TEST_PORT_BEGIN));
    resp_task = dsn::rpc::call(addr,
                               RPC_TEST_STRING_COMMAND,
                               std::string("expect_talk_to_others"),
                               nullptr,
                               typed_callback);
    resp_task->wait();
    LOG_INFO("addr.leader={}", addr.group_address()->leader());
    if (rpc_err == ERR_OK) {
        EXPECT_EQ(TEST_PORT_END, addr.group_address()->leader().port());
    }
}

typedef ::dsn::utils::priority_queue<::dsn::task_ptr, 1> task_resp_queue;
static void rpc_group_callback(error_code err,
                               dsn::message_ex *req,
                               dsn::message_ex *resp,
                               task_resp_queue *q,
                               rpc_reply_handler action_on_succeed,
                               rpc_reply_handler action_on_failure)
{
    if (ERR_OK == err) {
        action_on_succeed(err, req, resp);
    } else {
        action_on_failure(err, req, resp);

        dsn::rpc_address group_addr = ((dsn::message_ex *)req)->server_address;
        group_addr.group_address()->leader_forward();

        auto req_again = req->copy(false, false);
        auto call_again = ::dsn::rpc::call(
            group_addr,
            req_again,
            nullptr,
            [=](error_code err, dsn::message_ex *request, dsn::message_ex *response) {
                rpc_group_callback(err,
                                   request,
                                   response,
                                   q,
                                   std::move(action_on_succeed),
                                   std::move(action_on_failure));
            });
        q->enqueue(call_again, 0);
    }
}

static void send_message(::dsn::rpc_address addr,
                         const std::string &command,
                         int repeat_times,
                         rpc_reply_handler action_on_succeed,
                         rpc_reply_handler action_on_failure)
{
    task_resp_queue q("response.queue");
    for (int i = 0; i != repeat_times; ++i) {
        dsn::message_ptr request = dsn::message_ex::create_request(RPC_TEST_STRING_COMMAND);
        ::dsn::marshall(request.get(), command);
        dsn::task_ptr resp_task = ::dsn::rpc::call(
            addr,
            request.get(),
            nullptr,
            [&](error_code err, dsn::message_ex *request, dsn::message_ex *response) {
                rpc_group_callback(
                    err, request, response, &q, action_on_succeed, action_on_failure);
            });
        q.enqueue(resp_task, 0);
    }
    while (q.count() != 0) {
        task_ptr p = q.dequeue();
        p->wait();
    }
}

TEST(core, group_address_no_response_2)
{
    ::dsn::rpc_address addr = build_group();
    rpc_reply_handler action_on_succeed =
        [](error_code err, dsn::message_ex *, dsn::message_ex *resp) {
            EXPECT_TRUE(err == ERR_OK);
            std::string result;
            ::dsn::unmarshall(resp, result);
            ::dsn::rpc_address a = dsn_address_from_string(result);
            EXPECT_TRUE(a.port() == TEST_PORT_END);
        };

    rpc_reply_handler action_on_failure =
        [](error_code err, dsn::message_ex *req, dsn::message_ex *) {
            if (err == ERR_TIMEOUT) {
                EXPECT_TRUE(((dsn::message_ex *)req)->to_address.port() != TEST_PORT_END);
            }
        };

    send_message(addr, std::string("expect_no_reply"), 1, action_on_succeed, action_on_failure);
}

TEST(core, send_to_invalid_address)
{
    ::dsn::rpc_address group = build_group();
    /* here we assume 10.255.254.253:32766 is not assigned */
    group.group_address()->set_leader(dsn::rpc_address("10.255.254.253", 32766));

    rpc_reply_handler action_on_succeed =
        [](error_code err, dsn::message_ex *, dsn::message_ex *resp) {
            EXPECT_TRUE(err == ERR_OK);
            std::string hehe_str;
            ::dsn::unmarshall(resp, hehe_str);
            EXPECT_TRUE(hehe_str == "hehehe");
        };
    rpc_reply_handler action_on_failure = [](error_code err, dsn::message_ex *, dsn::message_ex *) {
        EXPECT_TRUE(err != ERR_OK);
    };

    send_message(group, std::string("echo hehehe"), 1, action_on_succeed, action_on_failure);
}
