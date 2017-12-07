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
 *     Unit-test for rpc related code.
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <vector>
#include <string>
#include <queue>

#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/utility/priority_queue.h>
#include "../core/group_address.h"
#include "test_utils.h"
#include <boost/lexical_cast.hpp>

typedef std::function<void(error_code, dsn_message_t, dsn_message_t)> rpc_reply_handler;

static ::dsn::rpc_address build_group()
{
    ::dsn::rpc_address server_group;
    server_group.assign_group(dsn_group_build("server_group.test"));
    for (uint16_t p = TEST_PORT_BEGIN; p <= TEST_PORT_END; ++p) {
        dsn_group_add(server_group.group_handle(), ::dsn::rpc_address("localhost", p).c_addr());
    }

    dsn_group_set_leader(server_group.group_handle(),
                         ::dsn::rpc_address("localhost", TEST_PORT_BEGIN).c_addr());
    return server_group;
}

static void destroy_group(::dsn::rpc_address group) { dsn_group_destroy(group.group_handle()); }

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
        ::dsn::rpc_address addr_got;
        ddebug("talk to others callback, result: %s", result.c_str());
        EXPECT_TRUE(addr_got.from_string_ipv4(result.c_str()));
        EXPECT_EQ(TEST_PORT_END, addr_got.port());
    };

    ::dsn::task_ptr resp = ::dsn::rpc::call(addr,
                                            RPC_TEST_STRING_COMMAND,
                                            std::string("expect_talk_to_others"),
                                            nullptr,
                                            typed_callback);
    resp->wait();
    destroy_group(addr);
}

TEST(core, group_address_change_leader)
{
    ::dsn::rpc_address addr = build_group();

    error_code rpc_err;
    auto typed_callback = [addr, &rpc_err](error_code err_code, const std::string &result) -> void {
        rpc_err = err_code;
        if (ERR_OK == err_code) {
            ::dsn::rpc_address addr_got;
            ddebug("talk to others callback, result: %s", result.c_str());
            EXPECT_TRUE(addr_got.from_string_ipv4(result.c_str()));
            EXPECT_EQ(TEST_PORT_END, addr_got.port());
        }
    };

    ::dsn::task_ptr resp_task;

    // not update leader on forwarding
    addr.group_address()->set_update_leader_automatically(false);
    dsn_group_set_leader(addr.group_handle(),
                         ::dsn::rpc_address("localhost", TEST_PORT_BEGIN).c_addr());
    resp_task = ::dsn::rpc::call(addr,
                                 RPC_TEST_STRING_COMMAND,
                                 std::string("expect_talk_to_others"),
                                 nullptr,
                                 typed_callback);
    resp_task->wait();
    if (rpc_err == ERR_OK) {
        EXPECT_EQ(::dsn::rpc_address("localhost", TEST_PORT_BEGIN),
                  ::dsn::rpc_address(dsn_group_get_leader(addr.group_handle())));
    }
    // update leader on forwarding
    addr.group_address()->set_update_leader_automatically(true);
    dsn_group_set_leader(addr.group_handle(),
                         ::dsn::rpc_address("localhost", TEST_PORT_BEGIN).c_addr());
    resp_task = ::dsn::rpc::call(addr,
                                 RPC_TEST_STRING_COMMAND,
                                 std::string("expect_talk_to_others"),
                                 nullptr,
                                 typed_callback);
    resp_task->wait();
    ddebug("addr.leader=%s",
           ::dsn::rpc_address(dsn_group_get_leader(addr.group_handle())).to_string());
    if (rpc_err == ERR_OK) {
        EXPECT_EQ(TEST_PORT_END,
                  ::dsn::rpc_address(dsn_group_get_leader(addr.group_handle())).port());
    }
    destroy_group(addr);
}

typedef ::dsn::utils::priority_queue<::dsn::task_ptr, 1> task_resp_queue;
static void rpc_group_callback(error_code err,
                               dsn_message_t req,
                               dsn_message_t resp,
                               task_resp_queue *q,
                               rpc_reply_handler action_on_succeed,
                               rpc_reply_handler action_on_failure)
{
    if (ERR_OK == err) {
        action_on_succeed(err, req, resp);
    } else {
        action_on_failure(err, req, resp);

        dsn::rpc_address group_addr = ((dsn::message_ex *)req)->server_address;
        dsn_group_forward_leader(group_addr.group_handle());

        auto req_again = dsn_msg_copy(req, false, false);
        auto call_again =
            ::dsn::rpc::call(group_addr,
                             req_again,
                             nullptr,
                             [=](error_code err, dsn_message_t request, dsn_message_t response) {
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
        dsn_message_t request = dsn_msg_create_request(RPC_TEST_STRING_COMMAND);
        ::dsn::marshall(request, command);
        dsn::task_ptr resp_task = ::dsn::rpc::call(
            addr,
            request,
            nullptr,
            [&](error_code err, dsn_message_t request, dsn_message_t response) {
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
    rpc_reply_handler action_on_succeed = [](error_code err, dsn_message_t, dsn_message_t resp) {
        EXPECT_TRUE(err == ERR_OK);
        std::string result;
        ::dsn::unmarshall(resp, result);
        ::dsn::rpc_address a = dsn_address_from_string(result);
        EXPECT_TRUE(a.port() == TEST_PORT_END);
    };

    rpc_reply_handler action_on_failure = [](error_code err, dsn_message_t req, dsn_message_t) {
        if (err == ERR_TIMEOUT) {
            EXPECT_TRUE(((dsn::message_ex *)req)->to_address.port() != TEST_PORT_END);
        }
    };

    send_message(addr, std::string("expect_no_reply"), 1, action_on_succeed, action_on_failure);
    destroy_group(addr);
}

TEST(core, send_to_invalid_address)
{
    ::dsn::rpc_address group = build_group();
    /* here we assume 10.255.254.253:32766 is not assigned */
    dsn_group_set_leader(group.group_handle(),
                         ::dsn::rpc_address("10.255.254.253", 32766).c_addr());

    rpc_reply_handler action_on_succeed = [](error_code err, dsn_message_t, dsn_message_t resp) {
        EXPECT_TRUE(err == ERR_OK);
        std::string hehe_str;
        ::dsn::unmarshall(resp, hehe_str);
        EXPECT_TRUE(hehe_str == "hehehe");
    };
    rpc_reply_handler action_on_failure = [](error_code err, dsn_message_t, dsn_message_t) {
        EXPECT_TRUE(err != ERR_OK);
    };

    send_message(group, std::string("echo hehehe"), 1, action_on_succeed, action_on_failure);
    destroy_group(group);
}
