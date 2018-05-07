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
 *     Unit-test for clientlet.
 *
 * Revision history:
 *     Nov., 2015, @shengofsun (Weijie Sun), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/clientlet.h>
#include <functional>
#include <chrono>
#include "test_utils.h"

DEFINE_TASK_CODE(LPC_TEST_CLIENTLET, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
using namespace dsn;

int global_value;
class test_clientlet : public clientlet
{
public:
    std::string str;
    int number;
    dsn::task_tracker _tracker;

public:
    test_clientlet() : clientlet(), str("before called"), number(0) { global_value = 0; }
    void callback_function1()
    {
        check_hashed_access();
        str = "after called";
        ++global_value;
    }

    void callback_function2()
    {
        check_hashed_access();
        number = 0;
        for (int i = 0; i < 1000; ++i)
            number += i;
        ++global_value;
    }

    void callback_function3() { ++global_value; }
};

TEST(dev_cpp, clientlet_task)
{
    /* normal lpc*/
    test_clientlet *cl = new test_clientlet();
    task_ptr t =
        tasking::enqueue(LPC_TEST_CLIENTLET, &cl->_tracker, [cl] { cl->callback_function1(); });
    EXPECT_TRUE(t != nullptr);
    t->wait();
    EXPECT_TRUE(cl->str == "after called");
    delete cl;

    /* task tracking */
    cl = new test_clientlet();
    std::vector<task_ptr> test_tasks;
    t = tasking::enqueue(LPC_TEST_CLIENTLET,
                         &cl->_tracker,
                         [=] { cl->callback_function1(); },
                         0,
                         std::chrono::seconds(30));
    test_tasks.push_back(t);
    t = tasking::enqueue(LPC_TEST_CLIENTLET,
                         &cl->_tracker,
                         [cl] { cl->callback_function1(); },
                         0,
                         std::chrono::seconds(30));
    test_tasks.push_back(t);
    t = tasking::enqueue_timer(LPC_TEST_CLIENTLET,
                               &cl->_tracker,
                               [cl] { cl->callback_function1(); },
                               std::chrono::seconds(20),
                               0,
                               std::chrono::seconds(30));
    test_tasks.push_back(t);

    delete cl;
    for (unsigned int i = 0; i != test_tasks.size(); ++i)
        EXPECT_FALSE(test_tasks[i]->cancel(true));
}

TEST(dev_cpp, clientlet_rpc)
{
    rpc_address addr("localhost", 20101);
    rpc_address addr2("localhost", TEST_PORT_END);
    rpc_address addr3("localhost", 32767);

    test_clientlet *cl = new test_clientlet();
    rpc::call_one_way_typed(addr, RPC_TEST_STRING_COMMAND, std::string("expect_no_reply"), 0);
    std::vector<task_ptr> task_vec;
    const char *command = "echo hello world";

    std::shared_ptr<std::string> str_command(new std::string(command));
    auto t = rpc::call(addr3,
                       RPC_TEST_STRING_COMMAND,
                       *str_command,
                       &cl->_tracker,
                       [str_command](error_code ec, std::string &&resp) {
                           if (ERR_OK == ec)
                               EXPECT_TRUE(str_command->substr(5) == resp);
                       });
    task_vec.push_back(t);
    t = rpc::call(addr2,
                  RPC_TEST_STRING_COMMAND,
                  std::string(command),
                  &cl->_tracker,
                  [](error_code ec, std::string &&resp) { EXPECT_TRUE(ec == ERR_OK); });
    task_vec.push_back(t);
    for (int i = 0; i != task_vec.size(); ++i)
        task_vec[i]->wait();
}
