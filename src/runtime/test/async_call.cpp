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

#include "runtime/task/async_calls.h"
#include "utils/thread_access_checker.h"
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

#include <gtest/gtest.h>
#include <functional>
#include <chrono>

#include "test_utils.h"

DEFINE_TASK_CODE(LPC_TEST_CLIENTLET, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
using namespace dsn;

int global_value;
class tracker_class
{
public:
    std::string str;
    int number;
    dsn::task_tracker _tracker;
    dsn::thread_access_checker _checker;

public:
    tracker_class() : str("before called"), number(0), _tracker(1) { global_value = 0; }
    void callback_function1()
    {
        _checker.only_one_thread_access();
        str = "after called";
        ++global_value;
    }

    void callback_function2()
    {
        _checker.only_one_thread_access();
        number = 0;
        for (int i = 0; i < 1000; ++i)
            number += i;
        ++global_value;
    }

    void callback_function3() { ++global_value; }
};

TEST(async_call, task_call)
{
    /* normal lpc*/
    tracker_class *tc = new tracker_class();
    task_ptr t =
        tasking::enqueue(LPC_TEST_CLIENTLET, &tc->_tracker, [tc] { tc->callback_function1(); });
    EXPECT_TRUE(t != nullptr);
    t->wait();
    EXPECT_TRUE(tc->str == "after called");
    delete tc;

    /* task tracking */
    tc = new tracker_class();
    std::vector<task_ptr> test_tasks;
    t = tasking::enqueue(LPC_TEST_CLIENTLET,
                         &tc->_tracker,
                         [=] { tc->callback_function1(); },
                         0,
                         std::chrono::seconds(30));
    test_tasks.push_back(t);
    t = tasking::enqueue(LPC_TEST_CLIENTLET,
                         &tc->_tracker,
                         [tc] { tc->callback_function1(); },
                         0,
                         std::chrono::seconds(30));
    test_tasks.push_back(t);
    t = tasking::enqueue_timer(LPC_TEST_CLIENTLET,
                               &tc->_tracker,
                               [tc] { tc->callback_function1(); },
                               std::chrono::seconds(20),
                               0,
                               std::chrono::seconds(30));
    test_tasks.push_back(t);

    delete tc;
    for (unsigned int i = 0; i != test_tasks.size(); ++i)
        EXPECT_FALSE(test_tasks[i]->cancel(true));
}

TEST(async_call, rpc_call)
{
    rpc_address addr("localhost", 20101);
    rpc_address addr2("localhost", TEST_PORT_END);
    rpc_address addr3("localhost", 32767);

    tracker_class *tc = new tracker_class();
    rpc::call_one_way_typed(addr, RPC_TEST_STRING_COMMAND, std::string("expect_no_reply"), 0);
    std::vector<task_ptr> task_vec;
    const char *command = "echo hello world";

    std::shared_ptr<std::string> str_command(new std::string(command));
    auto t = rpc::call(addr3,
                       RPC_TEST_STRING_COMMAND,
                       *str_command,
                       &tc->_tracker,
                       [str_command](error_code ec, std::string &&resp) {
                           if (ERR_OK == ec)
                               EXPECT_TRUE(str_command->substr(5) == resp);
                       });
    task_vec.push_back(t);
    t = rpc::call(addr2,
                  RPC_TEST_STRING_COMMAND,
                  std::string(command),
                  &tc->_tracker,
                  [](error_code ec, std::string &&resp) { EXPECT_TRUE(ec == ERR_OK); });
    task_vec.push_back(t);
    for (int i = 0; i != task_vec.size(); ++i)
        task_vec[i]->wait();

    delete tc;
}

class simple_task : public dsn::raw_task
{
public:
    simple_task(dsn::task_code code, const task_handler &h) : dsn::raw_task(code, h, 0, nullptr)
    {
        LOG_INFO("simple task {} created", fmt::ptr(this));
        allocate_count++;
    }
    virtual ~simple_task() override
    {
        LOG_INFO("simple task {} is deallocated", fmt::ptr(this));
        allocate_count--;
    }
    static std::atomic_int allocate_count;
};

class simple_task_container : public dsn::ref_counter
{
public:
    dsn::task_ptr t;
};

class simple_rpc_response_task : public dsn::rpc_response_task
{
public:
    simple_rpc_response_task(dsn::message_ex *m, const rpc_response_handler &h)
        : dsn::rpc_response_task(m, h)
    {
        LOG_INFO("simple rpc response task({}) created", fmt::ptr(this));
        allocate_count++;
    }
    virtual ~simple_rpc_response_task() override
    {
        LOG_INFO("simple rpc repsonse task({}) is dealloate", fmt::ptr(this));
        allocate_count--;
    }
    static std::atomic_int allocate_count;
};

std::atomic_int simple_task::allocate_count(0);
std::atomic_int simple_rpc_response_task::allocate_count(0);

DEFINE_TASK_CODE_RPC(TEST_CODE, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

bool spin_wait(const std::function<bool()> &pred, int wait_times)
{
    for (int i = 0; i != wait_times; ++i) {
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return pred();
}
TEST(async_call, task_destructor)
{
    {
        task_ptr t(new simple_task(LPC_TEST_CLIENTLET, nullptr));
        t->enqueue();
        t->wait();
    }
    ASSERT_TRUE(spin_wait([&]() { return simple_task::allocate_count.load() == 0; }, 10));

    dsn::ref_ptr<dsn::message_ex> req = message_ex::create_request(TEST_CODE);
    {
        dsn::rpc_response_task_ptr t(new simple_rpc_response_task(req.get(), nullptr));
        t->enqueue(dsn::ERR_OK, nullptr);
        t->wait();
    }
    ASSERT_TRUE(
        spin_wait([&]() { return simple_rpc_response_task::allocate_count.load() == 0; }, 10));

    {
        dsn::rpc_response_task_ptr t(new simple_rpc_response_task(req.get(), nullptr));
        t->replace_callback([t](dsn::error_code, dsn::message_ex *, dsn::message_ex *) {
            // ref_ptr out of callback + ref_ptr in callback + ref_added_in_enqueue
            ASSERT_EQ(3, t->get_count());
        });

        t->enqueue(dsn::ERR_OK, nullptr);
        t->wait();
    }
    ASSERT_TRUE(
        spin_wait([&]() { return simple_rpc_response_task::allocate_count.load() == 0; }, 10));

    {
        dsn::ref_ptr<simple_task_container> c(new simple_task_container());
        c->t =
            new simple_task(LPC_TEST_CLIENTLET, [c]() { LOG_INFO("cycle link reference test"); });

        c->t->enqueue();
        c->t->wait();
    }
    ASSERT_TRUE(spin_wait([&]() { return simple_task::allocate_count.load() == 0; }, 10));

    {
        dsn::ref_ptr<simple_task_container> c(new simple_task_container());
        c->t =
            new simple_task(LPC_TEST_CLIENTLET, [c]() { LOG_INFO("cycle link reference test"); });

        ASSERT_TRUE(c->t->cancel(false));
    }
    ASSERT_TRUE(spin_wait([&]() { return simple_task::allocate_count.load() == 0; }, 10));
}
