// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/task.h>
#include <dsn/tool-api/task_code.h>
#include <gtest/gtest.h>
#include <dsn/tool-api/file_io.h>

namespace dsn {

DEFINE_TASK_CODE_AIO(LPC_TASK_TEST, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

class task_test : public ::testing::Test
{
public:
    static void test_init()
    {
        aio_task t1(LPC_TASK_TEST, nullptr);
        ASSERT_TRUE(t1._is_null);
        ASSERT_EQ(t1._wait_event.load(), nullptr);
        ASSERT_EQ(t1.next, nullptr);
        ASSERT_EQ(t1._state, task_state::TASK_STATE_READY);
        ASSERT_FALSE(t1._wait_for_cancel);

        // TODO(wutao1): raw_task and rpc_request_task is not safe for
        //               null callback.
    }

    static void test_null_task()
    {
        aio_task_ptr t1 = new aio_task(LPC_TASK_TEST, nullptr);

        // empty task will executed at once
        t1->enqueue(ERR_OK, 100);
        ASSERT_EQ(t1->_state, task_state::TASK_STATE_FINISHED);

        // never wait for an empty task
        ASSERT_TRUE(t1->wait(10000));
        ASSERT_EQ(t1->_state, task_state::TASK_STATE_FINISHED);
        ASSERT_TRUE(t1->_wait_event.load() == nullptr);
        ASSERT_TRUE(t1->_is_null);
    }

    static void test_signal_finished_task()
    {
        disk_file *fp = file::open("config-test.ini", O_RDONLY | O_BINARY, 0);

        // this aio task is enqueued into read-queue of disk_engine
        char buffer[128];
        // in simulator environment this task will be executed immediately,
        // so we excluded config-test-sim.ini for this test.
        auto t = file::read(fp, buffer, 128, 0, LPC_TASK_TEST, nullptr, nullptr);

        t->wait(10000);
        ASSERT_EQ(t->_state, task_state::TASK_STATE_FINISHED);

        // signal a finished task won't cause failure
        t->signal_waiters(); // signal_waiters may return false
        t->signal_waiters();
    }
};

TEST_F(task_test, init) { test_init(); }

TEST_F(task_test, null_task) { test_null_task(); }

TEST_F(task_test, signal_finished_task) { test_signal_finished_task(); }

} // namespace dsn
