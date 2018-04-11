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
 *     Rpc performance test
 *
 * Revision history:
 *     2016-01-05, Tianyi Wang, first version
 */

#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/utility/priority_queue.h>
#include <dsn/tool-api/group_address.h>
#include <boost/lexical_cast.hpp>
#include "test_utils.h"
#include <mutex>
#include <condition_variable>

// worker = 1
DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_TASK_QUEUE_1);
// worker = 1
DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_TASK_QUEUE_2);
DEFINE_TASK_CODE(LPC_TEST_TASK_QUEUE_1, TASK_PRIORITY_HIGH, THREAD_POOL_TEST_TASK_QUEUE_1)
DEFINE_TASK_CODE(LPC_TEST_TASK_QUEUE_2, TASK_PRIORITY_HIGH, THREAD_POOL_TEST_TASK_QUEUE_2)

struct auto_timer
{
    std::string prefix;
    uint64_t delivery;
    std::chrono::steady_clock clock;
    decltype(std::chrono::steady_clock::now()) start_time;
    std::vector<task_ptr> waited_task;
    auto_timer(const std::string &prefix, uint64_t delivery) : prefix(prefix), delivery(delivery)
    {
        start_time = clock.now();
    }
    void wait_task(task_ptr ptask) { waited_task.push_back(ptask); }
    ~auto_timer()
    {
        for (auto task : waited_task) {
            task->wait();
        }
        auto end_time = clock.now();
        std::cout << prefix << "throughput = "
                  << delivery * 1000 * 1000 /
                         std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                               start_time)
                             .count()
                  << std::endl;
    }
};

void empty_cb(void *) {}
struct self_iterate_context
{
    std::mutex mut;
    std::condition_variable cv;
    bool done = false;
    std::vector<task_c *> tsks;
    std::vector<task_c *>::iterator it;
};
void iterate_over_preallocated_tasks(void *ctx)
{
    auto context = reinterpret_cast<self_iterate_context *>(ctx);
    if (context->it != context->tsks.end()) {
        auto it_save = context->it;
        ++context->it;
        (*it_save)->enqueue();
    } else {
        {
            std::lock_guard<std::mutex> _(context->mut);
            context->done = true;
        }
        context->cv.notify_one();
    }
}

void external_flooding(const int enqueue_time)
{
    std::vector<task_c *> tsks;
    for (int i = 0; i < enqueue_time; i++) {
        auto tsk = new task_c(LPC_TEST_TASK_QUEUE_1, empty_cb, nullptr, nullptr);
        tsks.push_back(tsk);
    }
    {
        auto_timer t("inter-thread flooding test:", enqueue_time);
        for (auto tsk : tsks) {
            if (tsk == tsks.back()) {
                tsk->add_ref();
            }
            tsk->enqueue();
        }
        tsks.back()->wait();
        tsks.back()->release_ref();
    }
}

void self_flooding(const int enqueue_time)
{
    std::vector<task_c *> tsks;
    for (int i = 0; i < enqueue_time; i++) {
        auto tsk = new task_c(LPC_TEST_TASK_QUEUE_1, empty_cb, nullptr, nullptr);
        tsks.push_back(tsk);
    }
    {
        auto_timer t("self-flooding test:", enqueue_time);
        tasking::enqueue(LPC_TEST_TASK_QUEUE_1, nullptr, [&]() {
            for (auto tsk : tsks) {
                if (tsk == tsks.back()) {
                    tsk->add_ref();
                }
                tsk->enqueue();
            }
            t.start_time = t.clock.now();
        });
        tsks.back()->wait();
        tsks.back()->release_ref();
    }
}
void external_blocking(const int enqueue_time)
{
    std::vector<task_c *> tsks;
    for (int i = 0; i < enqueue_time; i++) {
        auto tsk = new task_c(LPC_TEST_TASK_QUEUE_1, empty_cb, nullptr, nullptr);
        tsks.push_back(tsk);
    }
    {
        auto_timer t("inter-thread blocking test:", enqueue_time);
        for (auto tsk : tsks) {
            tsk->add_ref();
            tsk->enqueue();
            tsk->wait();
            tsk->release_ref();
        }
    }
}
void self_iterating(const int enqueue_time)
{
    self_iterate_context ctx;
    for (int i = 0; i < enqueue_time; i++) {
        auto tsk =
            new task_c(LPC_TEST_TASK_QUEUE_1, iterate_over_preallocated_tasks, &ctx, nullptr);
        ctx.tsks.push_back(tsk);
    }
    ctx.it = ctx.tsks.begin();
    {
        auto_timer t("self-iterating test:", enqueue_time);
        iterate_over_preallocated_tasks(&ctx);
        std::unique_lock<std::mutex> _lk(ctx.mut);
        ctx.cv.wait(_lk, [&] { return ctx.done; });
    }
}
void tic_tock_iterating(const int enqueue_time)
{
    self_iterate_context ctx;
    for (int i = 0; i < enqueue_time; i++) {
        auto tsk = new task_c(i % 2 == 0 ? LPC_TEST_TASK_QUEUE_1 : LPC_TEST_TASK_QUEUE_2,
                              iterate_over_preallocated_tasks,
                              &ctx,
                              nullptr);
        ctx.tsks.push_back(tsk);
    }
    ctx.it = ctx.tsks.begin();
    {
        auto_timer t("tick-tock test:", enqueue_time);
        iterate_over_preallocated_tasks(&ctx);
        std::unique_lock<std::mutex> _lk(ctx.mut);
        ctx.cv.wait(_lk, [&] { return ctx.done; });
    }
}
TEST(core, task_queue_perf_test)
{
    const int enqueue_time = 10000000;
    external_flooding(enqueue_time);
    self_flooding(enqueue_time);
    external_blocking(enqueue_time / 10);
    self_iterating(enqueue_time);
    tic_tock_iterating(enqueue_time / 10);
}
