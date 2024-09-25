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

#include <algorithm>
#include <thread>

#include "boost/asio/io_service.hpp"
#include "task_code.h"
#include "task_queue.h"
#include "timer_service.h"
#include "utils/priority_queue.h"

namespace dsn {
class service_node;
class task;
class task_worker_pool;

namespace tools {
class simple_task_queue : public task_queue
{
public:
    simple_task_queue(task_worker_pool *pool, int index, task_queue *inner_provider);

    ~simple_task_queue() override = default;

    virtual void enqueue(task *task) override;
    virtual task *dequeue(/*inout*/ int &batch_size) override;

private:
    typedef utils::blocking_priority_queue<task *, TASK_PRIORITY_COUNT> tqueue;
    tqueue _samples;
};

class simple_timer_service : public timer_service
{
public:
    simple_timer_service(service_node *node, timer_service *inner_provider);

    ~simple_timer_service() override { stop(); }

    // after milliseconds, the provider should call task->enqueue()
    virtual void add_timer(task *task) override;

    virtual void start() override;

    virtual void stop() override;

private:
    boost::asio::io_service _ios;
    std::thread _worker;
    bool _is_running;
};

} // namespace tools
} // namespace dsn
