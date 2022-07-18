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

#include "simple_task_queue.h"

namespace dsn {
namespace tools {

simple_timer_service::simple_timer_service(service_node *node, timer_service *inner_provider)
    : timer_service(node, inner_provider), _is_running(false)
{
}

void simple_timer_service::start()
{
    if (_is_running) {
        return;
    }

    _worker = std::thread([this]() {
        task::set_tls_dsn_context(node(), nullptr);

        char buffer[128];
        sprintf(buffer, "%s.timer", get_service_node_name(node()));

        task_worker::set_name(buffer);
        task_worker::set_priority(worker_priority_t::THREAD_xPRIORITY_ABOVE_NORMAL);

        boost::asio::io_service::work work(_ios);
        boost::system::error_code ec;
        _ios.run(ec);
        if (ec) {
            dassert(
                false, "io_service in simple_timer_service run failed: %s", ec.message().data());
        }
    });
    _is_running = true;
}

void simple_timer_service::stop()
{
    if (!_is_running) {
        return;
    }

    _ios.stop();
    _worker.join();
    _is_running = false;
}

void simple_timer_service::add_timer(task *task)
{
    std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(_ios));
    timer->expires_from_now(boost::posix_time::milliseconds(task->delay_milliseconds()));
    task->set_delay(0);

    timer->async_wait([task, timer](const boost::system::error_code &ec) {
        if (!ec) {
            task->enqueue();
        } else if (ec != ::boost::asio::error::operation_aborted) {
            dfatal("timer failed for task %s, err = %u", task->spec().name.c_str(), ec.value());
        }

        // to consume the added ref count by task::enqueue for add_timer
        task->release_ref();
    });
}

simple_task_queue::simple_task_queue(task_worker_pool *pool, int index, task_queue *inner_provider)
    : task_queue(pool, index, inner_provider), _samples("")
{
}

void simple_task_queue::enqueue(task *task) { _samples.enqueue(task, task->spec().priority); }

// always return 1 or 0 task so far
task *simple_task_queue::dequeue(/*inout*/ int &batch_size)
{
    long c = 0;
    auto t = _samples.dequeue(c);
    dassert(t != nullptr, "dequeue does not return empty tasks");
    batch_size = 1;
    return t;
}

} // namespace tools
} // namespace dsn
