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
# include "simple_task_queue.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task.queue.simple"

namespace dsn 
{
    namespace tools
    {
        simple_timer_service::simple_timer_service(service_node* node, timer_service* inner_provider)
            : timer_service(node, inner_provider)
        {
            _worker = std::shared_ptr<std::thread>(new std::thread([this, node]()
            {
                task_worker::set_name("timer");
                task_worker::set_priority(worker_priority_t::THREAD_xPRIORITY_ABOVE_NORMAL);

                task::set_current_worker(nullptr, node);
                boost::asio::io_service::work work(_ios);
                _ios.run();
            }));
        }

        void simple_timer_service::add_timer(task* task)
        {
            std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(_ios));
            timer->expires_from_now(boost::posix_time::milliseconds(task->delay_milliseconds()));
            task->set_delay(0);

            timer->async_wait([this, task, timer](const boost::system::error_code& ec)
            {
                if (!ec)
                {
                    task->enqueue();
                }
                else
                {
                    dfatal("delayed execution failed for task %s, err = %u",
                        task->spec().name.c_str(), ec.value());
                }

                // to consume the added ref count by another task::enqueue
                task->release_ref();
            });
        }

        simple_task_queue::simple_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
            : task_queue(pool, index, inner_provider), _samples("")
        {
        }

        void simple_task_queue::enqueue(task* task)
        {
            _samples.enqueue(task, task->spec().priority);
        }

        task* simple_task_queue::dequeue()
        {
            long c = 0;
            auto t = _samples.dequeue(c);
            dassert(t != nullptr, "dequeue does not return empty tasks");
            return t;
        }

        int      simple_task_queue::count() const
        {
            return _samples.count();
        }
    }
}
