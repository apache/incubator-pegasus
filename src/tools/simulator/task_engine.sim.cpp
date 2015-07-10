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
#include "task_engine.sim.h"
#include "scheduler.h"

namespace dsn { namespace tools {
    
sim_task_queue::sim_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
: task_queue(pool, index, inner_provider)
{
}

void sim_task_queue::enqueue(task* t)
{
    if (0 == t->delay_milliseconds())    
    {
        if (_tasks.size() > 0)
        {
            do {
                int random_pos = ::dsn::service::env::random32(0, 1000000);
                auto pr = _tasks.insert(std::map<uint32_t, task*>::value_type(random_pos, t));
                if (pr.second) break;
            } while (true);
        }
        else
        {
            int random_pos = ::dsn::service::env::random32(0, 1000000);
            _tasks.insert(std::map<uint32_t, task*>::value_type(random_pos, t));
        }
    }
    else
    {
        scheduler::instance().add_task(t, this);
    }
}

task* sim_task_queue::dequeue()
{
    scheduler::instance().wait_schedule(false);

    if (_tasks.size() > 0)
    {
        auto t = _tasks.begin()->second;
        _tasks.erase(_tasks.begin());
        return t;
    }
    else
    {
        return nullptr;
    }
}

void sim_semaphore_provider::signal(int count)
{
    _count += count;
    
    while (!_wait_threads.empty() && _count > 0)
    {
        --_count;

        sim_worker_state* thread = _wait_threads.front();
        _wait_threads.pop_front();
        thread->is_continuation_ready = true;
    }
}

bool sim_semaphore_provider::wait(int timeout_milliseconds)
{
    if (_count > 0)
    {
        --_count;
        scheduler::instance().wait_schedule(true, true);
        return true;
    }
    else
    {
        _wait_threads.push_back(scheduler::task_worker_ext::get(task::get_current_worker()));
        scheduler::instance().wait_schedule(true, false);
        return true;
    }
}

}} // end namespace
