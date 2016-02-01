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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/internal/task_queue.h>
# include "task_engine.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/network.h>
# include <dsn/internal/perf_counters.h>
# include <cstdio>
# include "rpc_engine.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task_queue"

namespace dsn {

task_queue::task_queue(task_worker_pool* pool, int index, task_queue* inner_provider) : _pool(pool), _controller(nullptr), _queue_length(0)
{
    char num[30];
    sprintf(num, "%u", index);
    _index = index;
    _name = pool->spec().name + '.';
    _name.append(num);
    _owner_worker = nullptr;
    _worker_count = _pool->spec().partitioned ? 1 : _pool->spec().worker_count;
    _queue_length_counter = perf_counters::instance().get_counter(_pool->node()->name(), "engine", (_name + ".queue.length").c_str(), COUNTER_TYPE_NUMBER, "task queue length", true);
    _virtual_queue_length = 0;
    _spec = (threadpool_spec*)&pool->spec();

    for (int i = 0; i < TASK_PRIORITY_COUNT; i++)
    {
        _appro_wait_time_ns[i].store(0);
    }
}

task_queue::~task_queue()
{
    perf_counters::instance().remove_counter(_queue_length_counter->full_name());
}

void task_queue::enqueue_internal(task* task)
{
    auto& sp = task->spec();
    if (sp.rpc_request_dropped_on_timeout_with_high_possibility)
    {
        rpc_request_task* rc = dynamic_cast<rpc_request_task*>(task);

        // drop incoming 
        if (_appro_wait_time_ns[sp.priority].load(std::memory_order_relaxed) / 1000000ULL
            >= (uint64_t)rc->get_request()->header->client.timeout_ms)
        {
            // TODO: reply busy
            task->release_ref(); // added in task::enqueue(pool)
            return;
        }
    }

    if (sp.rpc_request_dropped_on_long_queue)
    {        
        int ac_value = 0;
        if (_spec->enable_virtual_queue_throttling)
        {
            ac_value = _virtual_queue_length;
        }
        else
        {
            ac_value = count();
        }

        // drop incoming rpc request when task queue is too long
        if (ac_value > _spec->queue_length_throttling_threshold)
        {
            // TODO: reply busy
            task->release_ref(); // added in task::enqueue(pool)
            return;
        }
    }

    tls_dsn.last_worker_queue_size = increase_count();
    enqueue(task);
}

}
