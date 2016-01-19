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
# include <cstdio>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task_queue"

namespace dsn {

task_queue::task_queue(task_worker_pool* pool, int index, task_queue* inner_provider) : _pool(pool), _controller(nullptr)
{
    char num[30];
    sprintf(num, "%u", index);
    _index = index;
    _name = pool->spec().name + '.';
    _name.append(num);
    _owner_worker = nullptr;
    _worker_count = _pool->spec().partitioned ? 1 : _pool->spec().worker_count;
    _queue_length = 0;
    _virtual_queue_length = 0;
    _enable_virtual_queue_throttling = pool->spec().enable_virtual_queue_throttling;
    if (pool->spec().throttling_delay_vector_milliseconds.size() > 0)
    {
        _delayer.initialize(
            pool->spec().throttling_delay_vector_milliseconds,
            pool->spec().queue_length_throttling_threshold
            );
    }
    else
    {
        _delayer.initialize(
            pool->spec().queue_length_throttling_threshold
            );
    }
}

void task_queue::enqueue_internal(task* task)
{
    if (task->spec().rpc_allow_throttling)
    {
        int ac_value = 0;
        if (_enable_virtual_queue_throttling)
        {
            ac_value = _virtual_queue_length;
        }
        else
        {
            ac_value = count();
        }

        int dms = _delayer.delay(ac_value);
        if (dms > 0)
        {
            rpc_request_task* rc = dynamic_cast<rpc_request_task*>(task);
            rpc_session* s = rc->get_request()->io_session.get();
            if (s != nullptr)
            {
                // delay session recv
                s->delay_rpc_request_rate(dms);
            }
        }
    }

    increase_count();
    enqueue(task);
}

}
