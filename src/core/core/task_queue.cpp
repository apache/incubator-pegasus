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
    _appro_count = 0;
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
    // only apply admission control on rpc request typed tasks
    if (TASK_TYPE_RPC_REQUEST == task->spec().type)
    {
        int ac_value = 0;
        if (_enable_virtual_queue_throttling)
        {
            ac_value = _virtual_queue_length;
        }
        else
        {
            ac_value = approx_count();
        }

        _delayer.delay(ac_value);

        //auto controller = _controllers[idx];
        //if (controller != nullptr)
        //{
        //    int i = 0;
        //    while (!controller->is_task_accepted(t))
        //    {
        //        // any customized rejection handler?
        //        if (t->spec().rejection_handler != nullptr)
        //        {
        //            t->spec().rejection_handler(t, controller);

        //            ddebug("task %s (%016llx) is rejected",
        //                t->spec().name.c_str(),
        //                t->id()
        //                );

        //            return;
        //        }

        //        if (++i % 1000 == 0)
        //        {
        //            dwarn("task queue %s cannot accept new task now, size = %d",
        //                q->get_name().c_str(), q->approx_count());
        //        }
        //        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        //    }
        //}
        //else if (t->spec().type == TASK_TYPE_RPC_REQUEST
        //    && _spec.queue_length_throttling_threshold != 0x0FFFFFFFUL)
        //{
        //    int i = 0;
        //    while (q->approx_count() >= _spec.queue_length_throttling_threshold)
        //    {
        //        // any customized rejection handler?
        //        if (t->spec().rejection_handler != nullptr)
        //        {
        //            t->spec().rejection_handler(t, controller);

        //            ddebug("task %s (%016llx) is rejected because the target queue is full",
        //                t->spec().name.c_str(),
        //                t->id()
        //                );

        //            return;
        //        }

        //        if (++i % 1000 == 0)
        //        {
        //            dwarn("task queue %s cannot accept new task now, size = %d",
        //                q->get_name().c_str(), q->approx_count());
        //        }
        //        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        //    }
        //}
    }

    increase_count();
    enqueue(task);
}

}
