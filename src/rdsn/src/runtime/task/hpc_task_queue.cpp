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

#include "hpc_task_queue.h"
#include <boost/function_output_iterator.hpp>

namespace dsn {
namespace tools {

hpc_concurrent_task_queue::hpc_concurrent_task_queue(task_worker_pool *pool,
                                                     int index,
                                                     task_queue *inner_provider)
    : task_queue(pool, index, inner_provider)
{
}

void hpc_concurrent_task_queue::enqueue(task *task)
{
    _queues[task->spec().priority].q.enqueue(task);
    _sema.signal(1);
}

task *hpc_concurrent_task_queue::dequeue(int &batch_size)
{
    batch_size = _sema.waitMany(batch_size);
    if (batch_size == 0) {
        return nullptr;
    }
    task *head = nullptr, *last = nullptr;
    auto out = boost::make_function_output_iterator([&head, &last](task *in) {
        if (last) {
            last->next = in;
        } else {
            head = in;
        }

        last = in;
        last->next = nullptr;
    });
    auto count = batch_size;
    do {
        for (auto &qs : _queues) {
            count -= qs.q.try_dequeue_bulk(out, count);
            if (count == 0) {
                break;
            }
        }
    } while (count != 0);
    return head;
}
}
}
