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

#include <atomic>
#include <string>

#include "utils/autoref_ptr.h"
#include "utils/metrics.h"

namespace dsn {

class task;
class task_worker_pool;
struct threadpool_spec;

/*!
@addtogroup tool-api-providers
@{
*/
/*!
  task queue batches the input queue for the bound task worker(s) (threads)
 */
class task_queue
{
public:
    template <typename T>
    static task_queue *create(task_worker_pool *pool, int index, task_queue *inner_provider)
    {
        return new T(pool, index, inner_provider);
    }

    typedef task_queue *(*factory)(task_worker_pool *, int, task_queue *);

public:
    task_queue(task_worker_pool *pool, int index, task_queue *inner_provider);
    virtual ~task_queue();

    virtual void enqueue(task *task) = 0;
    // dequeue may return more than 1 tasks, but there is a configured
    // best batch size for each worker so that load among workers
    // are balanced,
    // returned batch size is stored in parameter batch_size
    virtual task *dequeue(/*inout*/ int &batch_size) = 0;

    int count() const { return _queue_length.load(std::memory_order_relaxed); }
    int decrease_count(int count = 1)
    {
        METRIC_VAR_DECREMENT_BY(queue_length, count);
        return _queue_length.fetch_sub(count, std::memory_order_relaxed) - count;
    }
    int increase_count(int count = 1)
    {
        METRIC_VAR_INCREMENT_BY(queue_length, count);
        return _queue_length.fetch_add(count, std::memory_order_relaxed) + count;
    }
    const std::string &get_name() { return _name; }
    task_worker_pool *pool() const { return _pool; }
    int index() const { return _index; }
    volatile int *get_virtual_length_ptr() { return &_virtual_queue_length; }

private:
    friend class task_worker_pool;
    void enqueue_internal(task *task);

    const metric_entity_ptr &queue_metric_entity() const;

private:
    task_worker_pool *_pool;
    std::string _name;
    int _index;
    std::atomic<int> _queue_length;
    threadpool_spec *_spec;
    volatile int _virtual_queue_length;

    const metric_entity_ptr _queue_metric_entity;
    METRIC_VAR_DECLARE_gauge_int64(queue_length);
    METRIC_VAR_DECLARE_counter(queue_delayed_tasks);
    METRIC_VAR_DECLARE_counter(queue_rejected_tasks);
};
/*@}*/
} // namespace dsn
