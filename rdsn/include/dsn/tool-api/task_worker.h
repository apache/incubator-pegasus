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
 *     task worker (thread) abstraction
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/tool-api/task_queue.h>
#include <dsn/utility/extensible_object.h>
#include <dsn/utility/synchronize.h>
#include <dsn/utility/dlib.h>
#include <dsn/perf_counter/perf_counter.h>
#include <thread>

namespace dsn {

/*!
@addtogroup tool-api-providers
@{
*/
/*!
 task worker processes the input tasks from the bound task queue
*/
class task_worker : public extensible_object<task_worker, 4>
{
public:
    template <typename T>
    static task_worker *
    create(task_worker_pool *pool, task_queue *q, int index, task_worker *inner_provider)
    {
        return new T(pool, q, index, inner_provider);
    }

    typedef task_worker *(*factory)(task_worker_pool *, task_queue *, int, task_worker *);

public:
    DSN_API
    task_worker(task_worker_pool *pool, task_queue *q, int index, task_worker *inner_provider);
    DSN_API virtual ~task_worker(void);

    // service management
    DSN_API void start();
    DSN_API void stop();

    DSN_API virtual void loop(); // run tasks from _input_queue

    // inquery
    const std::string &name() const { return _name; }
    int index() const { return _index; }
    int native_tid() const { return _native_tid; }
    task_worker_pool *pool() const { return _owner_pool; }
    task_queue *queue() const { return _input_queue; }
    DSN_API const threadpool_spec &pool_spec() const;
    DSN_API static task_worker *current();

private:
    task_worker_pool *_owner_pool;
    task_queue *_input_queue;
    int _index;
    int _native_tid;
    std::string _name;
    std::unique_ptr<std::thread> _thread;
    bool _is_running;
    utils::notify_event _started;
    int _processed_task_count;

public:
    DSN_API static void set_name(const char *name);
    DSN_API static void set_priority(worker_priority_t pri);
    DSN_API static void set_affinity(uint64_t affinity);

private:
    void run_internal();

public:
    /*!
    @addtogroup tool-api-hooks
    @{
    */
    DSN_API static join_point<void, task_worker *> on_start;
    DSN_API static join_point<void, task_worker *> on_create;
    /*@}*/
};
/*@}*/
} // end namespace
