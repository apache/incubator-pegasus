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
# pragma once

# include <dsn/internal/task.h>

namespace dsn {

class task_worker_pool;
class admission_controller;

class task_queue
{
public:
    template <typename T> static task_queue* create(task_worker_pool* pool, int index, task_queue* inner_provider)
    {
        return new T(pool, index, inner_provider);
    }

public:
    task_queue(task_worker_pool* pool, int index, task_queue* inner_provider); 
    ~task_queue() {}
    
    virtual void     enqueue(task_ptr& task) = 0;
    virtual task_ptr dequeue() = 0;
    virtual int      count() const = 0;

    const std::string & get_name() { return _name; }    
    task_worker_pool* pool() const { return _pool; }
    perf_counter_ptr& get_qps_counter() { return _qps_counter; }
    admission_controller* controller() const { return _controller; }
    void set_controller(admission_controller* controller) { _controller = controller; }

private:
    task_worker_pool*      _pool;
    std::string            _name;
    perf_counter_ptr       _qps_counter;
    admission_controller*  _controller;
};

} // end namespace
