/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include <rdsn/internal/task_queue.h>
# include "task_engine.h"
# include <rdsn/internal/perf_counters.h>
# include <cstdio>
# define __TITLE__ "task_queue"

namespace rdsn {

task_queue::task_queue(task_worker_pool* pool, int index, task_queue* inner_provider) : _pool(pool), _controller(nullptr)
{
    char num[30];
    sprintf(num, "%u", index);
    _name = pool->spec().name + '.';
    _name.append(num);
    _qps_counter = rdsn::utils::perf_counters::instance().get_counter((_name + std::string(".qps")).c_str(), COUNTER_TYPE_RATE, true);
}

//void task_queue::on_dequeue(int count)
//{
//    _qps_counter->add((unsigned long long)count);
//    _pool->on_dequeue(count);
//}

}
