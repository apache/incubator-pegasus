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
 *     define the interface of admissio controllers which is used to throttle the
 *     traffic into the task thread pools
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/tool-api/task_queue.h>
#include <dsn/utility/utils.h>

namespace dsn {

class admission_controller
{
public:
    template <typename T>
    static admission_controller *create(task_queue *q, const char *args);
    typedef admission_controller *(*factory)(task_queue *, const char *);

public:
    admission_controller(task_queue *q, std::vector<std::string> &sargs) : _queue(q) {}
    virtual ~admission_controller() {}

    virtual bool is_task_accepted(task *task) = 0;

    task_queue *bound_queue() const { return _queue; }

private:
    task_queue *_queue;
};

// ----------------- inline implementation -----------------
template <typename T>
admission_controller *admission_controller::create(task_queue *q, const char *args)
{
    std::vector<std::string> sargs;
    dsn::utils::split_args(args, sargs, ' ');
    return new T(q, sargs);
}

} // end namespace
