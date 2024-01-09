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

#include "task.h"

namespace dsn {
class service_node;

/*!
@addtogroup tool-api-providers
@{
*/
/*!
  timer service schedules the input tasks at specified timepoint
*/
class timer_service
{
public:
    template <typename T>
    static timer_service *create(service_node *node, timer_service *inner_provider)
    {
        return new T(node, inner_provider);
    }

    typedef timer_service *(*factory)(service_node *, timer_service *);

public:
    timer_service(service_node *node, timer_service *inner_provider) { _node = node; }

    virtual ~timer_service() = default;

    virtual void start() = 0;
    virtual void stop() = 0;

    // after milliseconds, the provider should call task->enqueue()
    virtual void add_timer(task *task) = 0;

    // inquery
    service_node *node() const { return _node; }

private:
    service_node *_node;
};
/*@}*/
} // end namespace
