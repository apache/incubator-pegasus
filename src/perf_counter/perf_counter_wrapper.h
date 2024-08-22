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

#include "perf_counter.h"
#include "perf_counters.h"

namespace dsn {

//
// perf_counter_wrapper is a wrapper class for perf-counter operations, users should use this class
// instead of the dsn::perf_counter where a performance counter is needed.
//
// for example:
// class A{
// public:
//   A() {
//      _p1.init_global_counter(...)
//      _p2.init_app_counter(...)
//   }
// private:
//   perf_counter_wrapper _p1;
//   perf_counter_wrapper _p2;
// };
//
// user should call init_global_counter/init_app_counter to initialize the counter.
// all the initialized counters are stored in the singleton dsn::perf_counters,
// users can collect all counters of the process and intergrate it with monitor system.
//
class perf_counter_wrapper
{
public:
    perf_counter_wrapper() { _counter = nullptr; }

    perf_counter_wrapper(const perf_counter_wrapper &other) = delete;
    perf_counter_wrapper(perf_counter_wrapper &other) = delete;
    perf_counter_wrapper(perf_counter_wrapper &&other) = delete;
    perf_counter_wrapper &operator=(const perf_counter_wrapper &other) = delete;
    perf_counter_wrapper &operator=(perf_counter_wrapper &other) = delete;
    perf_counter_wrapper &operator=(perf_counter_wrapper &&other) = delete;

    ~perf_counter_wrapper() {}

    // clear the real perf-counter object.
    // call this function if you want free the counter before the wrapper's destructor is called
    void clear()
    {
        if (nullptr != _counter) {
            dsn::perf_counters::instance().remove_counter(_counter->full_name());
            _counter = nullptr;
        }
    }

    // init app counter create counters for some specific service_app, so different
    // service_app can create counter with the same name.
    void init_app_counter(const char *section,
                          const char *name,
                          dsn_perf_counter_type_t type,
                          const char *dsptr)
    {
        dsn::perf_counter_ptr c =
            dsn::perf_counters::instance().get_app_counter(section, name, type, dsptr, true);
        clear();
        _counter = c.get();
    }

    // init global counter create counters globally.
    void init_global_counter(const char *app,
                             const char *section,
                             const char *name,
                             dsn_perf_counter_type_t type,
                             const char *dsptr)
    {
        dsn::perf_counter_ptr c = dsn::perf_counters::instance().get_global_counter(
            app, section, name, type, dsptr, true);
        clear();
        _counter = c.get();
    }

    dsn::perf_counter *get() const { return _counter; }
    dsn::perf_counter *operator->() const { return _counter; }

private:
    // use raw pointer to make the class object small, so it can be accessed quickly
    dsn::perf_counter *_counter;
};
} // namespace dsn
