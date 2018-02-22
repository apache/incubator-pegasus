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

#pragma once

#include <functional>
#include <memory>

#define TIME_MS_MAX 0xffffffff

namespace dsn {
namespace utils {

template <typename T>
std::shared_ptr<T> make_shared_array(size_t size)
{
    return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

void time_ms_to_string(uint64_t ts_ms, char *str); // yyyy-MM-dd hh:mm:ss.SSS

void time_ms_to_date(uint64_t ts_ms, char *str, int len); // yyyy-MM-dd

void time_ms_to_date_time(uint64_t ts_ms, char *str, int len); // yyyy-MM-dd hh:mm:ss

void time_ms_to_date_time(uint64_t ts_ms,
                          int32_t &hour,
                          int32_t &min,
                          int32_t &sec); // time to hour, min, sec

uint64_t get_current_physical_time_ns();

extern int get_current_tid_internal();

typedef struct _tls_tid
{
    unsigned int magic;
    int local_tid;
} tls_tid;
extern __thread tls_tid s_tid;

inline int get_current_tid()
{
    if (s_tid.magic == 0xdeadbeef)
        return s_tid.local_tid;
    else {
        s_tid.magic = 0xdeadbeef;
        s_tid.local_tid = get_current_tid_internal();
        return s_tid.local_tid;
    }
}

inline int get_invalid_tid() { return -1; }

// execute command in a seperate process,
// read it's stdout to output
// and return the retcode of command
int pipe_execute(const char *command, std::ostream &output);
}
}
