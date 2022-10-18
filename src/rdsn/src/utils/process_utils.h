// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <iostream>

#include "utils/ports.h"

namespace dsn {
namespace utils {
///
/// execute command in a seperate process,
/// read it's stdout to output
/// and return the retcode of command
///
int pipe_execute(const char *command, std::ostream &output);

///
/// process_mem_usage(double &, double &) - takes two doubles by reference,
/// attempts to read the system-dependent data for a process' virtual memory
/// size and resident set size, and return the results in KB.
///
/// On failure, returns 0.0, 0.0
///
void process_mem_usage(double &vm_usage, double &resident_set);

///
/// get the thread id.
/// for best performance, we cache the tid value
/// in the thread local variable
///
const int INVALID_TID = -1;

struct tls_tid
{
    unsigned int magic;
    int local_tid;
};
extern __thread tls_tid s_tid;

int get_current_tid_internal();

inline int get_current_tid()
{
    if (dsn_likely(s_tid.magic == 0xdeadbeef)) {
        return s_tid.local_tid;
    } else {
        s_tid.magic = 0xdeadbeef;
        s_tid.local_tid = get_current_tid_internal();
        return s_tid.local_tid;
    }
}

///
/// get the process start time.
/// please call these functions after the "main" function,
/// otherwise the return values are undefined.
///
uint64_t process_start_millis();
const char *process_start_date_time_mills();
}
}
