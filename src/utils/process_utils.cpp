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

#include <stdio.h>
#if defined(__linux__)
#include <syscall.h>
#else
#include <sys/syscall.h>
#endif
#include <unistd.h>
#include <array>
#include <fstream> // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <string>

#include "utils/preloadable.h"
#include "utils/process_utils.h"
#include "utils/time_utils.h"

using std::ifstream;
using std::ios_base;
using std::string;

namespace dsn {
namespace utils {

__thread tls_tid s_tid;
int get_current_tid_internal() { return static_cast<int>(syscall(SYS_gettid)); }

int pipe_execute(const char *command, std::ostream &output)
{
    std::array<char, 256> buffer;
    int retcode = 0;

    {
        std::shared_ptr<FILE> command_pipe(popen(command, "r"),
                                           [&retcode](FILE *p) { retcode = pclose(p); });
        while (!feof(command_pipe.get())) {
            if (fgets(buffer.data(), 256, command_pipe.get()) != NULL)
                output << buffer.data();
        }
    }
    return retcode;
}

void process_mem_usage(double &vm_usage, double &resident_set)
{
    vm_usage = 0.0;
    resident_set = 0.0;

    // 'file' stat seems to give the most reliable results
    //
    ifstream stat_stream("/proc/self/stat", ios_base::in);

    // dummy vars for leading entries in stat that we don't care about
    //
    string pid, comm, state, ppid, pgrp, session, tty_nr;
    string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    string utime, stime, cutime, cstime, priority, nice;
    string O, itrealvalue, starttime;

    // the two fields we want
    //
    unsigned long vsize;
    long rss;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >>
        minflt >> cminflt >> majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >>
        nice >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

    stat_stream.close();

    static long page_size_kb =
        sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}

class record_process_start_time : public preloadable<record_process_start_time>
{
public:
    record_process_start_time()
    {
        mills = get_current_physical_time_ns() / 1000000;
        time_ms_to_string(mills, date_time_mills);
    }
    uint64_t mills = 0;
    char date_time_mills[64] = {0};
};

//
// if you call these functions before "main" function,
// the memory space for these variables have been allocated,
// but the values aren't initialized as the constructor
// of "static_module" may not been called yet.
//
uint64_t process_start_millis() { return record_process_start_time::s_instance.mills; }
const char *process_start_date_time_mills()
{
    return record_process_start_time::s_instance.date_time_mills;
}
} // namespace utils
} // namespace dsn
