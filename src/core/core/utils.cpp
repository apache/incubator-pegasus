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

#include <dsn/utility/utils.h>
#include <dsn/utility/singleton.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <random>
#include <iostream>
#include <fstream>
#include <memory>
#include <array>

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#elif defined(__FreeBSD__)
#include <sys/thr.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace dsn {
namespace utils {

__thread tls_tid s_tid;
int get_current_tid_internal()
{
#if defined(_WIN32)
    return static_cast<int>(::GetCurrentThreadId());
#elif defined(__linux__)
    return static_cast<int>(syscall(SYS_gettid));
#elif defined(__FreeBSD__)
    long lwpid;
    thr_self(&lwpid);
    return static_cast<int>(lwpid);
#elif defined(__APPLE__)
    return static_cast<int>(pthread_mach_thread_np(pthread_self()));
#else
#error not implemented yet
#endif
}

uint64_t get_current_physical_time_ns()
{
    auto now = std::chrono::high_resolution_clock::now();
    auto nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return nanos;
}

// len >= 24
void time_ms_to_string(uint64_t ts_ms, char *str)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    sprintf(str,
            "%04d-%02d-%02d %02d:%02d:%02d.%03u",
            ret->tm_year + 1900,
            ret->tm_mon + 1,
            ret->tm_mday,
            ret->tm_hour,
            ret->tm_min,
            ret->tm_sec,
            static_cast<uint32_t>(ts_ms % 1000));
}

// len >= 11
void time_ms_to_date(uint64_t ts_ms, char *str, int len)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    strftime(str, len, "%Y-%m-%d", ret);
}

// len >= 20
void time_ms_to_date_time(uint64_t ts_ms, char *str, int len)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    strftime(str, len, "%Y-%m-%d %H:%M:%S", ret);
}

void time_ms_to_date_time(uint64_t ts_ms, int32_t &hour, int32_t &min, int32_t &sec)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    hour = ret->tm_hour;
    min = ret->tm_min;
    sec = ret->tm_sec;
}

int64_t get_unix_sec_today_midnight()
{
    time_t t = time(nullptr);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    ret->tm_hour = 0;
    ret->tm_min = 0;
    ret->tm_sec = 0;
    return static_cast<int64_t>(mktime(ret));
}

int hh_mm_to_seconds(dsn::string_view hhmm)
{
    int hour = 0, min = 0, sec = -1;
    if (::sscanf(hhmm.data(), "%d:%d", &hour, &min) == 2 && (0 <= hour && hour <= 23) &&
        (0 <= min && min <= 59)) {
        sec = 3600 * hour + 60 * min;
    }
    return sec;
}

int64_t hh_mm_today_to_unix_sec(string_view hhmm_of_day)
{
    int sec_of_day = hh_mm_to_seconds(hhmm_of_day);
    if (sec_of_day == -1) {
        return -1;
    }

    return get_unix_sec_today_midnight() + sec_of_day;
}

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
    using std::ios_base;
    using std::ifstream;
    using std::string;

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
}
}
