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

#include "hpc_tail_logger.h"
#include <dsn/utility/singleton_store.h>
#include <dsn/utility/utils.h>
#include <dsn/tool-api/command_manager.h>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <iostream>

namespace dsn {
namespace tools {
struct tail_log_hdr
{
    uint32_t log_break; // '\0'
    uint32_t magic;
    int32_t length;
    uint64_t ts;
    tail_log_hdr *prev;

    bool is_valid() { return magic == 0xdeadbeef; }
};

struct __tail_log_info__
{
    uint32_t magic;
    char *buffer;
    char *next_write_ptr;
    tail_log_hdr *last_hdr;
};

typedef ::dsn::utils::safe_singleton_store<int, struct __tail_log_info__ *> tail_log_manager;

static __thread struct __tail_log_info__ s_tail_log_info;

hpc_tail_logger::hpc_tail_logger(const char *log_dir) : logging_provider(log_dir)
{
    _log_dir = std::string(log_dir);
    _per_thread_buffer_bytes =
        (int)dsn_config_get_value_uint64("tools.hpc_tail_logger",
                                         "per_thread_buffer_bytes",
                                         10 * 1024 * 1024, // 10 MB by default
                                         "buffer size for per-thread logging");

    static bool register_it = false;
    if (register_it) {
        return;
    }

    register_it = true;

    // register command for tail logging
    ::dsn::command_manager::instance().register_command(
        {"tail-log"},
        "tail-log keyword back-seconds [back-start-seconds = 0] [tid1,tid2,...]",
        "tail-log find logs with given keyword and within [now - back-seconds, now - "
        "back-start-seconds]",
        [this](const std::vector<std::string> &args) {
            if (args.size() < 2)
                return std::string("invalid arguments for tail-log command");
            else {
                std::unordered_set<int> target_threads;
                if (args.size() >= 4) {
                    std::list<std::string> tids;
                    ::dsn::utils::split_args(args[3].c_str(), tids, ',');
                    for (auto &t : tids) {
                        target_threads.insert(atoi(t.c_str()));
                    }
                }

                return this->search(args[0].c_str(),
                                    atoi(args[1].c_str()),
                                    args.size() >= 3 ? atoi(args[2].c_str()) : 0,
                                    target_threads);
            }
        });

    ::dsn::command_manager::instance().register_command(
        {"tail-log-dump"},
        "tail-log-dump",
        "tail-log-dump dump all tail logs to log files",
        [this](const std::vector<std::string> &args) {
            hpc_tail_logs_dumpper();
            return std::string(
                "logs are dumped to coredurmp dir started with hpc_tail_logs.xxx.log");
        });
}

hpc_tail_logger::~hpc_tail_logger(void) {}

void hpc_tail_logger::flush() { hpc_tail_logs_dumpper(); }

void hpc_tail_logger::hpc_tail_logs_dumpper()
{
    uint64_t nts = dsn_now_ns();
    std::stringstream log;
    log << _log_dir << "/hpc_tail_logs." << nts << ".log";

    std::ofstream olog(log.str().c_str());

    std::vector<int> threads;
    tail_log_manager::instance().get_all_keys(threads);

    for (auto &tid : threads) {
        __tail_log_info__ *log;
        if (!tail_log_manager::instance().get(tid, log))
            continue;

        tail_log_hdr *hdr = log->last_hdr, *tmp = log->last_hdr;
        while (tmp != nullptr && tmp != hdr) {
            if (!tmp->is_valid())
                break;

            char *llog = (char *)(tmp)-tmp->length;
            olog << llog << std::endl;

            // try previous log
            tmp = tmp->prev;
        };
    }

    olog.close();
}

std::string hpc_tail_logger::search(const char *keyword,
                                    int back_seconds,
                                    int back_start_seconds,
                                    std::unordered_set<int> &target_threads)
{
    uint64_t nts = dsn_now_ns();
    uint64_t start =
        nts - static_cast<uint64_t>(back_seconds) * 1000 * 1000 * 1000; // second to nanosecond
    uint64_t end =
        nts -
        static_cast<uint64_t>(back_start_seconds) * 1000 * 1000 * 1000; // second to nanosecond

    std::vector<int> threads;
    tail_log_manager::instance().get_all_keys(threads);

    std::stringstream ss;
    int log_count = 0;

    for (auto &tid : threads) {
        __tail_log_info__ *log;
        if (!tail_log_manager::instance().get(tid, log))
            continue;

        // filter by tid
        if (target_threads.size() > 0 && target_threads.find(tid) == target_threads.end())
            continue;

        tail_log_hdr *hdr = log->last_hdr, *tmp = log->last_hdr;
        do {
            if (!tmp->is_valid())
                break;

            // filter by time
            if (tmp->ts < start)
                break;

            if (tmp->ts > end) {
                tmp = tmp->prev;
                continue;
            }

            // filter by keyword
            char *llog = (char *)(tmp)-tmp->length;
            if (strstr(llog, keyword)) {
                ss << llog << std::endl;
                log_count++;
            }

            // try previous log
            tmp = tmp->prev;

        } while (tmp != nullptr && tmp != hdr);
    }

    char strb[24], stre[24];
    ::dsn::utils::time_ms_to_string(start / 1000000, strb);
    ::dsn::utils::time_ms_to_string(end / 1000000, stre);

    ss << "------------------------------------------" << std::endl;
    ss << "In total (" << log_count << ") log entries are found between [" << strb << ", " << stre
       << "] " << std::endl;

    return std::move(ss.str());
}

void hpc_tail_logger::dsn_logv(const char *file,
                               const char *function,
                               const int line,
                               dsn_log_level_t log_level,
                               const char *fmt,
                               va_list args)
{
    // init log buffer if necessary
    if (s_tail_log_info.magic != 0xdeadbeef) {
        s_tail_log_info.buffer = (char *)malloc(_per_thread_buffer_bytes);
        s_tail_log_info.next_write_ptr = s_tail_log_info.buffer;
        s_tail_log_info.last_hdr = nullptr;
        memset(s_tail_log_info.buffer, '\0', _per_thread_buffer_bytes);

        tail_log_manager::instance().put(::dsn::utils::get_current_tid(), &s_tail_log_info);
        s_tail_log_info.magic = 0xdeadbeef;
    }

    // get enough write space >= 1K
    if (s_tail_log_info.next_write_ptr + 1024 > s_tail_log_info.buffer + _per_thread_buffer_bytes) {
        s_tail_log_info.next_write_ptr = s_tail_log_info.buffer;
    }
    char *ptr = s_tail_log_info.next_write_ptr;
    char *ptr0 = ptr; // remember it
    size_t capacity = static_cast<size_t>(s_tail_log_info.buffer + _per_thread_buffer_bytes - ptr);

    // print verbose log header
    uint64_t ts = 0;
    int tid = ::dsn::utils::get_current_tid();
    if (::dsn::tools::is_engine_ready())
        ts = dsn_now_ns();
    char str[24];
    ::dsn::utils::time_ms_to_string(ts / 1000000, str);
    auto wn = sprintf(ptr, "%s (%" PRIu64 " %04x) ", str, ts, tid);
    ptr += wn;
    capacity -= wn;

    auto t = task::get_current_task_id();
    if (t) {
        if (nullptr != task::get_current_worker2()) {
            wn = sprintf(ptr,
                         "%6s.%7s%d.%016" PRIx64 ": ",
                         task::get_current_node_name(),
                         task::get_current_worker2()->pool_spec().name.c_str(),
                         task::get_current_worker2()->index(),
                         t);
        } else {
            wn = sprintf(ptr,
                         "%6s.%7s.%05d.%016" PRIx64 ": ",
                         task::get_current_node_name(),
                         "io-thrd",
                         tid,
                         t);
        }
    } else {
        wn = sprintf(ptr, "%6s.%7s.%05d: ", task::get_current_node_name(), "io-thrd", tid);
    }

    ptr += wn;
    capacity -= wn;

    // print body
    wn = std::vsnprintf(ptr, capacity, fmt, args);
    if (wn < 0) {
        wn = snprintf_p(ptr, capacity, "-- cannot printf due to that log entry has error ---");
    } else if (static_cast<unsigned>(wn) > capacity) {
        // log truncated
        wn = capacity;
    }

    ptr += wn;
    capacity -= wn;

    // set binary entry header on tail
    tail_log_hdr *hdr = (tail_log_hdr *)ptr;
    hdr->log_break = 0;
    hdr->length = 0;
    hdr->magic = 0xdeadbeef;
    hdr->ts = ts;
    hdr->length = static_cast<int>(ptr - ptr0);
    hdr->prev = s_tail_log_info.last_hdr;
    s_tail_log_info.last_hdr = hdr;

    ptr += sizeof(tail_log_hdr);
    capacity -= sizeof(tail_log_hdr);

    // set next write ptr
    s_tail_log_info.next_write_ptr = ptr;

    // dump critical logs on screen
    if (log_level >= LOG_LEVEL_WARNING) {
        std::cout << ptr0 << std::endl;
    }
}
}
}
