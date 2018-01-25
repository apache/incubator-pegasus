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

/************************************************************
*   hpc_logger (High-Performance Computing logger)
*
*   Editor: Chang Lou (v-chlou@microsoft.com)
*
*
*   The structure of the logger is like the following graph.
*
*   For each thread:
*   -------------------------              -------------------------
*   |    new single log     |     ->       |                       |
*   -------------------------              | --------------------- |
*                                          |                       |
*                                          | --------------------- |
*                                          |                       |
*                                          | --------------------- |
*                                          |                       |
*                                          | --------------------- |
*                                                    ...
*                                          | --------------------- |
*                                          |                       |
*                                          -------------------------
*                                             buffer (per thread)
*                                                      |
*                                                      |   when the buffer is full,
*                                                      |   push the buffer and buffer size into
_write_list,
*                                                      |   malloc a new buffer for the thread to use
*                                                      V
*
==========================================================================================================
_write_list_lock
*
* -------------------------------------------------------------
*                                                {buf1, buf1_size} | {buf2, buf2_size} | {buf3,
buf3_size} | ...
* -------------------------------------------------------------
*                                                              _write_list
*
*
==========================================================================================================
_write_list_lock
*                                                                   |
*                                                                   |   when the _write_list is not
empty,
*                                                                   |   daemon thread is notified by
_write_list_cond
*                                                                    V
*
*                                                             Daemon thread
*
*                                                                   ||
*                                                                   ===========>     log.x.txt
*
*    Some other facts:
*    1. The log file size is restricted, when max size is achieved, a new log file will be
established.
*    2. When exiting, the logger flushes, in other words, print out the retained log info in buffers
of each thread and buffers in the buffer list.

************************************************************/

#include "hpc_logger.h"
#include <dsn/utility/singleton_store.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/command_manager.h>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <iostream>

#define MAX_FILE_SIZE 30 * 1024 * 1024
namespace dsn {
namespace tools {
typedef struct __hpc_log_info__
{
    uint32_t magic;
    char *buffer;
    char *next_write_ptr;
} hpc_log_tls_info;

// log ptr for each thread
static __thread hpc_log_tls_info s_hpc_log_tls_info;

// store log ptr for each thread
typedef ::dsn::utils::safe_singleton_store<int, hpc_log_tls_info *> hpc_log_manager;

// daemon thread
void hpc_logger::log_thread()
{
    std::vector<buffer_info> saved_list;

    while (!_stop_thread) {
        _write_list_lock.lock();
        _write_list_cond.wait(_write_list_lock,
                              [=] { return _stop_thread || _write_list.size() > 0; });
        saved_list = std::move(_write_list);
        _write_list.clear();
        _is_writing = true;
        _write_list_lock.unlock();

        write_buffer_list(saved_list);
        _is_writing = false;
    }

    _write_list_lock.lock();
    saved_list = _write_list;
    _write_list.clear();
    _is_writing = true;
    _write_list_lock.unlock();

    write_buffer_list(saved_list);
    _is_writing = false;
}

hpc_logger::hpc_logger(const char *log_dir)
    : logging_provider(log_dir), _stop_thread(false), _is_writing(false)
{
    _log_dir = std::string(log_dir);
    _per_thread_buffer_bytes =
        (int)dsn_config_get_value_uint64("tools.hpc_logger",
                                         "per_thread_buffer_bytes",
                                         64 * 1024, // 64 KB by default
                                         "buffer size for per-thread logging");
    _max_number_of_log_files_on_disk = dsn_config_get_value_uint64(
        "tools.hpc_logger",
        "max_number_of_log_files_on_disk",
        20,
        "max number of log files reserved on disk, older logs are auto deleted");

    _start_index = 0;
    _index = 1;
    _current_log_file_bytes = 0;

    // check existing log files and decide start_index
    std::vector<std::string> sub_list;
    if (!dsn::utils::filesystem::get_subfiles(_log_dir, sub_list, false)) {
        dassert(false, "Fail to get subfiles in %s.", _log_dir.c_str());
    }

    for (auto &fpath : sub_list) {
        auto &&name = dsn::utils::filesystem::get_file_name(fpath);
        if (name.length() <= 5 || name.substr(0, 4) != "log.")
            continue;

        int index;
        if (1 != sscanf(name.c_str(), "log.%d.txt", &index) || index < 1)
            continue;

        if (index > _index)
            _index = index;

        if (_start_index == 0 || index < _start_index)
            _start_index = index;
    }
    sub_list.clear();

    if (_start_index == 0)
        _start_index = _index;
    else
        _index++;

    _current_log = nullptr;
    create_log_file();
    _log_thread = std::thread(&hpc_logger::log_thread, this);
}

void hpc_logger::create_log_file()
{
    std::stringstream log;
    log << _log_dir << "/log." << _index++ << ".txt";
    _current_log = new std::ofstream(
        log.str().c_str(), std::ofstream::out | std::ofstream::app | std::ofstream::binary);
    _current_log_file_bytes = 0;

    // TODO: move gc out of criticial path
    while (_index - _start_index > _max_number_of_log_files_on_disk) {
        std::stringstream str2;
        str2 << "log." << _start_index++ << ".txt";
        auto dp = utils::filesystem::path_combine(_log_dir, str2.str());
        if (::remove(dp.c_str()) != 0) {
            printf("Failed to remove garbage log file %s\n", dp.c_str());
            _start_index--;
            break;
        }
    }
}

hpc_logger::~hpc_logger(void)
{
    flush();

    if (!_stop_thread) {
        _stop_thread = true;
        _write_list_cond.notify_one();
        _log_thread.join();
    }

    _current_log->close();
    delete _current_log;
}

void hpc_logger::flush()
{
    std::vector<int> threads;
    hpc_log_manager::instance().get_all_keys(threads);

    for (auto &tid : threads) {
        __hpc_log_info__ *log;
        if (!hpc_log_manager::instance().get(tid, log))
            continue;

        _write_list_lock.lock();
        if (log->next_write_ptr != log->buffer) {
            buffer_push(log->buffer, static_cast<int>(log->next_write_ptr - log->buffer));
            log->buffer = (char *)malloc(_per_thread_buffer_bytes);
            log->next_write_ptr = log->buffer;
        }
        _write_list_lock.unlock();
    }

    _write_list_cond.notify_one();

    bool wait = true;
    while (wait) {
        _write_list_lock.lock();
        if (_write_list.size() == 0 && !_is_writing)
            wait = false;
        _write_list_lock.unlock();
        if (wait)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void hpc_logger::dsn_logv(const char *file,
                          const char *function,
                          const int line,
                          dsn_log_level_t log_level,
                          const char *title,
                          const char *fmt,
                          va_list args)
{
    if (s_hpc_log_tls_info.magic != 0xdeadbeef) {
        s_hpc_log_tls_info.buffer = (char *)malloc(_per_thread_buffer_bytes);
        s_hpc_log_tls_info.next_write_ptr = s_hpc_log_tls_info.buffer;

        hpc_log_manager::instance().put(::dsn::utils::get_current_tid(), &s_hpc_log_tls_info);
        s_hpc_log_tls_info.magic = 0xdeadbeef;
    }

    // get enough write space >= 1K
    if (s_hpc_log_tls_info.next_write_ptr + 1024 >
        s_hpc_log_tls_info.buffer + _per_thread_buffer_bytes) {
        _write_list_lock.lock();
        buffer_push(
            s_hpc_log_tls_info.buffer,
            static_cast<int>(s_hpc_log_tls_info.next_write_ptr - s_hpc_log_tls_info.buffer));
        s_hpc_log_tls_info.buffer = (char *)malloc(_per_thread_buffer_bytes);
        s_hpc_log_tls_info.next_write_ptr = s_hpc_log_tls_info.buffer;
        _write_list_lock.unlock();

        _write_list_cond.notify_one();
    }

    char *ptr = s_hpc_log_tls_info.next_write_ptr;
    char *ptr0 = ptr; // remember it
    size_t capacity =
        static_cast<size_t>(s_hpc_log_tls_info.buffer + _per_thread_buffer_bytes - ptr);

    // print verbose log header
    uint64_t ts = 0;
    int tid = ::dsn::utils::get_current_tid();
    if (::dsn::tools::is_engine_ready())
        ts = dsn_now_ns();
    char str[24];
    ::dsn::utils::time_ms_to_string(ts / 1000000, str);
    static const char s_level_char[] = "IDWEF";
    auto wn = snprintf_p(
        ptr, capacity, "%c%s (%" PRIu64 " %04x) ", s_level_char[log_level], str, ts, tid);
    ptr += wn;
    capacity -= wn;

    auto t = task::get_current_task_id();
    if (t) {
        if (nullptr != task::get_current_worker2()) {
            wn = snprintf_p(ptr,
                            capacity,
                            "%6s.%7s%d.%016" PRIx64 ": ",
                            task::get_current_node_name(),
                            task::get_current_worker2()->pool_spec().name.c_str(),
                            task::get_current_worker2()->index(),
                            t);
        } else {
            wn = snprintf_p(ptr,
                            capacity,
                            "%6s.%7s.%05d.%016" PRIx64 ": ",
                            task::get_current_node_name(),
                            "io-thrd",
                            tid,
                            t);
        }
    } else {
        wn = snprintf_p(
            ptr, capacity, "%6s.%7s.%05d: ", task::get_current_node_name(), "io-thrd", tid);
    }

    ptr += wn;
    capacity -= wn;

    // print body
    wn = std::vsnprintf(ptr, capacity - 1, fmt, args);
    if (wn < 0) {
        wn = snprintf_p(ptr, capacity - 1, "-- cannot printf due to that log entry has error ---");
    } else if (static_cast<unsigned>(wn) >= capacity) {
        // log truncated
        wn = capacity - 1;
    }

    *(ptr + wn) = '\n';
    ptr += (wn + 1);
    capacity -= (wn + 1);

    // set next write ptr
    s_hpc_log_tls_info.next_write_ptr = ptr;

    // dump critical logs on screen
    if (log_level >= LOG_LEVEL_WARNING) {
        std::cout.write(ptr0, ptr - ptr0);
    }
}

void hpc_logger::buffer_push(char *buffer, int size) { _write_list.emplace_back(buffer, size); }

void hpc_logger::write_buffer_list(std::vector<buffer_info> &llist)
{
    for (auto &new_buffer_info : llist) {
        if (_current_log_file_bytes + new_buffer_info.buffer_size >= MAX_FILE_SIZE) {
            _current_log->close();
            delete _current_log;
            _current_log = nullptr;

            create_log_file();
        }

        if (new_buffer_info.buffer_size > 0) {
            _current_log->write(new_buffer_info.buffer, new_buffer_info.buffer_size);
            _current_log_file_bytes += new_buffer_info.buffer_size;
        }
    }
    _current_log->flush();
    llist.clear();
}
}
}
