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

#include <dsn/tool_api.h>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace dsn {
namespace tools {
struct buffer_info
{
    char *buffer;
    int buffer_size;
    buffer_info(char *buffer, int buffer_size) : buffer(buffer), buffer_size(buffer_size) {}
};

class hpc_logger : public logging_provider
{
public:
    hpc_logger(const char *log_dir);
    virtual ~hpc_logger(void);

    virtual void dsn_logv(const char *file,
                          const char *function,
                          const int line,
                          dsn_log_level_t log_level,
                          const char *fmt,
                          va_list args);

    virtual void flush();

private:
    void log_thread();

    void buffer_push(char *buffer, int size);
    // print logs in log list
    void write_buffer_list(std::vector<buffer_info> &llist);
    void create_log_file();

private:
    bool _stop_thread;
    std::thread _log_thread;
    std::string _log_dir;

    // global buffer list
    std::condition_variable_any _write_list_cond;
    ::dsn::utils::ex_lock_nr_spin _write_list_lock;
    std::vector<buffer_info> _write_list;
    volatile bool _is_writing;

    // log file and line count
    int _start_index;
    int _index;
    int _per_thread_buffer_bytes;
    int _current_log_file_bytes;
    int _max_number_of_log_files_on_disk;

    // current write file
    std::ofstream *_current_log;
};
}
}
