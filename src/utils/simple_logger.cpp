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

#include "simple_logger.h"
#include <sstream>
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/time_utils.h"
#include <fmt/format.h>

namespace dsn {
namespace tools {

DSN_DEFINE_bool("tools.simple_logger", fast_flush, false, "whether to flush immediately");

DSN_DEFINE_bool("tools.simple_logger",
                short_header,
                true,
                "whether to use short header (excluding file/function etc.)");

DSN_DEFINE_uint64("tools.simple_logger",
                  max_number_of_log_files_on_disk,
                  20,
                  "max number of log files reserved on disk, older logs are auto deleted");

DSN_DEFINE_string("tools.simple_logger",
                  stderr_start_level,
                  "LOG_LEVEL_WARNING",
                  "copy log messages at or above this level to stderr in addition to logfiles");
DSN_DEFINE_validator(stderr_start_level, [](const char *level) -> bool {
    return strcmp(level, "LOG_LEVEL_INVALID") != 0;
});

static void print_header(FILE *fp, dsn_log_level_t log_level)
{
    // The leading character of each log lines, corresponding to the log level
    // D: Debug
    // I: Info
    // W: Warning
    // E: Error
    // F: Fatal
    static char s_level_char[] = "DIWEF";

    uint64_t ts = dsn_now_ns();
    std::string time_str;
    dsn::utils::time_ms_to_string(ts / 1000000, time_str);

    int tid = dsn::utils::get_current_tid();
    fmt::print(fp,
               "{}{} ({} {}) {}",
               s_level_char[log_level],
               time_str,
               ts,
               tid,
               log_prefixed_message_func().c_str());
}

screen_logger::screen_logger(bool short_header) : logging_provider("./")
{
    _short_header = short_header;
}

screen_logger::screen_logger(const char *log_dir) : logging_provider(log_dir)
{
    _short_header =
        dsn_config_get_value_bool("tools.screen_logger",
                                  "short_header",
                                  true,
                                  "whether to use short header (excluding file/function etc.)");
}

screen_logger::~screen_logger(void) {}

void screen_logger::dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             va_list args)
{
    utils::auto_lock<::dsn::utils::ex_lock_nr> l(_lock);

    print_header(stdout, log_level);
    if (!_short_header) {
        printf("%s:%d:%s(): ", file, line, function);
    }
    vprintf(fmt, args);
    printf("\n");
}

void screen_logger::flush() { ::fflush(stdout); }

simple_logger::simple_logger(const char *log_dir) : logging_provider(log_dir)
{
    _log_dir = std::string(log_dir);
    // we assume all valid entries are positive
    _start_index = 0;
    _index = 1;
    _lines = 0;
    _log = nullptr;
    _stderr_start_level = enum_from_string(FLAGS_stderr_start_level, LOG_LEVEL_INVALID);

    // check existing log files
    std::vector<std::string> sub_list;
    CHECK(dsn::utils::filesystem::get_subfiles(_log_dir, sub_list, false),
          "Fail to get subfiles in {}",
          _log_dir);
    for (auto &fpath : sub_list) {
        auto &&name = dsn::utils::filesystem::get_file_name(fpath);
        if (name.length() <= 8 || name.substr(0, 4) != "log.")
            continue;

        int index;
        if (1 != sscanf(name.c_str(), "log.%d.txt", &index) || index <= 0)
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
        ++_index;

    create_log_file();
}

void simple_logger::create_log_file()
{
    if (_log != nullptr)
        ::fclose(_log);

    _lines = 0;

    std::stringstream str;
    str << _log_dir << "/log." << _index++ << ".txt";
    _log = ::fopen(str.str().c_str(), "w+");

    // TODO: move gc out of criticial path
    while (_index - _start_index > FLAGS_max_number_of_log_files_on_disk) {
        std::stringstream str2;
        str2 << "log." << _start_index++ << ".txt";
        auto dp = utils::filesystem::path_combine(_log_dir, str2.str());
        if (utils::filesystem::file_exists(dp)) {
            if (::remove(dp.c_str()) != 0) {
                // if remove failed, just print log and ignore it.
                printf("Failed to remove garbage log file %s\n", dp.c_str());
            }
        }
    }
}

simple_logger::~simple_logger(void)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fclose(_log);
}

void simple_logger::flush()
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fflush(_log);
    ::fflush(stdout);
}

void simple_logger::dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             va_list args)
{
    va_list args2;
    if (log_level >= _stderr_start_level) {
        va_copy(args2, args);
    }

    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    print_header(_log, log_level);
    if (!FLAGS_short_header) {
        fprintf(_log, "%s:%d:%s(): ", file, line, function);
    }
    vfprintf(_log, fmt, args);
    fprintf(_log, "\n");
    if (FLAGS_fast_flush || log_level >= LOG_LEVEL_ERROR) {
        ::fflush(_log);
    }

    if (log_level >= _stderr_start_level) {
        print_header(stdout, log_level);
        if (!FLAGS_short_header) {
            printf("%s:%d:%s(): ", file, line, function);
        }
        vprintf(fmt, args2);
        printf("\n");
    }

    if (++_lines >= 200000) {
        create_log_file();
    }
}

void simple_logger::dsn_log(const char *file,
                            const char *function,
                            const int line,
                            dsn_log_level_t log_level,
                            const char *str)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    print_header(_log, log_level);
    if (!FLAGS_short_header) {
        fprintf(_log, "%s:%d:%s(): ", file, line, function);
    }
    fprintf(_log, "%s\n", str);
    if (FLAGS_fast_flush || log_level >= LOG_LEVEL_ERROR) {
        ::fflush(_log);
    }

    if (log_level >= _stderr_start_level) {
        print_header(stdout, log_level);
        if (!FLAGS_short_header) {
            printf("%s:%d:%s(): ", file, line, function);
        }
        printf("%s\n", str);
    }

    if (++_lines >= 200000) {
        create_log_file();
    }
}

} // namespace tools
} // namespace dsn
