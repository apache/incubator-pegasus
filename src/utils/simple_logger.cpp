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

#include "utils/simple_logger.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <fmt/printf.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <vector>

#include "absl/strings/string_view.h"
#include "runtime/api_layer1.h"
#include "utils/command_manager.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/string_conv.h"
#include "utils/time_utils.h"

DSN_DEFINE_bool(tools.simple_logger, fast_flush, false, "Whether to flush logs immediately");
DSN_DEFINE_bool(tools.simple_logger,
                short_header,
                false,
                "Whether to use short header (excluding "
                "file, file number and function name "
                "fields in each line)");

DSN_DEFINE_uint64(
    tools.simple_logger,
    max_number_of_log_files_on_disk,
    20,
    "The maximum number of log files to be reserved on disk, older logs are deleted automatically");
DSN_DEFINE_validator(max_number_of_log_files_on_disk,
                     [](int32_t value) -> bool { return value > 0; });

DSN_DEFINE_string(
    tools.simple_logger,
    stderr_start_level,
    "LOG_LEVEL_WARNING",
    "The lowest level of log messages to be copied to stderr in addition to log files");
DSN_DEFINE_validator(stderr_start_level, [](const char *value) -> bool {
    const auto level = enum_from_string(value, LOG_LEVEL_INVALID);
    return LOG_LEVEL_DEBUG <= level && level <= LOG_LEVEL_FATAL;
});

DSN_DECLARE_string(logging_start_level);

namespace dsn {
namespace tools {
namespace {
int print_header(FILE *fp, log_level_t stderr_start_level, log_level_t log_level)
{
    // The leading character of each log line, corresponding to the log level
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
    const auto header = fmt::format(
        "{}{} ({} {}) {}", s_level_char[log_level], time_str, ts, tid, log_prefixed_message_func());
    const int written_size = fmt::fprintf(fp, "%s", header.c_str());
    if (log_level >= stderr_start_level) {
        fmt::fprintf(stderr, "%s", header.c_str());
    }
    return written_size;
}

int print_long_header(FILE *fp,
                      const char *file,
                      const char *function,
                      const int line,
                      bool short_header,
                      log_level_t stderr_start_level,
                      log_level_t log_level)
{
    if (short_header) {
        return 0;
    }

    const auto long_header = fmt::format("{}:{}:{}(): ", file, line, function);
    const int written_size = fmt::fprintf(fp, "%s", long_header.c_str());
    if (log_level >= stderr_start_level) {
        fmt::fprintf(stderr, "%s", long_header.c_str());
    }
    return written_size;
}

int print_body(FILE *fp, const char *body, log_level_t stderr_start_level, log_level_t log_level)
{
    const int written_size = fmt::fprintf(fp, "%s\n", body);
    if (log_level >= stderr_start_level) {
        fmt::fprintf(stderr, "%s\n", body);
    }
    return written_size;
}

inline void process_fatal_log(log_level_t log_level)
{
    if (dsn_likely(log_level < LOG_LEVEL_FATAL)) {
        return;
    }

    bool coredump = true;
    FAIL_POINT_INJECT_NOT_RETURN_F("coredump_for_fatal_log", [&coredump](absl::string_view str) {
        CHECK(buf2bool(str, coredump),
              "invalid coredump toggle for fatal log, should be true or false: {}",
              str);
    });

    if (dsn_likely(coredump)) {
        dsn_coredump();
    }
}

} // anonymous namespace

screen_logger::screen_logger(bool short_header) : _short_header(short_header) {}

void screen_logger::print_header(log_level_t log_level)
{
    ::dsn::tools::print_header(stdout, LOG_LEVEL_COUNT, log_level);
}

void screen_logger::print_long_header(const char *file,
                                      const char *function,
                                      const int line,
                                      log_level_t log_level)
{
    ::dsn::tools::print_long_header(
        stdout, file, function, line, _short_header, LOG_LEVEL_COUNT, log_level);
}

void screen_logger::print_body(const char *body, log_level_t log_level)
{
    ::dsn::tools::print_body(stdout, body, LOG_LEVEL_COUNT, log_level);
}

void screen_logger::log(
    const char *file, const char *function, const int line, log_level_t log_level, const char *str)
{
    utils::auto_lock<::dsn::utils::ex_lock_nr> l(_lock);

    print_header(log_level);
    print_long_header(file, function, line, log_level);
    print_body(str, log_level);
    if (log_level >= LOG_LEVEL_ERROR) {
        ::fflush(stdout);
    }

    process_fatal_log(log_level);
}

void screen_logger::flush() { ::fflush(stdout); }

simple_logger::simple_logger(const char *log_dir)
    : _log_dir(std::string(log_dir)),
      _log(nullptr),
      // we assume all valid entries are positive
      _start_index(0),
      _index(1),
      _lines(0),
      _stderr_start_level(enum_from_string(FLAGS_stderr_start_level, LOG_LEVEL_INVALID))
{
    // check existing log files
    std::vector<std::string> sub_list;
    CHECK(dsn::utils::filesystem::get_subfiles(_log_dir, sub_list, false),
          "Fail to get subfiles in {}",
          _log_dir);
    for (auto &fpath : sub_list) {
        auto &&name = dsn::utils::filesystem::get_file_name(fpath);
        if (name.length() <= 8 || name.substr(0, 4) != "log.") {
            continue;
        }

        int index;
        if (1 != sscanf(name.c_str(), "log.%d.txt", &index) || index <= 0) {
            continue;
        }

        if (index > _index) {
            _index = index;
        }

        if (_start_index == 0 || index < _start_index) {
            _start_index = index;
        }
    }
    sub_list.clear();

    if (_start_index == 0) {
        _start_index = _index;
    } else {
        ++_index;
    }

    create_log_file();

    // TODO(yingchun): simple_logger is destroyed after command_manager, so will cause crash like
    //  "assertion expression: [_handlers.empty()] All commands must be deregistered before
    //  command_manager is destroyed, however 'flush-log' is still registered".
    //  We need to fix it.
    _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
        "flush-log",
        "Flush log to stderr or file",
        "",
        [this](const std::vector<std::string> &args) {
            this->flush();
            return "Flush done.";
        }));

    _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
        "reset-log-start-level",
        "Reset the log start level",
        "[DEBUG | INFO | WARNING | ERROR | FATAL]",
        [](const std::vector<std::string> &args) {
            log_level_t start_level;
            if (args.size() == 0) {
                start_level = enum_from_string(FLAGS_logging_start_level, LOG_LEVEL_INVALID);
            } else {
                std::string level_str = "LOG_LEVEL_" + args[0];
                start_level = enum_from_string(level_str.c_str(), LOG_LEVEL_INVALID);
                if (start_level == LOG_LEVEL_INVALID) {
                    return "ERROR: invalid level '" + args[0] + "'";
                }
            }
            set_log_start_level(start_level);
            return std::string("OK, current level is ") + enum_to_string(start_level);
        }));
}

void simple_logger::create_log_file()
{
    // Close the current log file if it is opened.
    if (_log != nullptr) {
        ::fclose(_log);
        _log = nullptr;
    }

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

simple_logger::~simple_logger()
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fclose(_log);
    _log = nullptr;
}

void simple_logger::flush()
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fflush(_log);
    ::fflush(stderr);
    ::fflush(stdout);
}

void simple_logger::print_header(log_level_t log_level)
{
    ::dsn::tools::print_header(_log, _stderr_start_level, log_level);
}

void simple_logger::print_long_header(const char *file,
                                      const char *function,
                                      const int line,
                                      log_level_t log_level)
{
    ::dsn::tools::print_long_header(
        _log, file, function, line, FLAGS_short_header, _stderr_start_level, log_level);
}

void simple_logger::print_body(const char *body, log_level_t log_level)
{
    ::dsn::tools::print_body(_log, body, _stderr_start_level, log_level);
}

void simple_logger::log(
    const char *file, const char *function, const int line, log_level_t log_level, const char *str)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    CHECK_NOTNULL(_log, "Log file hasn't been initialized yet");
    print_header(log_level);
    print_long_header(file, function, line, log_level);
    print_body(str, log_level);
    if (FLAGS_fast_flush || log_level >= LOG_LEVEL_ERROR) {
        ::fflush(_log);
    }

    process_fatal_log(log_level);

    if (++_lines >= 200000) {
        create_log_file();
    }
}

} // namespace tools
} // namespace dsn
