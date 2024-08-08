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

#include <errno.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <fmt/printf.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <ctime>
#include <functional>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

#include <string_view>
#include "runtime/api_layer1.h"
#include "utils/command_manager.h"
#include "utils/errors.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/safe_strerror_posix.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/time_utils.h"

DSN_DEFINE_uint64(tools.simple_logger,
                  max_log_file_bytes,
                  64 * 1024 * 1024,
                  "The maximum bytes of a log file. A new log file will be created if the current "
                  "log file exceeds this size.");
DSN_DEFINE_validator(max_log_file_bytes, [](int32_t value) -> bool { return value > 0; });

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

DSN_DEFINE_string(tools.screen_logger,
                  stderr_start_level_on_stdout,
                  "LOG_LEVEL_WARNING",
                  "The lowest level of log messages to be copied to stderr in addition to stdout");
DSN_DEFINE_validator(stderr_start_level_on_stdout, [](const char *value) -> bool {
    const auto level = enum_from_string(value, LOG_LEVEL_INVALID);
    return LOG_LEVEL_DEBUG <= level && level <= LOG_LEVEL_FATAL;
});

DSN_DEFINE_string(
    tools.simple_logger,
    stderr_start_level,
    "LOG_LEVEL_WARNING",
    "The lowest level of log messages to be copied to stderr in addition to log files");
DSN_DEFINE_validator(stderr_start_level, [](const char *value) -> bool {
    const auto level = enum_from_string(value, LOG_LEVEL_INVALID);
    return LOG_LEVEL_DEBUG <= level && level <= LOG_LEVEL_FATAL;
});

DSN_DEFINE_string(tools.simple_logger, base_name, "pegasus", "The default base name for log file");

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
    FAIL_POINT_INJECT_NOT_RETURN_F("coredump_for_fatal_log", [&coredump](std::string_view str) {
        CHECK(buf2bool(str, coredump),
              "invalid coredump toggle for fatal log, should be true or false: {}",
              str);
    });

    if (dsn_likely(coredump)) {
        dsn_coredump();
    }
}

} // anonymous namespace

screen_logger::screen_logger(const char *, const char *)
    : logging_provider(enum_from_string(FLAGS_stderr_start_level_on_stdout, LOG_LEVEL_INVALID)),
      _short_header(true)
{
}

void screen_logger::print_header(log_level_t log_level)
{
    ::dsn::tools::print_header(stdout, _stderr_start_level, log_level);
}

void screen_logger::print_long_header(const char *file,
                                      const char *function,
                                      const int line,
                                      log_level_t log_level)
{
    ::dsn::tools::print_long_header(
        stdout, file, function, line, _short_header, _stderr_start_level, log_level);
}

void screen_logger::print_body(const char *body, log_level_t log_level)
{
    ::dsn::tools::print_body(stdout, body, _stderr_start_level, log_level);
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

simple_logger::simple_logger(const char *log_dir, const char *role_name)
    : logging_provider(enum_from_string(FLAGS_stderr_start_level, LOG_LEVEL_INVALID)),
      _log_dir(std::string(log_dir)),
      _log(nullptr),
      _file_bytes(0)
{
    // Use 'role_name' if it is specified, otherwise use 'base_name'.
    const std::string symlink_name(
        fmt::format("{}.log", utils::is_empty(role_name) ? FLAGS_base_name : role_name));
    _file_name_prefix = fmt::format("{}.", symlink_name);
    _symlink_path = utils::filesystem::path_combine(_log_dir, symlink_name);

    create_log_file();

    static std::once_flag flag;
    std::call_once(flag, [&]() {
        ::dsn::command_manager::instance().add_global_cmd(
            ::dsn::command_manager::instance().register_single_command(
                "flush-log",
                "Flush log to stderr or file",
                "",
                [this](const std::vector<std::string> &args) {
                    this->flush();
                    return "Flush done.";
                }));

        ::dsn::command_manager::instance().add_global_cmd(
            ::dsn::command_manager::instance().register_single_command(
                "reset-log-start-level",
                "Reset the log start level",
                "[DEBUG | INFO | WARNING | ERROR | FATAL]",
                [](const std::vector<std::string> &args) {
                    log_level_t start_level;
                    if (args.size() == 0) {
                        start_level =
                            enum_from_string(FLAGS_logging_start_level, LOG_LEVEL_INVALID);
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
    });
}

void simple_logger::create_log_file()
{
    // Close the current log file if it is opened.
    if (_log != nullptr) {
        ::fclose(_log);
        _log = nullptr;
    }

    // Reset the file size.
    _file_bytes = 0;

    // Open the new log file.
    uint64_t ts = dsn::utils::get_current_physical_time_ns();
    std::string time_str;
    ::dsn::utils::time_ms_to_sequent_string(ts / 1000000, time_str);
    const std::string file_name(fmt::format("{}{}", _file_name_prefix, time_str));
    const std::string path(utils::filesystem::path_combine(_log_dir, file_name));
    _log = ::fopen(path.c_str(), "w+");
    CHECK_NOTNULL(_log, "Failed to fopen {}: {}", path, dsn::utils::safe_strerror(errno));

    // Unlink the latest log file.
    if (::unlink(_symlink_path.c_str()) != 0) {
        if (errno != ENOENT) {
            fmt::print(stderr,
                       "Failed to unlink {}: {}\n",
                       _symlink_path,
                       dsn::utils::safe_strerror(errno));
        }
    }

    // Create a new symlink to the newly created log file.
    if (::symlink(file_name.c_str(), _symlink_path.c_str()) != 0) {
        fmt::print(stderr,
                   "Failed to symlink {} as {}: {}\n",
                   file_name,
                   _symlink_path,
                   dsn::utils::safe_strerror(errno));
    }

    // Remove redundant log files.
    remove_redundant_files();
}

void simple_logger::remove_redundant_files()
{
    // Collect log files.
    const auto file_path_pattern =
        fmt::format("{}*", utils::filesystem::path_combine(_log_dir, _file_name_prefix));
    std::vector<std::string> matching_files;
    const auto es = dsn::utils::filesystem::glob(file_path_pattern, matching_files);
    if (!es) {
        fmt::print(
            stderr, "{}: Failed to glob '{}', error \n", es.description(), file_path_pattern);
        return;
    }

    // Skip if the number of log files is not exceeded.
    auto max_matches = static_cast<size_t>(FLAGS_max_number_of_log_files_on_disk);
    if (matching_files.size() <= max_matches) {
        return;
    }

    // Collect mtimes of log files.
    std::vector<std::pair<time_t, std::string>> matching_file_mtimes;
    for (auto &matching_file_path : matching_files) {
        struct stat s;
        if (::stat(matching_file_path.c_str(), &s) != 0) {
            fmt::print(stderr,
                       "Failed to stat {}: {}\n",
                       matching_file_path,
                       dsn::utils::safe_strerror(errno));
            continue;
        }

#ifdef __APPLE__
        int64_t mtime = s.st_mtimespec.tv_sec * 1000000 + s.st_mtimespec.tv_nsec / 1000;
#else
        int64_t mtime = s.st_mtim.tv_sec * 1000000 + s.st_mtim.tv_nsec / 1000;
#endif
        matching_file_mtimes.emplace_back(mtime, std::move(matching_file_path));
    }

    // Use mtime to determine which matching files to delete. This could
    // potentially be ambiguous, depending on the resolution of last-modified
    // timestamp in the filesystem, but that is part of the contract.
    std::sort(matching_file_mtimes.begin(), matching_file_mtimes.end());
    matching_file_mtimes.resize(matching_file_mtimes.size() - max_matches);

    // Remove redundant log files.
    for (const auto &[_, matching_file] : matching_file_mtimes) {
        if (::remove(matching_file.c_str()) != 0) {
            // If remove failed, just print log and ignore it.
            fmt::print(stderr,
                       "Failed to remove redundant log file {}: {}\n",
                       matching_file,
                       dsn::utils::safe_strerror(errno));
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
    add_bytes_if_valid(::dsn::tools::print_header(_log, _stderr_start_level, log_level));
}

void simple_logger::print_long_header(const char *file,
                                      const char *function,
                                      const int line,
                                      log_level_t log_level)
{
    add_bytes_if_valid(::dsn::tools::print_long_header(
        _log, file, function, line, FLAGS_short_header, _stderr_start_level, log_level));
}

void simple_logger::print_body(const char *body, log_level_t log_level)
{
    add_bytes_if_valid(::dsn::tools::print_body(_log, body, _stderr_start_level, log_level));
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

    if (_file_bytes >= FLAGS_max_log_file_bytes) {
        create_log_file();
    }
}

} // namespace tools
} // namespace dsn
