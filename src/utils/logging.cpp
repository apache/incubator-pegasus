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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <fmt/format.h>
// IWYU pragma: no_include <spdlog/details/file_helper-inl.h>
// IWYU pragma: no_include <spdlog/details/periodic_worker-inl.h>
#include <spdlog/formatter.h>
#include <spdlog/logger.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/stdout_sinks.h> // IWYU pragma: keep
// IWYU pragma: no_include <spdlog/pattern_formatter-inl.h>
// IWYU pragma: no_include <spdlog/sinks/stdout_sinks-inl.h>
// IWYU pragma: no_include <spdlog/spdlog-inl.h>
#include <cstdint>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "spdlog/common.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "task/task.h"
#include "task/task_worker.h"
#include "utils/command_manager.h"
#include "utils/enum_helper.h"
#include "utils/flags.h"
#include "utils/join_point.h"
#include "utils/logging.h"
#include "utils/process_utils.h"
#include "utils/sys_exit_hook.h"
#include "utils/threadpool_spec.h"

enum log_level_t
{
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_COUNT,
    LOG_LEVEL_INVALID
};

ENUM_BEGIN(log_level_t, LOG_LEVEL_INVALID)
ENUM_REG(LOG_LEVEL_DEBUG)
ENUM_REG(LOG_LEVEL_INFO)
ENUM_REG(LOG_LEVEL_WARNING)
ENUM_REG(LOG_LEVEL_ERROR)
ENUM_REG(LOG_LEVEL_FATAL)
ENUM_END(log_level_t)

DSN_DEFINE_string(core,
                  logging_start_level,
                  "LOG_LEVEL_INFO",
                  "Logs with level larger than or equal to this level be logged");
DSN_DEFINE_validator(logging_start_level, [](const char *value) -> bool {
    const auto level = enum_from_string(value, LOG_LEVEL_INVALID);
    return LOG_LEVEL_DEBUG <= level && level <= LOG_LEVEL_FATAL;
});

DSN_DEFINE_bool(core,
                logging_flush_on_exit,
                true,
                "Whether to flush the logs when the process exits");

DSN_DEFINE_bool(tools.simple_logger,
                short_header,
                false,
                "Whether to use short header (excluding "
                "file, file number and function name "
                "fields in each line)");

DSN_DEFINE_bool(tools.simple_logger, fast_flush, false, "Whether to flush logs every second");

DSN_DEFINE_uint64(tools.simple_logger,
                  max_log_file_bytes,
                  64UL * 1024 * 1024,
                  "The maximum bytes of a log file. A new log file will be created if the current "
                  "log file exceeds this size.");
DSN_DEFINE_validator(max_log_file_bytes, [](int32_t value) -> bool { return value > 0; });

DSN_DEFINE_uint64(
    tools.simple_logger,
    max_number_of_log_files_on_disk,
    20,
    "The maximum number of log files to be reserved on disk, older logs are deleted automatically");
DSN_DEFINE_validator(max_number_of_log_files_on_disk,
                     [](int32_t value) -> bool { return value > 0; });

DSN_DEFINE_string(tools.simple_logger, base_name, "pegasus", "The default base name for log file");

DSN_DEFINE_string(
    tools.simple_logger,
    stderr_start_level,
    "LOG_LEVEL_WARNING",
    "The lowest level of log messages to be copied to stderr in addition to log files");
DSN_DEFINE_validator(stderr_start_level, [](const char *value) -> bool {
    const auto level = enum_from_string(value, LOG_LEVEL_INVALID);
    return LOG_LEVEL_DEBUG <= level && level <= LOG_LEVEL_FATAL;
});

std::shared_ptr<spdlog::logger> g_stderr_logger = spdlog::stderr_logger_mt("stderr");
std::shared_ptr<spdlog::logger> g_file_logger;

static std::map<log_level_t, spdlog::level::level_enum> to_spdlog_levels = {
    {LOG_LEVEL_DEBUG, spdlog::level::debug},
    {LOG_LEVEL_INFO, spdlog::level::info},
    {LOG_LEVEL_WARNING, spdlog::level::warn},
    {LOG_LEVEL_ERROR, spdlog::level::err},
    {LOG_LEVEL_FATAL, spdlog::level::critical}};

static void log_on_sys_exit(::dsn::sys_exit_type)
{
    g_stderr_logger->flush();
    g_file_logger->flush();
}

void dsn_log_init(const std::string &log_dir, const std::string &role_name)
{
    g_file_logger = spdlog::rotating_logger_mt(
        "file",
        fmt::format("{}/{}.log", log_dir, role_name.empty() ? FLAGS_base_name : role_name),
        FLAGS_max_log_file_bytes,
        FLAGS_max_number_of_log_files_on_disk,
        true /*rotate_on_open*/);
    //    _symlink_path = utils::filesystem::path_combine(_log_dir, symlink_name);
    const auto file_start_level =
        to_spdlog_levels[enum_from_string(FLAGS_logging_start_level, LOG_LEVEL_INVALID)];
    g_file_logger->set_level(file_start_level);
    g_file_logger->flush_on(spdlog::level::err);

    const auto stderr_start_level =
        to_spdlog_levels[enum_from_string(FLAGS_stderr_start_level, LOG_LEVEL_INVALID)];
    g_stderr_logger->set_level(stderr_start_level);
    g_stderr_logger->flush_on(spdlog::level::err);

    if (FLAGS_fast_flush) {
        spdlog::flush_every(std::chrono::seconds(1));
    }

    // register log flush on exit
    if (FLAGS_logging_flush_on_exit) {
        ::dsn::tools::sys_exit.put_back(log_on_sys_exit, "log.flush");
    }

    // See: https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags
    auto formatter = std::make_unique<spdlog::pattern_formatter>();
    if (FLAGS_short_header) {
        static const std::string kPattern = "%L%Y-%m-%d %H:%M:%S.%e %t %* %v";
        formatter->add_flag<pegasus_formatter_flag>('*').set_pattern(kPattern);
    } else {
        static const std::string kPattern = "%L%Y-%m-%d %H:%M:%S.%e %t %* %s:%#:%!(): %v";
        formatter->add_flag<pegasus_formatter_flag>('*').set_pattern(kPattern);
    }
    spdlog::set_formatter(std::move(formatter));

    ::dsn::command_manager::instance().add_global_cmd(
        ::dsn::command_manager::instance().register_single_command(
            "flush-log",
            "Flush log to stderr or file",
            "",
            [](const std::vector<std::string> & /*args*/) {
                g_stderr_logger->flush();
                g_file_logger->flush();
                return "Flush done.";
            }));

    ::dsn::command_manager::instance().add_global_cmd(
        ::dsn::command_manager::instance().register_single_command(
            "reset-log-start-level",
            "Reset the log start level",
            "[DEBUG | INFO | WARNING | ERROR | FATAL]",
            [](const std::vector<std::string> &args) {
                log_level_t start_level = LOG_LEVEL_INVALID;
                if (args.empty()) {
                    start_level = enum_from_string(FLAGS_logging_start_level, LOG_LEVEL_INVALID);
                } else {
                    std::string level_str = "LOG_LEVEL_" + args[0];
                    start_level = enum_from_string(level_str.c_str(), LOG_LEVEL_INVALID);
                    if (start_level == LOG_LEVEL_INVALID) {
                        return "ERROR: invalid level '" + args[0] + "'";
                    }
                }
                g_file_logger->set_level(to_spdlog_levels[start_level]);
                return std::string("OK, current level is ") + enum_to_string(start_level);
            }));
}

std::string log_prefixed_message_func()
{
    const static thread_local int tid = dsn::utils::get_current_tid();
    const auto t = dsn::task::get_current_task_id();
    if (t != 0) {
        if (nullptr != dsn::task::get_current_worker2()) {
            return fmt::format("{}.{}{}.{:016}",
                               dsn::task::get_current_node_name(),
                               dsn::task::get_current_worker2()->pool_spec().name,
                               dsn::task::get_current_worker2()->index(),
                               t);
        }
        return fmt::format("{}.io-thrd.{}.{:016}", dsn::task::get_current_node_name(), tid, t);
    }

    if (nullptr != dsn::task::get_current_worker2()) {
        const static thread_local std::string prefix =
            fmt::format("{}.{}{}",
                        dsn::task::get_current_node_name(),
                        dsn::task::get_current_worker2()->pool_spec().name,
                        dsn::task::get_current_worker2()->index());
        return prefix;
    }
    const static thread_local std::string prefix =
        fmt::format("{}.io-thrd.{}", dsn::task::get_current_node_name(), tid);
    return prefix;
}

void pegasus_formatter_flag::format(const spdlog::details::log_msg &,
                                    const std::tm &,
                                    spdlog::memory_buf_t &dest)
{
    const auto prefix = log_prefixed_message_func();
    dest.append(prefix.data(), prefix.data() + prefix.size());
}

std::unique_ptr<spdlog::custom_flag_formatter> pegasus_formatter_flag::clone() const
{
    return spdlog::details::make_unique<pegasus_formatter_flag>();
}
