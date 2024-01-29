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

#include <string>

#include "utils/api_utilities.h"
#include "utils/chrono_literals.h"
#include "utils/fmt_logging.h"

/// These macros are inspired by Apache Kudu
/// https://github.com/apache/kudu/blob/1.17.0/src/kudu/util/stopwatch.h.

// Macro for logging timing of a block. Usage:
//   LOG_TIMING_PREFIX_IF(INFO, FLAGS_should_record_time, "Tablet X: ", "doing some task") {
//     ... some task which takes some time
//   }
// If FLAGS_should_record_time is true, yields a log like:
// I1102 14:35:51.726186 23082 file.cc:167] Tablet X: Time spent doing some task:
//   real 3.729s user 3.570s sys 0.150s
// The task will always execute regardless of whether the timing information is
// printed.
#define LOG_TIMING_PREFIX_IF(severity, condition, prefix, ...)                                     \
    for (dsn::timer_internal::LogTiming _l(__FILENAME__,                                           \
                                           __FUNCTION__,                                           \
                                           __LINE__,                                               \
                                           LOG_LEVEL_##severity,                                   \
                                           prefix,                                                 \
                                           fmt::format(__VA_ARGS__),                               \
                                           -1,                                                     \
                                           (condition));                                           \
         !_l.HasRun();                                                                             \
         _l.MarkHasRun())

// Conditionally log, no prefix.
#define LOG_TIMING_IF(severity, condition, ...)                                                    \
    LOG_TIMING_PREFIX_IF(severity, (condition), "", (fmt::format(__VA_ARGS__)))

// Always log, including prefix.
#define LOG_TIMING_PREFIX(severity, prefix, ...)                                                   \
    LOG_TIMING_PREFIX_IF(severity, true, (prefix), (fmt::format(__VA_ARGS__)))

// Always log, no prefix.
#define LOG_TIMING(severity, ...) LOG_TIMING_IF(severity, true, (fmt::format(__VA_ARGS__)))

// Macro to log the time spent in the rest of the block.
#define SCOPED_LOG_TIMING(severity, ...)                                                           \
    dsn::timer_internal::LogTiming VARNAME_LINENUM(_log_timing)(__FILENAME__,                      \
                                                                __FUNCTION__,                      \
                                                                __LINE__,                          \
                                                                LOG_LEVEL_##severity,              \
                                                                "",                                \
                                                                fmt::format(__VA_ARGS__),          \
                                                                -1,                                \
                                                                true);

// Scoped version of LOG_SLOW_EXECUTION().
#define SCOPED_LOG_SLOW_EXECUTION(severity, max_expected_millis, ...)                              \
    dsn::timer_internal::LogTiming VARNAME_LINENUM(_log_timing)(__FILENAME__,                      \
                                                                __FUNCTION__,                      \
                                                                __LINE__,                          \
                                                                LOG_LEVEL_##severity,              \
                                                                "",                                \
                                                                fmt::format(__VA_ARGS__),          \
                                                                max_expected_millis,               \
                                                                true)

// Scoped version of LOG_SLOW_EXECUTION() but with a prefix.
#define SCOPED_LOG_SLOW_EXECUTION_PREFIX(severity, max_expected_millis, prefix, ...)               \
    dsn::timer_internal::LogTiming VARNAME_LINENUM(_log_timing)(__FILENAME__,                      \
                                                                __FUNCTION__,                      \
                                                                __LINE__,                          \
                                                                LOG_LEVEL_##severity,              \
                                                                prefix,                            \
                                                                fmt::format(__VA_ARGS__),          \
                                                                max_expected_millis,               \
                                                                true)

// Macro for logging timing of a block. Usage:
//   LOG_SLOW_EXECUTION(INFO, 5, "doing some task") {
//     ... some task which takes some time
//   }
// when slower than 5 milliseconds, yields a log like:
// I1102 14:35:51.726186 23082 file.cc:167] Time spent doing some task:
//   real 3.729s user 3.570s sys 0.150s
#define LOG_SLOW_EXECUTION(severity, max_expected_millis, ...)                                     \
    for (dsn::timer_internal::LogTiming _l(__FILENAME__,                                           \
                                           __FUNCTION__,                                           \
                                           __LINE__,                                               \
                                           LOG_LEVEL_##severity,                                   \
                                           "",                                                     \
                                           fmt::format(__VA_ARGS__),                               \
                                           max_expected_millis,                                    \
                                           true);                                                  \
         !_l.HasRun();                                                                             \
         _l.MarkHasRun())

namespace dsn {

class timer
{
public:
    timer() = default;

    // Start this timer
    void start()
    {
        _start = std::chrono::system_clock::now();
        _stop = _start;
    }

    // Stop this timer
    void stop() { _stop = std::chrono::system_clock::now(); }

    // Get the elapse from start() to stop(), in various units.
    std::chrono::nanoseconds n_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(_stop - _start);
    }
    std::chrono::microseconds u_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(_stop - _start);
    }
    std::chrono::milliseconds m_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(_stop - _start);
    }
    std::chrono::seconds s_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(_stop - _start);
    }

private:
    std::chrono::time_point<std::chrono::system_clock> _stop;
    std::chrono::time_point<std::chrono::system_clock> _start;
};

namespace timer_internal {
// Internal class used by the LOG_TIMING* macros.
class LogTiming
{
public:
    LogTiming(const char *file,
              const char *function,
              int line,
              log_level_t severity,
              std::string prefix,
              std::string description,
              int64_t max_expected_millis,
              bool should_print)
        : file_(file),
          function_(function),
          line_(line),
          severity_(severity),
          prefix_(std::move(prefix)),
          description_(std::move(description)),
          max_expected_millis_(max_expected_millis),
          should_print_(should_print),
          has_run_(false)
    {
        stopwatch_.start();
    }

    ~LogTiming()
    {
        if (should_print_) {
            Print(max_expected_millis_);
        }
    }

    // Allows this object to be used as the loop variable in for-loop macros.
    // Call HasRun() in the conditional check in the for-loop.
    bool HasRun() { return has_run_; }

    // Allows this object to be used as the loop variable in for-loop macros.
    // Call MarkHasRun() in the "increment" section of the for-loop.
    void MarkHasRun() { has_run_ = true; }

private:
    timer stopwatch_;
    const char *file_;
    const char *function_;
    const int line_;
    const log_level_t severity_;
    const std::string prefix_;
    const std::string description_;
    const int64_t max_expected_millis_;
    const bool should_print_;
    bool has_run_;

    // Print if the number of expected millis exceeds the max.
    // Passing a negative number implies "always print".
    void Print(int64_t max_expected_millis)
    {
        stopwatch_.stop();
        auto ms = stopwatch_.m_elapsed();
        if (max_expected_millis < 0 || ms.count() > max_expected_millis) {
            global_log(file_,
                       function_,
                       line_,
                       severity_,
                       fmt::format("{}ime spent {}: {}ms",
                                   prefix_.empty() ? "T" : fmt::format("{} t", prefix_),
                                   description_,
                                   ms.count())
                           .c_str());
        }
    }
};

} // namespace timer_internal
} // namespace dsn
