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

#pragma once

#include <fmt/ostream.h>

// The macros below no longer use the default snprintf method for log message formatting,
// instead we use fmt::format.
// TODO(wutao1): prevent construction of std::string for each log.

#define dlog_f(level, ...)                                                                         \
    do {                                                                                           \
        if (level >= dsn_log_start_level)                                                          \
            dsn_log(                                                                               \
                __FILENAME__, __FUNCTION__, __LINE__, level, fmt::format(__VA_ARGS__).c_str());    \
    } while (false)
#define dinfo_f(...) dlog_f(LOG_LEVEL_INFORMATION, __VA_ARGS__)
#define ddebug_f(...) dlog_f(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define dwarn_f(...) dlog_f(LOG_LEVEL_WARNING, __VA_ARGS__)
#define derror_f(...) dlog_f(LOG_LEVEL_ERROR, __VA_ARGS__)
#define dfatal_f(...) dlog_f(LOG_LEVEL_FATAL, __VA_ARGS__)
#define dassert_f(x, ...)                                                                          \
    do {                                                                                           \
        if (dsn_unlikely(!(x))) {                                                                  \
            dlog_f(LOG_LEVEL_FATAL, "assertion expression: " #x);                                  \
            dlog_f(LOG_LEVEL_FATAL, __VA_ARGS__);                                                  \
            dsn_coredump();                                                                        \
        }                                                                                          \
    } while (false)

// Macros for writing log message prefixed by gpid and address.
#define dinfo_replica(...) dinfo_f("[{}] {}", replica_name(), fmt::format(__VA_ARGS__))
#define ddebug_replica(...) ddebug_f("[{}] {}", replica_name(), fmt::format(__VA_ARGS__))
#define dwarn_replica(...) dwarn_f("[{}] {}", replica_name(), fmt::format(__VA_ARGS__))
#define derror_replica(...) derror_f("[{}] {}", replica_name(), fmt::format(__VA_ARGS__))
#define dfatal_replica(...) dfatal_f("[{}] {}", replica_name(), fmt::format(__VA_ARGS__))
#define dassert_replica(x, ...) dassert_f(x, "[{}] {}", replica_name(), fmt::format(__VA_ARGS__))

// Macros to check expected condition. It will abort the application
// and log a fatal message when the condition is not met.
#define dcheck_eq(var1, var2) dassert_f(var1 == var2, "{} vs {}", var1, var2)
#define dcheck_ge(var1, var2) dassert_f(var1 >= var2, "{} vs {}", var1, var2)
#define dcheck_le(var1, var2) dassert_f(var1 <= var2, "{} vs {}", var1, var2)
#define dcheck_gt(var1, var2) dassert_f(var1 > var2, "{} vs {}", var1, var2)
#define dcheck_lt(var1, var2) dassert_f(var1 < var2, "{} vs {}", var1, var2)

#define dcheck_eq_replica(var1, var2) dassert_replica(var1 == var2, "{} vs {}", var1, var2)
#define dcheck_ge_replica(var1, var2) dassert_replica(var1 >= var2, "{} vs {}", var1, var2)
#define dcheck_le_replica(var1, var2) dassert_replica(var1 <= var2, "{} vs {}", var1, var2)
#define dcheck_gt_replica(var1, var2) dassert_replica(var1 > var2, "{} vs {}", var1, var2)
#define dcheck_lt_replica(var1, var2) dassert_replica(var1 < var2, "{} vs {}", var1, var2)
