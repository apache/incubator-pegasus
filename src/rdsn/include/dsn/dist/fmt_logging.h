/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
#define dcheck_ne(var1, var2) dassert_f(var1 != var2, "{} vs {}", var1, var2)
#define dcheck_ge(var1, var2) dassert_f(var1 >= var2, "{} vs {}", var1, var2)
#define dcheck_le(var1, var2) dassert_f(var1 <= var2, "{} vs {}", var1, var2)
#define dcheck_gt(var1, var2) dassert_f(var1 > var2, "{} vs {}", var1, var2)
#define dcheck_lt(var1, var2) dassert_f(var1 < var2, "{} vs {}", var1, var2)

#define dcheck_eq_replica(var1, var2) dassert_replica(var1 == var2, "{} vs {}", var1, var2)
#define dcheck_ne_replica(var1, var2) dassert_replica(var1 != var2, "{} vs {}", var1, var2)
#define dcheck_ge_replica(var1, var2) dassert_replica(var1 >= var2, "{} vs {}", var1, var2)
#define dcheck_le_replica(var1, var2) dassert_replica(var1 <= var2, "{} vs {}", var1, var2)
#define dcheck_gt_replica(var1, var2) dassert_replica(var1 > var2, "{} vs {}", var1, var2)
#define dcheck_lt_replica(var1, var2) dassert_replica(var1 < var2, "{} vs {}", var1, var2)

// Return the given status if condition is not true.
#define ERR_LOG_AND_RETURN_NOT_TRUE(s, err, ...)                                                   \
    do {                                                                                           \
        if (dsn_unlikely(!(s))) {                                                                  \
            derror_f("{}: {}", err, fmt::format(__VA_ARGS__));                                     \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

// Return the given status if it is not ERR_OK.
#define ERR_LOG_AND_RETURN_NOT_OK(s, ...)                                                          \
    do {                                                                                           \
        error_code _err = (s);                                                                     \
        ERR_LOG_AND_RETURN_NOT_TRUE(_err == ERR_OK, _err, __VA_ARGS__);                            \
    } while (0)
