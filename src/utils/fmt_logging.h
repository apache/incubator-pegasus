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

#define LOG_DEBUG_F(...) dlog_f(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define LOG_INFO_F(...) dlog_f(LOG_LEVEL_INFO, __VA_ARGS__)
#define LOG_WARNING_F(...) dlog_f(LOG_LEVEL_WARNING, __VA_ARGS__)
#define LOG_ERROR_F(...) dlog_f(LOG_LEVEL_ERROR, __VA_ARGS__)
#define LOG_FATAL_F(...) dlog_f(LOG_LEVEL_FATAL, __VA_ARGS__)
#define dassert_f(x, ...)                                                                          \
    do {                                                                                           \
        if (dsn_unlikely(!(x))) {                                                                  \
            dlog_f(LOG_LEVEL_FATAL, "assertion expression: " #x);                                  \
            dlog_f(LOG_LEVEL_FATAL, __VA_ARGS__);                                                  \
            dsn_coredump();                                                                        \
        }                                                                                          \
    } while (false)

#define CHECK dassert_f
#define CHECK_NOTNULL(p, ...) CHECK(p != nullptr, __VA_ARGS__)

// Macros for writing log message prefixed by log_prefix().
#define LOG_DEBUG_PREFIX(...) LOG_DEBUG_F("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_INFO_PREFIX(...) LOG_INFO_F("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_WARNING_PREFIX(...) LOG_WARNING_F("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_ERROR_PREFIX(...) LOG_ERROR_F("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_FATAL_PREFIX(...) LOG_FATAL_F("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define dassert_replica(x, ...) CHECK(x, "[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))

// Macros to check expected condition. It will abort the application
// and log a fatal message when the condition is not met.
#define CHECK_NE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 != _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                      \
    } while (false)

#define CHECK_EQ_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 == _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                      \
    } while (false)

#define CHECK_GE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 >= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                      \
    } while (false)

#define CHECK_LE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 <= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                      \
    } while (false)

#define CHECK_GT_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 > _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                       \
    } while (false)

#define CHECK_LT_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK(_v1 < _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));                       \
    } while (false)

#define CHECK_NE(var1, var2) CHECK_NE_MSG(var1, var2, "")
#define CHECK_EQ(var1, var2) CHECK_EQ_MSG(var1, var2, "")
#define CHECK_GE(var1, var2) CHECK_GE_MSG(var1, var2, "")
#define CHECK_LE(var1, var2) CHECK_LE_MSG(var1, var2, "")
#define CHECK_GT(var1, var2) CHECK_GT_MSG(var1, var2, "")
#define CHECK_LT(var1, var2) CHECK_LT_MSG(var1, var2, "")

// TODO(yingchun): add CHECK_NULL(ptr), CHECK_OK(err), CHECK(cond)
#define CHECK_EQ_PREFIX(var1, var2)                                                                \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        dassert_replica(_v1 == _v2, "{} vs {}", _v1, _v2);                                         \
    } while (false)

#define CHECK_GE_PREFIX(var1, var2)                                                                \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        dassert_replica(_v1 >= _v2, "{} vs {}", _v1, _v2);                                         \
    } while (false)

#define CHECK_LE_PREFIX(var1, var2)                                                                \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        dassert_replica(_v1 <= _v2, "{} vs {}", _v1, _v2);                                         \
    } while (false)

#define CHECK_GT_PREFIX(var1, var2)                                                                \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        dassert_replica(_v1 > _v2, "{} vs {}", _v1, _v2);                                          \
    } while (false)

#define CHECK_LT_PREFIX(var1, var2)                                                                \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        dassert_replica(_v1 < _v2, "{} vs {}", _v1, _v2);                                          \
    } while (false)

// Return the given status if condition is not true.
#define ERR_LOG_AND_RETURN_NOT_TRUE(s, err, ...)                                                   \
    do {                                                                                           \
        if (dsn_unlikely(!(s))) {                                                                  \
            LOG_ERROR_F("{}: {}", err, fmt::format(__VA_ARGS__));                                  \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

// Return the given status if it is not ERR_OK.
#define ERR_LOG_AND_RETURN_NOT_OK(s, ...)                                                          \
    do {                                                                                           \
        error_code _err = (s);                                                                     \
        ERR_LOG_AND_RETURN_NOT_TRUE(_err == ERR_OK, _err, __VA_ARGS__);                            \
    } while (0)
