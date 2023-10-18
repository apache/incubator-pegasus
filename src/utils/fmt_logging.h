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
#include <rocksdb/status.h>

#include "utils/api_utilities.h"

// The macros below no longer use the default snprintf method for log message formatting,
// instead we use fmt::format.
// TODO(wutao1): prevent construction of std::string for each log.

#define dlog_f(level, ...)                                                                         \
    do {                                                                                           \
        if (level >= dsn_log_start_level)                                                          \
            dsn_log(                                                                               \
                __FILENAME__, __FUNCTION__, __LINE__, level, fmt::format(__VA_ARGS__).c_str());    \
    } while (false)

#define LOG_DEBUG(...) dlog_f(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define LOG_INFO(...) dlog_f(LOG_LEVEL_INFO, __VA_ARGS__)
#define LOG_WARNING(...) dlog_f(LOG_LEVEL_WARNING, __VA_ARGS__)
#define LOG_ERROR(...) dlog_f(LOG_LEVEL_ERROR, __VA_ARGS__)
#define LOG_FATAL(...) dlog_f(LOG_LEVEL_FATAL, __VA_ARGS__)

#define LOG_WARNING_IF(x, ...)                                                                     \
    do {                                                                                           \
        if (dsn_unlikely(x)) {                                                                     \
            LOG_WARNING(__VA_ARGS__);                                                              \
        }                                                                                          \
    } while (false)

#define LOG_ERROR_IF(x, ...)                                                                       \
    do {                                                                                           \
        if (dsn_unlikely(x)) {                                                                     \
            LOG_ERROR(__VA_ARGS__);                                                                \
        }                                                                                          \
    } while (false)

#define CHECK_EXPRESSION(expression, evaluation, ...)                                              \
    do {                                                                                           \
        if (dsn_unlikely(!(evaluation))) {                                                         \
            std::string assertion_info("assertion expression: [" #expression "] ");                \
            assertion_info += fmt::format(__VA_ARGS__);                                            \
            LOG_FATAL(assertion_info);                                                             \
        }                                                                                          \
    } while (false)

#define CHECK(x, ...) CHECK_EXPRESSION(x, x, __VA_ARGS__)
#define CHECK_NOTNULL(p, ...) CHECK((p) != nullptr, __VA_ARGS__)
#define CHECK_NULL(p, ...) CHECK((p) == nullptr, __VA_ARGS__)

// Macros for writing log message prefixed by log_prefix().
#define LOG_DEBUG_PREFIX(...) LOG_DEBUG("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_INFO_PREFIX(...) LOG_INFO("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_WARNING_PREFIX(...) LOG_WARNING("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_ERROR_PREFIX(...) LOG_ERROR("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define LOG_FATAL_PREFIX(...) LOG_FATAL("[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))

namespace {

inline const char *null_str_printer(const char *s) { return s == nullptr ? "(null)" : s; }

} // anonymous namespace

// Macros to check expected condition. It will abort the application
// and log a fatal message when the condition is not met.

#define CHECK_STREQ_MSG(var1, var2, ...)                                                           \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(var1 == var2,                                                             \
                         dsn::utils::equals(_v1, _v2),                                             \
                         "{} vs {} {}",                                                            \
                         null_str_printer(_v1),                                                    \
                         null_str_printer(_v2),                                                    \
                         fmt::format(__VA_ARGS__));                                                \
    } while (false)

#define CHECK_STRNE_MSG(var1, var2, ...)                                                           \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(var1 != var2,                                                             \
                         !dsn::utils::equals(_v1, _v2),                                            \
                         "{} vs {} {}",                                                            \
                         null_str_printer(_v1),                                                    \
                         null_str_printer(_v2),                                                    \
                         fmt::format(__VA_ARGS__));                                                \
    } while (false)

#define CHECK_STRCASEEQ_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(var1 == var2,                                                             \
                         dsn::utils::iequals(_v1, _v2),                                            \
                         "{} vs {} {}",                                                            \
                         null_str_printer(_v1),                                                    \
                         null_str_printer(_v2),                                                    \
                         fmt::format(__VA_ARGS__));                                                \
    } while (false)

#define CHECK_STRCASENE_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(var1 != var2,                                                             \
                         !dsn::utils::iequals(_v1, _v2),                                           \
                         "{} vs {} {}",                                                            \
                         null_str_printer(_v1),                                                    \
                         null_str_printer(_v2),                                                    \
                         fmt::format(__VA_ARGS__));                                                \
    } while (false)

#define CHECK_NE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 != var2, _v1 != _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_EQ_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 == var2, _v1 == _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_GE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 >= var2, _v1 >= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_LE_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 <= var2, _v1 <= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_GT_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 > var2, _v1 > _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));            \
    } while (false)

#define CHECK_LT_MSG(var1, var2, ...)                                                              \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION(                                                                          \
            var1 < var2, _v1 < _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));            \
    } while (false)

#define CHECK_STREQ(var1, var2) CHECK_STREQ_MSG(var1, var2, "")
#define CHECK_STRNE(var1, var2) CHECK_STRNE_MSG(var1, var2, "")

#define CHECK_NE(var1, var2) CHECK_NE_MSG(var1, var2, "")
#define CHECK_EQ(var1, var2) CHECK_EQ_MSG(var1, var2, "")
#define CHECK_GE(var1, var2) CHECK_GE_MSG(var1, var2, "")
#define CHECK_LE(var1, var2) CHECK_LE_MSG(var1, var2, "")
#define CHECK_GT(var1, var2) CHECK_GT_MSG(var1, var2, "")
#define CHECK_LT(var1, var2) CHECK_LT_MSG(var1, var2, "")

#define CHECK_TRUE(var) CHECK_EQ(var, true)
#define CHECK_FALSE(var) CHECK_EQ(var, false)

// TODO(yingchun): add CHECK_NULL(ptr), CHECK_OK(err), CHECK(cond)

#define CHECK_EXPRESSION_PREFIX_MSG(expression, evaluation, ...)                                   \
    CHECK_EXPRESSION(expression, evaluation, "[{}] {}", log_prefix(), fmt::format(__VA_ARGS__))
#define CHECK_PREFIX_MSG(x, ...) CHECK_EXPRESSION_PREFIX_MSG(x, x, __VA_ARGS__)
#define CHECK_NOTNULL_PREFIX_MSG(p, ...) CHECK_PREFIX_MSG(p != nullptr, __VA_ARGS__)
#define CHECK_NOTNULL_PREFIX(p) CHECK_NOTNULL_PREFIX_MSG(p, "")

#define CHECK_NE_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 != var2, _v1 != _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_EQ_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 == var2, _v1 == _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_GE_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 >= var2, _v1 >= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_LE_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 <= var2, _v1 <= _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));          \
    } while (false)

#define CHECK_GT_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 > var2, _v1 > _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));            \
    } while (false)

#define CHECK_LT_PREFIX_MSG(var1, var2, ...)                                                       \
    do {                                                                                           \
        const auto &_v1 = (var1);                                                                  \
        const auto &_v2 = (var2);                                                                  \
        CHECK_EXPRESSION_PREFIX_MSG(                                                               \
            var1 < var2, _v1 < _v2, "{} vs {} {}", _v1, _v2, fmt::format(__VA_ARGS__));            \
    } while (false)

#define CHECK_PREFIX(x) CHECK_PREFIX_MSG(x, "")
#define CHECK_NE_PREFIX(var1, var2) CHECK_NE_PREFIX_MSG(var1, var2, "")
#define CHECK_EQ_PREFIX(var1, var2) CHECK_EQ_PREFIX_MSG(var1, var2, "")
#define CHECK_GE_PREFIX(var1, var2) CHECK_GE_PREFIX_MSG(var1, var2, "")
#define CHECK_LE_PREFIX(var1, var2) CHECK_LE_PREFIX_MSG(var1, var2, "")
#define CHECK_GT_PREFIX(var1, var2) CHECK_GT_PREFIX_MSG(var1, var2, "")
#define CHECK_LT_PREFIX(var1, var2) CHECK_LT_PREFIX_MSG(var1, var2, "")
#define CHECK_OK_PREFIX(x) CHECK_EQ_PREFIX_MSG(x, ::dsn::ERR_OK, "")

// Return the given status if condition is not true.
#define LOG_AND_RETURN_NOT_TRUE(level, s, err, ...)                                                \
    do {                                                                                           \
        if (dsn_unlikely(!(s))) {                                                                  \
            LOG_##level("{}: {}", err, fmt::format(__VA_ARGS__));                                  \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

// Return the given status if it is not ERR_OK.
#define LOG_AND_RETURN_NOT_OK(level, s, ...)                                                       \
    do {                                                                                           \
        ::dsn::error_code _err = (s);                                                              \
        LOG_AND_RETURN_NOT_TRUE(level, _err == ::dsn::ERR_OK, _err, __VA_ARGS__);                  \
    } while (0)

// Return the given rocksdb::Status 's' if it is not OK.
#define LOG_AND_RETURN_NOT_RDB_OK(level, s, ...)                                                   \
    do {                                                                                           \
        const auto &_s = (s);                                                                      \
        if (dsn_unlikely(!_s.ok())) {                                                              \
            LOG_##level("{}: {}", _s.ToString(), fmt::format(__VA_ARGS__));                        \
            return _s;                                                                             \
        }                                                                                          \
    } while (0)

#ifndef NDEBUG
#define DCHECK CHECK
#define DCHECK_NOTNULL CHECK_NOTNULL

#define DCHECK_NE_MSG CHECK_NE_MSG
#define DCHECK_EQ_MSG CHECK_EQ_MSG
#define DCHECK_GE_MSG CHECK_GE_MSG
#define DCHECK_LE_MSG CHECK_LE_MSG
#define DCHECK_GT_MSG CHECK_GT_MSG
#define DCHECK_LT_MSG CHECK_LT_MSG

#define DCHECK_NE CHECK_NE
#define DCHECK_EQ CHECK_EQ
#define DCHECK_GE CHECK_GE
#define DCHECK_LE CHECK_LE
#define DCHECK_GT CHECK_GT
#define DCHECK_LT CHECK_LT

#define DCHECK_NE_PREFIX CHECK_NE_PREFIX
#define DCHECK_EQ_PREFIX CHECK_EQ_PREFIX
#define DCHECK_GE_PREFIX CHECK_GE_PREFIX
#define DCHECK_LE_PREFIX CHECK_LE_PREFIX
#define DCHECK_GT_PREFIX CHECK_GT_PREFIX
#define DCHECK_LT_PREFIX CHECK_LT_PREFIX
#else
#define DCHECK(x, ...)
#define DCHECK_NOTNULL(p, ...)

#define DCHECK_NE_MSG(var1, var2, ...)
#define DCHECK_EQ_MSG(var1, var2, ...)
#define DCHECK_GE_MSG(var1, var2, ...)
#define DCHECK_LE_MSG(var1, var2, ...)
#define DCHECK_GT_MSG(var1, var2, ...)
#define DCHECK_LT_MSG(var1, var2, ...)

#define DCHECK_NE(var1, var2)
#define DCHECK_EQ(var1, var2)
#define DCHECK_GE(var1, var2)
#define DCHECK_LE(var1, var2)
#define DCHECK_GT(var1, var2)
#define DCHECK_LT(var1, var2)

#define DCHECK_NE_PREFIX(var1, var2)
#define DCHECK_EQ_PREFIX(var1, var2)
#define DCHECK_GE_PREFIX(var1, var2)
#define DCHECK_LE_PREFIX(var1, var2)
#define DCHECK_GT_PREFIX(var1, var2)
#define DCHECK_LT_PREFIX(var1, var2)
#endif
