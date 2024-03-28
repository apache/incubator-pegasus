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

// Some useful utility functions for logging and mocking provided by rDSN.

#pragma once

#include "utils/enum_helper.h"
#include "utils/fmt_utils.h"
#include "utils/ports.h"

/*!
@defgroup logging Logging Service
@ingroup service-api-utilities

 Logging Service

 Note developers can plug into rDSN their own logging libraryS
 implementation, so as to integrate rDSN logs into
 their own cluster operation systems.
@{
*/

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

USER_DEFINED_ENUM_FORMATTER(log_level_t)

// logs with level smaller than this start_level will not be logged
extern log_level_t log_start_level;
extern log_level_t get_log_start_level();
extern void set_log_start_level(log_level_t level);
extern void global_log(
    const char *file, const char *function, const int line, log_level_t log_level, const char *str);
extern void dsn_coredump();

#define dreturn_not_ok_logged(err, ...)                                                            \
    do {                                                                                           \
        if (dsn_unlikely((err) != dsn::ERR_OK)) {                                                  \
            LOG_ERROR(__VA_ARGS__);                                                                \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

#ifdef MOCK_TEST
#define mock_private public
#define mock_virtual virtual
#else
#define mock_private private
#define mock_virtual
#endif

/*@}*/

#define dverify(exp)                                                                               \
    if (dsn_unlikely(!(exp)))                                                                      \
    return false

#define dverify_exception(exp)                                                                     \
    do {                                                                                           \
        try {                                                                                      \
            exp;                                                                                   \
        } catch (...) {                                                                            \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define dverify_logged(exp, level, ...)                                                            \
    do {                                                                                           \
        if (dsn_unlikely(!(exp))) {                                                                \
            LOG(level, __VA_ARGS__);                                                               \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define dstop_on_false(exp)                                                                        \
    if (dsn_unlikely(!(exp)))                                                                      \
    return
#define dstop_on_false_logged(exp, level, ...)                                                     \
    do {                                                                                           \
        if (dsn_unlikely(!(exp))) {                                                                \
            LOG(level, __VA_ARGS__);                                                               \
            return;                                                                                \
        }                                                                                          \
    } while (0)
/*@}*/
