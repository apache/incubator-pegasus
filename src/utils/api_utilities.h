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

// some useful utility functions provided by rDSN,
// such as logging, performance counter, checksum,
// command line interface registration and invocation,
// etc.

#pragma once

#include <stdarg.h>

#include "ports.h"

/*!
@defgroup logging Logging Service
@ingroup service-api-utilities

 Logging Service

 Note developers can plug into rDSN their own logging libraryS
 implementation, so as to integrate rDSN logs into
 their own cluster operation systems.
@{
*/

typedef enum dsn_log_level_t {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_COUNT,
    LOG_LEVEL_INVALID
} dsn_log_level_t;

// logs with level smaller than this start_level will not be logged
extern dsn_log_level_t dsn_log_start_level;
extern dsn_log_level_t dsn_log_get_start_level();
extern void dsn_log_set_start_level(dsn_log_level_t level);
extern void dsn_logv(const char *file,
                     const char *function,
                     const int line,
                     dsn_log_level_t log_level,
                     const char *fmt,
                     va_list args);
extern void dsn_logf(const char *file,
                     const char *function,
                     const int line,
                     dsn_log_level_t log_level,
                     const char *fmt,
                     ...);
extern void dsn_log(const char *file,
                    const char *function,
                    const int line,
                    dsn_log_level_t log_level,
                    const char *str);
extern void dsn_coredump();

// __FILENAME__ macro comes from the cmake, in which we calculate a filename without path.
#define dlog(level, ...)                                                                           \
    do {                                                                                           \
        if (level >= dsn_log_start_level)                                                          \
            dsn_logf(__FILENAME__, __FUNCTION__, __LINE__, level, __VA_ARGS__);                    \
    } while (false)

#define LOG_DEBUG(...) dlog(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define LOG_INFO(...) dlog(LOG_LEVEL_INFO, __VA_ARGS__)
#define LOG_WARNING(...) dlog(LOG_LEVEL_WARNING, __VA_ARGS__)
#define LOG_ERROR(...) dlog(LOG_LEVEL_ERROR, __VA_ARGS__)
#define LOG_FATAL(...) dlog(LOG_LEVEL_FATAL, __VA_ARGS__)

#define dreturn_not_ok_logged(err, ...)                                                            \
    do {                                                                                           \
        if (dsn_unlikely((err) != dsn::ERR_OK)) {                                                  \
            LOG_ERROR(__VA_ARGS__);                                                                \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

#ifdef DSN_MOCK_TEST
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
            dlog(level, __VA_ARGS__);                                                              \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define dstop_on_false(exp)                                                                        \
    if (dsn_unlikely(!(exp)))                                                                      \
    return
#define dstop_on_false_logged(exp, level, ...)                                                     \
    do {                                                                                           \
        if (dsn_unlikely(!(exp))) {                                                                \
            dlog(level, __VA_ARGS__);                                                              \
            return;                                                                                \
        }                                                                                          \
    } while (0)
/*@}*/
