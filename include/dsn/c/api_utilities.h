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

/*
 * Description:
 *     useful utilities in rDSN exposed via C API
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/c/api_common.h>
#include <dsn/utility/ports.h>

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
    LOG_LEVEL_INFORMATION,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_COUNT,
    LOG_LEVEL_INVALID
} dsn_log_level_t;

// logs with level smaller than this start_level will not be logged
extern DSN_API dsn_log_level_t dsn_log_start_level;
extern DSN_API dsn_log_level_t dsn_log_get_start_level();
extern DSN_API void dsn_log_set_start_level(dsn_log_level_t level);
extern DSN_API void dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             va_list args);
extern DSN_API void dsn_logf(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             ...);
extern DSN_API void
dsn_log(const char *file, const char *function, const int line, dsn_log_level_t log_level);
extern DSN_API void dsn_coredump();

// __FILENAME__ macro comes from the cmake, in which we calculate a filename without path.
#define dlog(level, ...)                                                                           \
    do {                                                                                           \
        if (level >= dsn_log_start_level)                                                          \
            dsn_logf(__FILENAME__, __FUNCTION__, __LINE__, level, __VA_ARGS__);                    \
    } while (false)
#define dinfo(...) dlog(LOG_LEVEL_INFORMATION, __VA_ARGS__)
#define ddebug(...) dlog(LOG_LEVEL_DEBUG, __VA_ARGS__)
#define dwarn(...) dlog(LOG_LEVEL_WARNING, __VA_ARGS__)
#define derror(...) dlog(LOG_LEVEL_ERROR, __VA_ARGS__)
#define dfatal(...) dlog(LOG_LEVEL_FATAL, __VA_ARGS__)
#define dassert(x, ...)                                                                            \
    do {                                                                                           \
        if (dsn_unlikely(!(x))) {                                                                  \
            dlog(LOG_LEVEL_FATAL, "assertion expression: " #x);                                    \
            dlog(LOG_LEVEL_FATAL, __VA_ARGS__);                                                    \
            dsn_coredump();                                                                        \
        }                                                                                          \
    } while (false)

#define dreturn_not_ok_logged(err, ...)                                                            \
    do {                                                                                           \
        if ((err) != dsn::ERR_OK) {                                                                \
            derror(__VA_ARGS__);                                                                   \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

#ifndef NDEBUG
#define dbg_dassert dassert
#else
#define dbg_dassert(x, ...)
#endif

#ifdef DSN_MOCK_TEST
#define mock_private public
#define mock_virtual virtual
#else
#define mock_private private
#define mock_virtual
#endif

/*@}*/

#define dverify(exp)                                                                               \
    if (!(exp))                                                                                    \
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
        if (!(exp)) {                                                                              \
            dlog(level, __VA_ARGS__);                                                              \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define dstop_on_false(exp)                                                                        \
    if (!(exp))                                                                                    \
    return
#define dstop_on_false_logged(exp, level, ...)                                                     \
    do {                                                                                           \
        if (!(exp)) {                                                                              \
            dlog(level, __VA_ARGS__);                                                              \
            return;                                                                                \
        }                                                                                          \
    } while (0)
/*@}*/
