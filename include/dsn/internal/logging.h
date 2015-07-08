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
# pragma once

# include <exception>
# include <stdarg.h>
# include <cstdlib>
# include <dsn/internal/coredump.h>
# include <dsn/internal/enum_helper.h>
# include <dsn/internal/configuration.h>

#ifdef _WIN32
__pragma(warning(disable:4127))
#endif

namespace dsn {

enum logging_level
{
    log_level_INVALID,

    log_level_INFORMATION,
    log_level_DEBUG,
    log_level_WARNING,
    log_level_ERROR,
    log_level_FATAL    
};

ENUM_BEGIN(logging_level, log_level_INVALID)
    ENUM_REG(log_level_INFORMATION)
    ENUM_REG(log_level_DEBUG)
    ENUM_REG(log_level_WARNING)
    ENUM_REG(log_level_ERROR)
    ENUM_REG(log_level_FATAL)
ENUM_END(logging_level)

extern logging_level logging_start_level;

extern void log_init(configuration_ptr config);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, va_list args);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, ...);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title);
} // end namespace

#define dlog(level, title, ...) do {if (level >= ::dsn::logging_start_level) dsn::logv(__FILE__, __FUNCTION__, __LINE__, level, title, __VA_ARGS__); } while(false)
#define dinfo(...)  dlog(dsn::log_level_INFORMATION, __TITLE__, __VA_ARGS__)
#define ddebug(...) dlog(dsn::log_level_DEBUG, __TITLE__, __VA_ARGS__)
#define dwarn(...)  dlog(dsn::log_level_WARNING, __TITLE__, __VA_ARGS__)
#define derror(...) dlog(dsn::log_level_ERROR, __TITLE__, __VA_ARGS__)
#define dfatal(...) dlog(dsn::log_level_FATAL, __TITLE__, __VA_ARGS__)
#define dassert(x, ...) do { if (!(x)) {                    \
        dlog(dsn::log_level_FATAL, "assert", #x);           \
        dlog(dsn::log_level_FATAL, "assert", __VA_ARGS__);  \
        ::dsn::utils::coredump::write(); ::abort();         \
    } } while (false)

#ifdef _DEBUG
#define dbg_dassert dassert
#else
#define dbg_dassert(x, ...) 
#endif
