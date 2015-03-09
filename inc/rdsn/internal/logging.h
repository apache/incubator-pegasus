/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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

#ifdef _WIN32
__pragma(warning(disable:4127))
#endif

namespace rdsn {

enum logging_level
{
    log_level_INFORMATION,
    log_level_DEBUG,
    log_level_WARNING,
    log_level_ERROR,
    log_level_FATAL
};

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, va_list args);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, ...);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title);
} // end namespace

#define rlog(level, title, ...) rdsn::logv(__FILE__, __FUNCTION__, __LINE__, level, title, __VA_ARGS__)
#define rinfo(...)  rlog(rdsn::log_level_INFORMATION, __TITLE__, __VA_ARGS__)
#define rdebug(...) rlog(rdsn::log_level_DEBUG, __TITLE__, __VA_ARGS__)
#define rwarn(...)  rlog(rdsn::log_level_WARNING, __TITLE__, __VA_ARGS__)
#define rerror(...) rlog(rdsn::log_level_ERROR, __TITLE__, __VA_ARGS__)
#define rfatal(...) rlog(rdsn::log_level_FATAL, __TITLE__, __VA_ARGS__)
#define rassert(x, ...) do { if (!(x)) {rlog(rdsn::log_level_FATAL, #x, __VA_ARGS__); *((int*)0x1) = 0; } } while (false)

#ifdef _DEBUG
#define dbg_rassert rassert
#else
#define dbg_rassert(x, ...) 
#endif
