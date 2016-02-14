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
 *     basic data structures and macros for rDSN service API
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <stdint.h>
# include <stddef.h>
# include <stdarg.h>

# ifdef __cplusplus
# define DEFAULT(value) = value
# define NORETURN [[noreturn]]
# else
# define DEFAULT(value)
# define NORETURN 
# include <stdbool.h>
# endif

# if defined(DSN_IN_CORE)
# if defined(_WIN32)
# define DSN_API __declspec(dllexport)
# else
# define DSN_API __attribute__((visibility("default")))
# endif
# else
# if defined(_WIN32)
# define DSN_API __declspec(dllimport)
# else
# define DSN_API
# endif
# endif

# ifdef __cplusplus
extern "C" {
# endif

# define DSN_MAX_TASK_CODE_NAME_LENGTH     48
# define DSN_MAX_ADDRESS_NAME_LENGTH       48
# define DSN_MAX_BUFFER_COUNT_IN_MESSAGE   64
# define DSN_MAX_APP_TYPE_NAME_LENGTH      32
# define DSN_MAX_APP_COUNT_IN_SAME_PROCESS 256
# define DSN_MAX_PATH                      1024
# define TIME_MS_MAX                       0xffffffff

struct dsn_app_info;
typedef struct      dsn_app_info dsn_app_info; ///< rDSN app information
typedef int         dsn_error_t;
typedef int         dsn_task_code_t;
typedef int         dsn_threadpool_code_t;
typedef void*       dsn_handle_t;
typedef void*       dsn_task_t;
typedef void*       dsn_task_tracker_t;
typedef void*       dsn_message_t;
typedef void*       dsn_group_t;
typedef void*       dsn_uri_t;

/*! the following ctrl code are used by \ref dsn_file_ctrl. */
typedef enum dsn_ctrl_code_t
{
    CTL_BATCH_INVALID,
    CTL_BATCH_WRITE,            ///< (batch) set write batch size
    CTL_MAX_CON_READ_OP_COUNT,  ///< (throttling) maximum concurrent read ops
    CTL_MAX_CON_WRITE_OP_COUNT, ///< (throttling) maximum concurrent write ops
} dsn_ctrl_code_t;



# ifdef __cplusplus
}
# endif