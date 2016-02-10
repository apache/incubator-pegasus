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
 *     the tracer toollets traces all the asynchonous execution flow
 *     in the system through the join-point mechanism
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/c/app_model.h>

# ifdef __cplusplus
extern "C" {
# endif


/*!
\defgroup app-checker global checker
\ingroup service-api-c
@{
*/

/*!
\brief global checker (assertion) on state across nodes

rDSN allows global assert across many apps in the same process
the global assertions are called checkers.
*/
typedef void*       (*dsn_checker_create)( ///< return a checker
    const char*,    ///< checker name
    dsn_app_info*,  ///< apps available to the checker
    int             ///< apps count
    );
typedef void(*dsn_checker_apply)(void*); ///< run the given checker

extern DSN_API void      dsn_register_app_checker(
    const char* name,
    dsn_checker_create create,
    dsn_checker_apply apply
    );
/*@}*/

# ifdef __cplusplus
}
# endif