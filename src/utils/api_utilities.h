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

// Some useful utility functions for mocking provided by rDSN.

#pragma once

#include "utils/fmt_utils.h"
#include "utils/ports.h"

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
