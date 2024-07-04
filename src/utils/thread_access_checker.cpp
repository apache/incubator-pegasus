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

#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/thread_access_checker.h"

namespace dsn {

thread_access_checker::thread_access_checker() { _access_thread_id_inited = false; }

thread_access_checker::~thread_access_checker() { _access_thread_id_inited = false; }

void thread_access_checker::only_one_thread_access()
{
    if (_access_thread_id_inited) {
        CHECK_EQ_MSG(::dsn::utils::get_current_tid(),
                     _access_thread_id,
                     "the service is assumed to be accessed by one thread only!");
    } else {
        _access_thread_id = ::dsn::utils::get_current_tid();
        _access_thread_id_inited = true;
    }
}
} // namespace dsn
