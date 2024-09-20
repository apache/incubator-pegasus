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

#include <functional>
#include <string>

#include "gtest/gtest.h"
#include "runtime/api_task.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_worker.h"
#include "runtime/test_utils.h"
#include "utils/autoref_ptr.h"

DEFINE_TASK_CODE(LPC_TEST_HASH, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)

void on_lpc_test(void *p)
{
    std::string &result = *(std::string *)p;
    result = ::dsn::task::get_current_worker()->name();
}

TEST(lpc_test, basic)
{
    std::string result = "heheh";
    dsn::task_ptr t(new dsn::raw_task(LPC_TEST_HASH, std::bind(&on_lpc_test, (void *)&result), 1));
    t->enqueue();
    t->wait();
    EXPECT_TRUE(result.substr(0, result.length() - 2) == "client.THREAD_POOL_TEST_SERVER");
}
