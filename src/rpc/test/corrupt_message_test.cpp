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
// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "rpc/rpc_address.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "runtime/test_utils.h"
#include "utils/error_code.h"

// TODO(yingchun): this test is not running in CI, fix it later
// this only works with the fault injector
TEST(corrupt_message_test, DISABLED_basic)
{
    ::dsn::rpc_address server("localhost", 20101);

    std::vector<task_code> codes({RPC_TEST_HASH1, RPC_TEST_HASH2, RPC_TEST_HASH3, RPC_TEST_HASH4});
    for (const auto &code : codes) {
        auto result =
            ::dsn::rpc::call_wait<std::string>(server, code, 0, std::chrono::milliseconds(0), 1);
        ASSERT_EQ(ERR_TIMEOUT, result.first) << code;
    }
}
