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
#include "runtime/test_utils.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/error_code.h"

// TODO(yingchun): the tests are failed because the fault injector is not work well as expected.
//  Now just disable the tests before we fix it.
// this only works with the fault injector
TEST(corrupt_message_test, DISABLED_basic)
{
    int req = 0;
    const auto server = dsn::rpc_address::from_host_port("localhost", 20101);
    std::vector<dsn::task_code> codes(
        {RPC_TEST_HASH1, RPC_TEST_HASH2, RPC_TEST_HASH3, RPC_TEST_HASH4});
    for (const auto &code : codes) {
        const auto result =
            ::dsn::rpc::call_wait<std::string>(server, code, req, std::chrono::milliseconds(1), 1);
        EXPECT_EQ(dsn::ERR_TIMEOUT, result.first) << code;
    }
}
