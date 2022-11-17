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
#include <vector>
#include <string>

#include <gtest/gtest.h>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "runtime/task/async_calls.h"

#include <boost/lexical_cast.hpp>
#include <iostream>

#include "test_utils.h"

// this only works with the fault injector
TEST(core, corrupt_message)
{
    int req = 0;
    ::dsn::rpc_address server("localhost", 20101);

    auto result = ::dsn::rpc::call_wait<std::string>(
        server, RPC_TEST_HASH1, req, std::chrono::milliseconds(0), 1);
    ASSERT_EQ(result.first, ERR_TIMEOUT);

    result = ::dsn::rpc::call_wait<std::string>(
        server, RPC_TEST_HASH2, req, std::chrono::milliseconds(0), 1);
    ASSERT_EQ(result.first, ERR_TIMEOUT);

    result = ::dsn::rpc::call_wait<std::string>(
        server, RPC_TEST_HASH3, req, std::chrono::milliseconds(0), 1);
    ASSERT_EQ(result.first, ERR_TIMEOUT);

    result = ::dsn::rpc::call_wait<std::string>(
        server, RPC_TEST_HASH4, req, std::chrono::milliseconds(0), 1);
    ASSERT_EQ(result.first, ERR_TIMEOUT);
}
