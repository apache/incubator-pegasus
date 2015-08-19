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

# include <dsn/internal/aio_provider.h>
# include <gtest/gtest.h>
# include <dsn/service_api_cpp.h>
# include "test_utils.h"

TEST(core, rpc)
{
    int req = 0;
    std::string result;
    dsn_address_t server;
    dsn_address_build(&server, "localhost", 20101);

    ::dsn::message_ptr response;
    auto err = ::dsn::rpc::call_typed_wait(
        &response,
        server,
        RPC_TEST_HASH,
        req,
        1
        );
    EXPECT_TRUE(err == ERR_OK);

    ::unmarshall(response.get(), result);
    EXPECT_TRUE(result.substr(result.length() - 9) == "default.1");

    err = ::dsn::rpc::call_typed_wait(
        &response,
        server,
        RPC_TEST_HASH,
        req,
        0
        );
    EXPECT_TRUE(err == ERR_TIMEOUT);
}
