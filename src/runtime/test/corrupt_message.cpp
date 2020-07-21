#include <vector>
#include <string>

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/tool-api/async_calls.h>

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
