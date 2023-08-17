// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "http/http_client.h"
#include "http/http_method.h"
#include "utils/error_code.h"
#include "utils/errors.h"

namespace dsn {

TEST(http_client_test, connect)
{
    http_client client;
    ASSERT_TRUE(client.init().is_ok());

    // No one has listened on port 20000, thus this would lead to "Connection refused".
    client.set_url("http://127.0.0.1:20000/test/get");

    const auto &err = client.do_method();
    ASSERT_EQ(dsn::ERR_CURL_FAILED, err.code());

    // Would print something like "Failed to connect to 127.0.0.1 port 20000: Connection refused".
    std::cout << "failed to connect: " << err.description() << std::endl;
}

void test_http_client(http_client &client,
                      const http_method method,
                      const long expected_http_status,
                      const std::string &expected_response)
{
    client.set_method(method);

    std::string actual_response;
    ASSERT_TRUE(client.do_method(&actual_response));

    long actual_http_status;
    ASSERT_TRUE(client.get_http_status(actual_http_status).is_ok());
    EXPECT_EQ(expected_http_status, actual_http_status);
    EXPECT_EQ(expected_response, actual_response);
}

TEST(http_client_test, get)
{
    http_client client;
    ASSERT_TRUE(client.init().is_ok());

    client.set_url("http://127.0.0.1:20001/test/get");

    test_http_client(client, http_method::POST, 400, "please use GET method");
    test_http_client(client, http_method::GET, 200, "you are using GET method");
}

} // namespace dsn
