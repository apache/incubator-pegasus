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
// IWYU pragma: no_include <gtest/gtest-param-test.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <cstring>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

#include "http/http_client.h"
#include "http/http_method.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"

namespace dsn {

TEST(HttpClientTest, Connect)
{
    http_client client;
    ASSERT_TRUE(client.init());

    // No one has listened on port 20000, thus this would lead to "Connection refused".
    ASSERT_TRUE(client.set_url("http://127.0.0.1:20000/test/get"));

    const auto &err = client.do_method();
    ASSERT_EQ(dsn::ERR_CURL_CONNECT_FAILED, err.code());

    const std::string actual_description(err.description());
    std::cout << "failed to connect: " << actual_description << std::endl;

    const std::string expected_description_prefix(
        "ERR_CURL_CONNECT_FAILED: failed to perform http request("
        "method=GET, url=http://127.0.0.1:20000/test/get): code=7");
    ASSERT_LT(expected_description_prefix.size(), actual_description.size());
    EXPECT_EQ(expected_description_prefix,
              actual_description.substr(0, expected_description_prefix.size()));
}

TEST(HttpClientTest, Callback)
{
    http_client client;
    ASSERT_TRUE(client.init());

    ASSERT_TRUE(client.set_url("http://127.0.0.1:20001/test/get"));
    ASSERT_TRUE(client.with_get_method());

    auto callback = [](const void *, size_t) { return false; };

    const auto &err = client.do_method(callback);
    ASSERT_EQ(dsn::ERR_CURL_WRITE_ERROR, err.code());

    long actual_http_status;
    ASSERT_TRUE(client.get_http_status(actual_http_status));
    EXPECT_EQ(200, actual_http_status);

    const std::string actual_description(err.description());
    std::cout << "failed for callback: " << actual_description << std::endl;
    const auto expected_description =
        fmt::format("ERR_CURL_WRITE_ERROR: failed to perform http request("
                    "method=GET, url=http://127.0.0.1:20001/test/get): code=23, "
                    "desc=\"Failed writing received data to disk/application\", "
                    "msg=\"Failed writing body ({} != 24)\"",
                    std::numeric_limits<size_t>::max());
    EXPECT_EQ(expected_description, actual_description);
}

using http_client_method_case =
    std::tuple<const char *, http_method, const char *, long, const char *>;

class HttpClientMethodTest : public testing::TestWithParam<http_client_method_case>
{
public:
    void SetUp() override { ASSERT_TRUE(_client.init()); }

    void test_method_with_response_string(const long expected_http_status,
                                          const std::string &expected_response)
    {
        std::string actual_response;
        ASSERT_TRUE(_client.do_method(&actual_response));

        long actual_http_status;
        ASSERT_TRUE(_client.get_http_status(actual_http_status));

        EXPECT_EQ(expected_http_status, actual_http_status);
        EXPECT_EQ(expected_response, actual_response);
    }

    void test_method_with_response_callback(const long expected_http_status,
                                            const std::string &expected_response)
    {
        auto callback = [&expected_response](const void *data, size_t length) {
            auto compare = [](const char *expected_data,
                              size_t expected_length,
                              const void *actual_data,
                              size_t actual_length) {
                if (expected_length != actual_length) {
                    return false;
                }
                return std::memcmp(expected_data, actual_data, actual_length) == 0;
            };
            EXPECT_PRED4(compare, expected_response.data(), expected_response.size(), data, length);
            return true;
        };
        ASSERT_TRUE(_client.do_method(callback));

        long actual_http_status;
        ASSERT_TRUE(_client.get_http_status(actual_http_status));
        EXPECT_EQ(expected_http_status, actual_http_status);
    }

    void test_mothod(const std::string &url,
                     const http_method method,
                     const std::string &post_data,
                     const long expected_http_status,
                     const std::string &expected_response)
    {
        _client.set_url(url);

        switch (method) {
        case http_method::GET:
            ASSERT_TRUE(_client.with_get_method());
            break;
        case http_method::POST:
            ASSERT_TRUE(_client.with_post_method(post_data));
            break;
        default:
            LOG_FATAL("Unsupported http_method");
        }

        test_method_with_response_string(expected_http_status, expected_response);
        test_method_with_response_callback(expected_http_status, expected_response);
    }

private:
    http_client _client;
};

TEST_P(HttpClientMethodTest, DoMethod)
{
    const char *url;
    http_method method;
    const char *post_data;
    long expected_http_status;
    const char *expected_response;
    std::tie(url, method, post_data, expected_http_status, expected_response) = GetParam();

    http_client _client;
    test_mothod(url, method, post_data, expected_http_status, expected_response);
}

const std::vector<http_client_method_case> http_client_method_tests = {
    {"http://127.0.0.1:20001/test/get",
     http_method::POST,
     "with POST DATA",
     400,
     "please use GET method"},
    {"http://127.0.0.1:20001/test/get", http_method::GET, "", 200, "you are using GET method"},
    {"http://127.0.0.1:20001/test/post",
     http_method::POST,
     "with POST DATA",
     200,
     "you are using POST method with POST DATA"},
    {"http://127.0.0.1:20001/test/post", http_method::GET, "", 400, "please use POST method"},
};

INSTANTIATE_TEST_CASE_P(HttpClientTest,
                        HttpClientMethodTest,
                        testing::ValuesIn(http_client_method_tests));

} // namespace dsn
