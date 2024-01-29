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

#include <fmt/core.h>
#include <stdint.h>
// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-param-test.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "http/http_client.h"
#include "http/http_method.h"
#include "http/http_status_code.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/test_macros.h"

// IWYU pragma: no_forward_declare dsn::HttpClientMethodTest_ExecMethodByUrlString_Test
// IWYU pragma: no_forward_declare dsn::HttpClientMethodTest_ExecMethodByCopyUrlObject_Test
// IWYU pragma: no_forward_declare dsn::HttpClientMethodTest_ExecMethodByMoveUrlObject_Test
namespace dsn {

void check_expected_description_prefix(const std::string &expected_description_prefix,
                                       const dsn::error_s &err)
{
    const std::string actual_description(err.description());
    std::cout << actual_description << std::endl;

    ASSERT_LE(expected_description_prefix.size(), actual_description.size());
    EXPECT_EQ(expected_description_prefix,
              actual_description.substr(0, expected_description_prefix.size()));
}

const std::string kTestUrlA("http://192.168.1.2/test/api0?key0=val0");
const std::string kTestUrlB("http://10.10.1.2/test/api1?key1=val1");
const std::string kTestUrlC("http://172.16.1.2/test/api2?key2=val2");

void set_url_string(http_url &url, const std::string &str)
{
    ASSERT_TRUE(url.set_url(str.c_str()));
}

void check_url_eq(const http_url &url, const std::string &expected_str)
{
    std::string actual_str;
    ASSERT_TRUE(url.to_string(actual_str));
    EXPECT_EQ(expected_str, actual_str);
}

void init_test_url(http_url &url)
{
    // Set original url with kTestUrlA before copy.
    set_url_string(url, kTestUrlA);
    check_url_eq(url, kTestUrlA);
}

void test_after_copy(http_url &url_1, http_url &url_2)
{
    // Check if both urls are expected immediately after copy.
    check_url_eq(url_1, kTestUrlA);
    check_url_eq(url_2, kTestUrlA);

    // Set both urls with values other than kTestUrlA.
    set_url_string(url_2, kTestUrlB);
    check_url_eq(url_1, kTestUrlA);
    check_url_eq(url_2, kTestUrlB);

    set_url_string(url_1, kTestUrlC);
    check_url_eq(url_1, kTestUrlC);
    check_url_eq(url_2, kTestUrlB);
}

TEST(HttpUrlTest, CallCopyConstructor)
{
    http_url url_1;
    init_test_url(url_1);

    http_url url_2(url_1);
    test_after_copy(url_1, url_2);
}

TEST(HttpUrlTest, CallCopyAssignmentOperator)
{
    http_url url_1;
    init_test_url(url_1);

    http_url url_2;
    url_2 = url_1;
    test_after_copy(url_1, url_2);
}

void test_after_move(http_url &url_1, http_url &url_2)
{
    // Check if both urls are expected immediately after move.
    EXPECT_THAT(url_1._url, testing::IsNull());
    check_url_eq(url_2, kTestUrlA);

    // Set another url with value other than kTestUrlA.
    set_url_string(url_2, kTestUrlB);
    EXPECT_THAT(url_1._url, testing::IsNull());
    check_url_eq(url_2, kTestUrlB);
}

TEST(HttpUrlTest, CallMoveConstructor)
{
    http_url url_1;
    init_test_url(url_1);

    http_url url_2(std::move(url_1));
    test_after_move(url_1, url_2);
}

TEST(HttpUrlTest, CallMoveAssignmentOperator)
{
    http_url url_1;
    init_test_url(url_1);

    http_url url_2;
    url_2 = std::move(url_1);
    test_after_move(url_1, url_2);
}

const std::string kTestHost("192.168.1.2");
const std::string kTestHttpUrl(fmt::format("http://{}/", kTestHost));

struct http_scheme_case
{
    http_scheme scheme;
    error_code expected_err_code;
    std::string expected_description_prefix;
    std::string expected_str;
};

class HttpSchemeTest : public testing::TestWithParam<http_scheme_case>
{
protected:
    void test_scheme(const http_scheme_case &scheme_case)
    {
        const auto &actual_err = _url.set_scheme(scheme_case.scheme);
        ASSERT_EQ(scheme_case.expected_err_code, actual_err.code());
        if (!actual_err) {
            NO_FATALS(check_expected_description_prefix(scheme_case.expected_description_prefix,
                                                        actual_err));
            return;
        }

        ASSERT_TRUE(_url.set_host(kTestHost.c_str()));

        std::string actual_str;
        ASSERT_TRUE(_url.to_string(actual_str));
        EXPECT_EQ(scheme_case.expected_str, actual_str);
    }

private:
    http_url _url;
};

TEST_P(HttpSchemeTest, SetScheme) { test_scheme(GetParam()); }

const std::vector<http_scheme_case> http_scheme_tests = {
    {http_scheme::kHttp, ERR_OK, "", kTestHttpUrl},
    {http_scheme::kHttps,
     ERR_CURL_FAILED,
     "ERR_CURL_FAILED: failed to set CURLUPART_SCHEME to https: code=5, desc=\"Unsupported URL "
     "scheme\"",
     ""},
    {http_scheme::kFtp,
     ERR_CURL_FAILED,
     "ERR_CURL_FAILED: failed to set CURLUPART_SCHEME to ftp: code=5, desc=\"Unsupported URL "
     "scheme\"",
     ""},
};

INSTANTIATE_TEST_SUITE_P(HttpClientTest, HttpSchemeTest, testing::ValuesIn(http_scheme_tests));

TEST(HttpUrlTest, Clear)
{
    http_url url;
    ASSERT_TRUE(url.set_url(kTestUrlA.c_str()));

    std::string str;
    ASSERT_TRUE(url.to_string(str));
    ASSERT_EQ(kTestUrlA, str);

    url.clear();

    // After cleared, at least it should be set with some host; otherwise, extracting the URL
    // string would lead to error.
    const auto &err = url.to_string(str);
    ASSERT_FALSE(err);
    const std::string expected_description_prefix(
        "ERR_CURL_FAILED: failed to get CURLUPART_URL: code=14, desc=\"No host part in the URL\"");
    NO_FATALS(check_expected_description_prefix(expected_description_prefix, err));

    ASSERT_TRUE(url.set_host(kTestHost.c_str()));
    ASSERT_TRUE(url.to_string(str));
    ASSERT_EQ(kTestHttpUrl, str);
}

struct http_url_build_case
{
    const char *scheme;
    const char *host;
    uint16_t port;
    const char *path;
    const char *query;
    const char *expected_url;
};

class HttpUrlBuildTest : public testing::TestWithParam<http_url_build_case>
{
protected:
    void test_build_url(const http_url_build_case &build_case)
    {
        if (!utils::is_empty(build_case.scheme)) {
            // Empty scheme will lead to error.
            ASSERT_TRUE(_url.set_scheme(build_case.scheme));
        }

        ASSERT_TRUE(_url.set_host(build_case.host));
        ASSERT_TRUE(_url.set_port(build_case.port));
        ASSERT_TRUE(_url.set_path(build_case.path));
        ASSERT_TRUE(_url.set_query(build_case.query));

        std::string actual_url;
        ASSERT_TRUE(_url.to_string(actual_url));
        EXPECT_STREQ(build_case.expected_url, actual_url.c_str());
    }

private:
    http_url _url;
};

TEST_P(HttpUrlBuildTest, BuildUrl) { test_build_url(GetParam()); }

const std::vector<http_url_build_case> http_url_tests = {
    // Test default scheme, specified ip, empty path and query.
    {nullptr, "10.10.1.2", 34801, "", "", "http://10.10.1.2:34801/"},
    // Test default scheme, specified host, empty path and query.
    {nullptr, "www.example.com", 8080, "", "", "http://www.example.com:8080/"},
    // Test default scheme, specified ip and path, empty query.
    {nullptr, "10.10.1.2", 34801, "/api", "", "http://10.10.1.2:34801/api"},
    // Test default scheme, specified host and path, empty query.
    {nullptr, "www.example.com", 8080, "/api", "", "http://www.example.com:8080/api"},
    // Test default scheme, specified ip, path and query.
    {nullptr, "10.10.1.2", 34801, "/api", "foo=bar", "http://10.10.1.2:34801/api?foo=bar"},
    // Test default scheme, specified ip, path and query.
    {nullptr,
     "www.example.com",
     8080,
     "/api",
     "foo=bar",
     "http://www.example.com:8080/api?foo=bar"},
    // Test default scheme, specified ip, path and query with multiple keys.
    {nullptr,
     "10.10.1.2",
     34801,
     "/api",
     "key1=abc&key2=123456",
     "http://10.10.1.2:34801/api?key1=abc&key2=123456"},
    // Test default scheme, specified ip, multi-level path and query with multiple keys.
    {nullptr,
     "10.10.1.2",
     34801,
     "/api/multi/level/path",
     "key1=abc&key2=123456",
     "http://10.10.1.2:34801/api/multi/level/path?key1=abc&key2=123456"},
    // Test specified scheme, ip, path and query with multiple keys.
    {"http",
     "10.10.1.2",
     34801,
     "/api",
     "key1=abc&key2=123456",
     "http://10.10.1.2:34801/api?key1=abc&key2=123456"},
    // Test specified scheme, ip, multi-level path and query with multiple keys.
    {"http",
     "10.10.1.2",
     34801,
     "/api/multi/level/path",
     "key1=abc&key2=123456",
     "http://10.10.1.2:34801/api/multi/level/path?key1=abc&key2=123456"},
};

INSTANTIATE_TEST_SUITE_P(HttpUrlTest, HttpUrlBuildTest, testing::ValuesIn(http_url_tests));

TEST(HttpClientTest, Connect)
{
    http_client client;
    ASSERT_TRUE(client.init());

    // No one has listened on port 20000, thus this would lead to "Connection refused".
    ASSERT_TRUE(client.set_url("http://127.0.0.1:20000/test/get"));

    const auto &err = client.exec_method();
    ASSERT_EQ(dsn::ERR_CURL_FAILED, err.code());

    std::cout << "failed to connect: ";

    // "code=7" means CURLE_COULDNT_CONNECT, see https://curl.se/libcurl/c/libcurl-errors.html
    // for details.
    //
    // We just check the prefix of description, including `method`, `url`, `code` and `desc`.
    // The `msg` differ in various systems, such as:
    // * msg="Failed to connect to 127.0.0.1 port 20000: Connection refused"
    // * msg="Failed to connect to 127.0.0.1 port 20000 after 0 ms: Connection refused"
    // Thus we don't check if `msg` fields are consistent.
    const std::string expected_description_prefix(
        "ERR_CURL_FAILED: failed to perform http request("
        "method=GET, url=http://127.0.0.1:20000/test/get): code=7, "
        "desc=\"Couldn't connect to server\"");
    NO_FATALS(check_expected_description_prefix(expected_description_prefix, err));
}

TEST(HttpClientTest, Callback)
{
    http_client client;
    ASSERT_TRUE(client.init());

    ASSERT_TRUE(client.set_url("http://127.0.0.1:20001/test/get"));
    ASSERT_TRUE(client.with_get_method());

    auto callback = [](const void *, size_t) { return false; };

    const auto &err = client.exec_method(callback);
    ASSERT_EQ(dsn::ERR_CURL_FAILED, err.code());

    http_status_code actual_http_status;
    ASSERT_TRUE(client.get_http_status(actual_http_status));
    EXPECT_EQ(http_status_code::kOk, actual_http_status);

    std::cout << "failed for callback: ";

    // "code=23" means CURLE_WRITE_ERROR, see https://curl.se/libcurl/c/libcurl-errors.html
    // for details.
    //
    // We just check the prefix of description, including `method`, `url`, `code` and `desc`.
    // The `msg` differ in various systems, such as:
    // * msg="Failed writing body (18446744073709551615 != 24)"
    // * msg="Failure writing output to destination"
    // Thus we don't check if `msg` fields are consistent.
    const auto expected_description_prefix =
        fmt::format("ERR_CURL_FAILED: failed to perform http request("
                    "method=GET, url=http://127.0.0.1:20001/test/get): code=23, "
                    "desc=\"Failed writing received data to disk/application\"");
    NO_FATALS(check_expected_description_prefix(expected_description_prefix, err));
}

struct http_client_method_case
{
    const char *host;
    uint16_t port;
    const char *path;
    http_method method;
    const char *post_data;
    http_status_code expected_http_status;
    const char *expected_response;
};

class HttpClientMethodTest : public testing::TestWithParam<http_client_method_case>
{
protected:
    void SetUp() override { ASSERT_TRUE(_client.init()); }

    void test_method_with_response_string(const http_status_code expected_http_status,
                                          const std::string &expected_response)
    {
        std::string actual_response;
        ASSERT_TRUE(_client.exec_method(&actual_response));

        http_status_code actual_http_status;
        ASSERT_TRUE(_client.get_http_status(actual_http_status));

        EXPECT_EQ(expected_http_status, actual_http_status);
        EXPECT_EQ(expected_response, actual_response);
    }

    void test_method_with_response_callback(const http_status_code expected_http_status,
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
        ASSERT_TRUE(_client.exec_method(callback));

        http_status_code actual_http_status;
        ASSERT_TRUE(_client.get_http_status(actual_http_status));
        EXPECT_EQ(expected_http_status, actual_http_status);
    }

    void test_mothod(const http_method method,
                     const std::string &post_data,
                     const http_status_code expected_http_status,
                     const std::string &expected_response)
    {
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

    http_client _client;
};

#define BUILD_URL_STRING(host, port, path)                                                         \
    const auto &url = fmt::format("http://{}:{}{}", host, port, path)

#define BUILD_URL_OBJECT(host, port, path)                                                         \
    http_url url;                                                                                  \
    do {                                                                                           \
        ASSERT_TRUE(url.set_host(host));                                                           \
        ASSERT_TRUE(url.set_port(port));                                                           \
        ASSERT_TRUE(url.set_path(path));                                                           \
    } while (0)

#define TEST_HTTP_CLIENT_EXEC_METHOD(url_builder, ...)                                             \
    do {                                                                                           \
        const auto &method_case = GetParam();                                                      \
        url_builder(method_case.host, method_case.port, method_case.path);                         \
        ASSERT_TRUE(_client.set_url(__VA_ARGS__(url)));                                            \
        test_mothod(method_case.method,                                                            \
                    method_case.post_data,                                                         \
                    method_case.expected_http_status,                                              \
                    method_case.expected_response);                                                \
    } while (0)

TEST_P(HttpClientMethodTest, ExecMethodByUrlString)
{
    TEST_HTTP_CLIENT_EXEC_METHOD(BUILD_URL_STRING);
}

TEST_P(HttpClientMethodTest, ExecMethodByCopyUrlObject)
{
    TEST_HTTP_CLIENT_EXEC_METHOD(BUILD_URL_OBJECT);
}

TEST_P(HttpClientMethodTest, ExecMethodByMoveUrlObject)
{
    TEST_HTTP_CLIENT_EXEC_METHOD(BUILD_URL_OBJECT, std::move);
}

const std::vector<http_client_method_case> http_client_method_tests = {
    {"127.0.0.1",
     20001,
     "/test/get",
     http_method::POST,
     "with POST DATA",
     http_status_code::kBadRequest,
     "please use GET method"},
    {"127.0.0.1",
     20001,
     "/test/get",
     http_method::GET,
     "",
     http_status_code::kOk,
     "you are using GET method"},
    {"127.0.0.1",
     20001,
     "/test/post",
     http_method::POST,
     "with POST DATA",
     http_status_code::kOk,
     "you are using POST method with POST DATA"},
    {"127.0.0.1",
     20001,
     "/test/post",
     http_method::GET,
     "",
     http_status_code::kBadRequest,
     "please use POST method"},
};

INSTANTIATE_TEST_SUITE_P(HttpClientTest,
                         HttpClientMethodTest,
                         testing::ValuesIn(http_client_method_tests));

struct http_get_case
{
    const char *host;
    uint16_t port;
    const char *path;
    error_code expected_err_code;
    std::string expected_description_prefix;
    http_status_code expected_status;
    const char *expected_body;
};

class HttpGetTest : public testing::TestWithParam<http_get_case>
{
};

template <typename TUrl>
void test_http_get(TUrl &&url,
                   const error_code &expected_err_code,
                   const std::string &expected_description_prefix,
                   const http_status_code expected_status,
                   const char *expected_body)
{
    const auto &result = http_get(std::forward<TUrl>(url));
    EXPECT_EQ(expected_err_code, result.error().code());
    EXPECT_EQ(expected_status, result.status());
    EXPECT_STREQ(expected_body, result.body().c_str());

    if (result) {
        EXPECT_EQ(dsn::ERR_OK, result.error().code());
    } else {
        EXPECT_NE(dsn::ERR_OK, result.error().code());
        NO_FATALS(check_expected_description_prefix(expected_description_prefix, result.error()));
    }
}

#define TEST_HTTP_GET(url_builder, ...)                                                            \
    do {                                                                                           \
        const auto &get_case = GetParam();                                                         \
        url_builder(get_case.host, get_case.port, get_case.path);                                  \
        test_http_get(__VA_ARGS__(url),                                                            \
                      get_case.expected_err_code,                                                  \
                      get_case.expected_description_prefix,                                        \
                      get_case.expected_status,                                                    \
                      get_case.expected_body);                                                     \
    } while (0)

TEST_P(HttpGetTest, GetByUrlString) { TEST_HTTP_GET(BUILD_URL_STRING); }

TEST_P(HttpGetTest, GetByCopyUrlObject) { TEST_HTTP_GET(BUILD_URL_OBJECT); }

TEST_P(HttpGetTest, GetByMoveUrlObject) { TEST_HTTP_GET(BUILD_URL_OBJECT, std::move); }

const std::vector<http_get_case> http_get_tests = {
    {
        "127.0.0.1",
        20000,
        "/test/get",
        dsn::ERR_CURL_FAILED,
        "ERR_CURL_FAILED: failed to perform http request("
        "method=GET, url=http://127.0.0.1:20000/test/get): code=7, "
        "desc=\"Couldn't connect to server\"",
        http_status_code::kInvalidCode,
        "",
    },
    {"127.0.0.1",
     20001,
     "/test/get",
     dsn::ERR_OK,
     "",
     http_status_code::kOk,
     "you are using GET method"},
    {"127.0.0.1",
     20001,
     "/test/post",
     dsn::ERR_OK,
     "",
     http_status_code::kBadRequest,
     "please use POST method"},
};

INSTANTIATE_TEST_SUITE_P(HttpClientTest, HttpGetTest, testing::ValuesIn(http_get_tests));

} // namespace dsn
