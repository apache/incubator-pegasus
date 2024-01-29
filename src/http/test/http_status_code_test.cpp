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

#include "http/http_status_code.h"

#include <boost/algorithm/string/predicate.hpp>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"

namespace dsn {

using http_status_message_case = std::tuple<http_status_code, long>;

class HttpStatusMessageTest : public testing::TestWithParam<http_status_message_case>
{
};

TEST_P(HttpStatusMessageTest, GetHttpStatusMessage)
{
    http_status_code code;
    long expected_val;
    std::tie(code, expected_val) = GetParam();

    long actual_val(enum_to_val(code, kInvalidHttpStatus));
    ASSERT_EQ(expected_val, actual_val);

    std::string msg(get_http_status_message(code));

    if (actual_val == kInvalidHttpStatus) {
        EXPECT_TRUE(boost::algorithm::starts_with(msg, "Unknown"));
        return;
    }

    std::string actual_prefix(std::to_string(actual_val));
    EXPECT_TRUE(boost::algorithm::starts_with(msg, actual_prefix));
}

const std::vector<http_status_message_case> http_status_message_tests = {
    {http_status_code::kOk, 200},
    {http_status_code::kTemporaryRedirect, 307},
    {http_status_code::kBadRequest, 400},
    {http_status_code::kNotFound, 404},
    {http_status_code::kInternalServerError, 500},
    {http_status_code::kCount, kInvalidHttpStatus},
    {http_status_code::kInvalidCode, kInvalidHttpStatus},
};

INSTANTIATE_TEST_SUITE_P(HttpStatusCodeTest,
                         HttpStatusMessageTest,
                         testing::ValuesIn(http_status_message_tests));

} // namespace dsn
