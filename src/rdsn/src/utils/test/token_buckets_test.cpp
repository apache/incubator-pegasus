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

#include <gtest/gtest.h>
#include "utils/token_buckets.h"

namespace dsn {
namespace utils {

class token_buckets_test : public testing::Test
{
public:
    std::unique_ptr<dsn::utils::token_buckets> _token_buckets_wrapper;

    void SetUp() override
    {
        _token_buckets_wrapper = std::make_unique<dsn::utils::token_buckets>();
    }

    std::unordered_map<std::string, std::shared_ptr<folly::DynamicTokenBucket>>
    token_buckets() const
    {
        return _token_buckets_wrapper->_token_buckets;
    }
};

TEST_F(token_buckets_test, test_token_buckets)
{
    auto token1 = _token_buckets_wrapper->get_token_bucket("test1");
    auto token2 = _token_buckets_wrapper->get_token_bucket("test1");
    ASSERT_EQ(token_buckets().size(), 1);
    ASSERT_EQ(token1, token2);

    auto token3 = _token_buckets_wrapper->get_token_bucket("test2");
    ASSERT_EQ(token_buckets().size(), 2);
    ASSERT_NE(token1, token3);
}

} // namespace utils
} // namespace dsn
