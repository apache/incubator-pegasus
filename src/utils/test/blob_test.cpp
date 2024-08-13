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

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "utils/blob.h"

namespace dsn {

struct blob_base_case
{
    std::string expected_str;
};

class BlobBaseTest : public testing::TestWithParam<blob_base_case>
{
public:
    void SetUp() override
    {
        const auto &test_case = GetParam();
        expected_str = test_case.expected_str;
    }

    void check_blob_value(const blob &obj) const
    {
        EXPECT_EQ(expected_str, obj.to_string());

        EXPECT_EQ(expected_str.size(), obj.length());
        EXPECT_EQ(expected_str.size(), obj.size());

        if (expected_str.empty()) {
            EXPECT_TRUE(obj.empty());
        } else {
            EXPECT_FALSE(obj.empty());
        }
    }

    std::string expected_str;
};

const std::vector<blob_base_case> blob_base_tests = {
    // Test empty case.
    {""},
    // Test non-empty case.
    {"hello"},
};

class BlobCreateTest : public BlobBaseTest
{
};

TEST_P(BlobCreateTest, CreateFromCString)
{
    const auto &obj = blob::create_from_bytes(expected_str.data(), expected_str.size());
    check_blob_value(obj);
}

TEST_P(BlobCreateTest, CreateFromString)
{
    const auto &obj = blob::create_from_bytes(std::string(expected_str));
    check_blob_value(obj);
}

INSTANTIATE_TEST_SUITE_P(BlobTest, BlobCreateTest, testing::ValuesIn(blob_base_tests));

class BlobInitTest : public BlobBaseTest
{
public:
    blob create() { return blob::create_from_bytes(std::string(expected_str)); }
};

TEST_P(BlobInitTest, CopyConstructor)
{
    const auto &obj = create();

    blob copy(obj);
    check_blob_value(copy);
}

TEST_P(BlobInitTest, CopyAssignment)
{
    const auto &obj = create();

    blob copy;
    copy = obj;
    check_blob_value(copy);
}

TEST_P(BlobInitTest, MoveConstructor)
{
    auto obj = create();

    blob move(std::move(obj));
    check_blob_value(move);
}

TEST_P(BlobInitTest, MoveAssignment)
{
    auto obj = create();

    blob move;
    move = std::move(obj);
    check_blob_value(move);
}

INSTANTIATE_TEST_SUITE_P(BlobTest, BlobInitTest, testing::ValuesIn(blob_base_tests));

} // namespace dsn
