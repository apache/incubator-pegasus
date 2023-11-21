/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <string>

#include "base/pegasus_utils.h"
#include "gtest/gtest.h"

const std::string test_string = "pegasus";

TEST(pegasus_utils, redact_sensitive_string)
{
    FLAGS_encrypt_data_at_rest = true;
    auto result_string = pegasus::utils::redact_sensitive_string(test_string);
    ASSERT_EQ(pegasus::utils::kRedactedString, result_string);
}

TEST(pegasus_utils, redact_sensitive_string_with_encrypt)
{
    FLAGS_encrypt_data_at_rest = false;
    auto result_string = pegasus::utils::redact_sensitive_string(test_string);
    ASSERT_EQ("pegasus", result_string);
}

TEST(pegasus_utils, c_escape_sensitive_string_with_no_encrypt_and_escape)
{
    FLAGS_encrypt_data_at_rest = false;
    auto result_string = pegasus::utils::c_escape_sensitive_string(test_string, false);
    ASSERT_EQ("pegasus", result_string);
}

TEST(pegasus_utils, c_escape_sensitive_string_with_no_encrypt)
{
    FLAGS_encrypt_data_at_rest = false;
    auto result_string = pegasus::utils::c_escape_sensitive_string(test_string, true);
    ASSERT_EQ("\\x70\\x65\\x67\\x61\\x73\\x75\\x73", result_string);
}

TEST(pegasus_utils, c_escape_sensitive_string_with_encrypt)
{
    FLAGS_encrypt_data_at_rest = true;
    auto result_string = pegasus::utils::c_escape_sensitive_string(test_string, false);
    ASSERT_EQ(pegasus::utils::kRedactedString, result_string);
}

TEST(pegasus_utils, c_escape_sensitive_string_with_encrypt_and_escape)
{
    FLAGS_encrypt_data_at_rest = true;
    auto result_string = pegasus::utils::c_escape_sensitive_string(test_string, true);
    ASSERT_EQ(pegasus::utils::kRedactedString, result_string);
}
