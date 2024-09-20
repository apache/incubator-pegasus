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
#include <vector>

#include "common/gpid.h"
#include "gtest/gtest.h"
#include "replica/replica_stub.h"

namespace dsn::replication {

struct get_replica_dir_name_case
{
    std::string path;
    std::string expected_replica_dir_name;
};

class GetReplicaDirNameTest : public testing::TestWithParam<get_replica_dir_name_case>
{
public:
    static void test_get_replica_dir_name()
    {
        const auto &test_case = GetParam();
        const auto &actual_replica_dir_name = replica_stub::get_replica_dir_name(test_case.path);
        EXPECT_EQ(test_case.expected_replica_dir_name, actual_replica_dir_name);
    }
};

TEST_P(GetReplicaDirNameTest, GetReplicaDirName) { test_get_replica_dir_name(); }

const std::vector<get_replica_dir_name_case> get_replica_dir_name_tests{
    // Linux absolute path and non-empty dir name.
    {"/data/pegasus/1.2.pegasus", "1.2.pegasus"},
    // Linux absolute path and empty dir name.
    {"/data/pegasus/1.2.pegasus/", ""},
    // Windows absolute path and non-empty dir name.
    {R"(D:\data\pegasus\1.2.pegasus)", "1.2.pegasus"},
    // Windows absolute path and empty dir name.
    {R"(D:\data\pegasus\1.2.pegasus\)", ""},
    // Linux relative path and non-empty dir name.
    {"./1.2.pegasus", "1.2.pegasus"},
    // Linux relative path and empty dir name.
    {"./1.2.pegasus/", ""},
    // Windows relative path and non-empty dir name.
    {R"(.\1.2.pegasus)", "1.2.pegasus"},
    // Windows relative path and empty dir name.
    {R"(.\1.2.pegasus\)", ""},
};

INSTANTIATE_TEST_SUITE_P(ReplicaDirTest,
                         GetReplicaDirNameTest,
                         testing::ValuesIn(get_replica_dir_name_tests));

struct parse_replica_dir_name_case
{
    std::string replica_dir_name;
    bool ok;
    gpid expected_pid;
    std::string expected_app_type;
};

class ParseReplicaDirNameTest : public testing::TestWithParam<parse_replica_dir_name_case>
{
public:
    static void test_parse_replica_dir_name()
    {
        const auto &test_case = GetParam();

        gpid actual_pid;
        std::string actual_app_type;
        ASSERT_EQ(test_case.ok,
                  replica_stub::parse_replica_dir_name(
                      test_case.replica_dir_name, actual_pid, actual_app_type));
        if (!test_case.ok) {
            return;
        }

        EXPECT_EQ(test_case.expected_pid, actual_pid);
        EXPECT_EQ(test_case.expected_app_type, actual_app_type);
    }
};

TEST_P(ParseReplicaDirNameTest, ParseReplicaDirName) { test_parse_replica_dir_name(); }

const std::vector<parse_replica_dir_name_case> parse_replica_dir_name_tests{
    // Empty dir name.
    {"", false, {}, ""},
    // Single-digit IDs.
    {"1.2.pegasus", true, {1, 2}, "pegasus"},
    // Multi-digit IDs.
    {"1234.56789.pegasus", true, {1234, 56789}, "pegasus"},
    // Custom app type other than "pegasus".
    {"1.2.another", true, {1, 2}, "another"},
    // Custom app type with dot.
    {"1.2.another.pegasus", true, {1, 2}, "another.pegasus"},
    // Custom app type with other specific symbol.
    {"1.2.another_pegasus", true, {1, 2}, "another_pegasus"},
    // Missing one ID.
    {"1.pegasus", false, {}, ""},
    // Missing both IDs.
    {"pegasus", false, {}, ""},
    // ID with letter.
    {"1.2a.pegasus", false, {}, ""},
    // ID with minus.
    {"1.-2.pegasus", false, {}, ""},
};

INSTANTIATE_TEST_SUITE_P(ReplicaDirTest,
                         ParseReplicaDirNameTest,
                         testing::ValuesIn(parse_replica_dir_name_tests));

} // namespace dsn::replication
