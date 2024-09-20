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

#include <cstddef>
#include <cstdint>
#include <fmt/core.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "replica/test/mock_utils.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/ports.h"

namespace dsn::replication {

struct get_replica_dir_name_case
{
    std::string path;
    std::string expected_replica_dir_name;
};

class GetReplicaDirNameTest : public testing::TestWithParam<get_replica_dir_name_case>
{
public:
    GetReplicaDirNameTest() = default;

    ~GetReplicaDirNameTest() override = default;

    void test_get_replica_dir_name()
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
    {"D:\\data\\pegasus\\1.2.pegasus", "1.2.pegasus"},
    // Windows absolute path and empty dir name.
    {"D:\\data\\pegasus\\1.2.pegasus\\", ""},
    // Linux relative path and non-empty dir name.
    {"./1.2.pegasus", "1.2.pegasus"},
    // Linux relative path and empty dir name.
    {"./1.2.pegasus/", ""},
    // Windows relative path and non-empty dir name.
    {".\\1.2.pegasus", "1.2.pegasus"},
    // Windows relative path and empty dir name.
    {".\\1.2.pegasus\\", ""},
};

INSTANTIATE_TEST_SUITE_P(ReplicaDirTest,
                         GetReplicaDirNameTest,
                         testing::ValuesIn(get_replica_dir_name_tests));

} // namespace dsn::replication
