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

#include "replica/test/replica_test_base.h"
#include "replica/backup/replica_backup_manager.h"

namespace dsn {
namespace replication {

class replica_backup_manager_test : public replica_test_base
{
public:
    void clear_backup_checkpoint(const std::string &policy_name)
    {
        _replica->get_backup_manager()->clear_backup_checkpoint(policy_name);
    }
};

TEST_F(replica_backup_manager_test, clear_cold_backup)
{
    std::string policy_name = "test_policy";

    // create policy dir: <backup_dir>/backup.<policy_name>.*
    std::string policy_dir = _replica->get_app()->backup_dir() + "/backup." + policy_name;
    utils::filesystem::create_directory(policy_dir);

    // clear policy dir
    clear_backup_checkpoint(policy_name);
    ASSERT_FALSE(utils::filesystem::directory_exists(policy_dir));
}

} // namespace replication
} // namespace dsn
