// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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
