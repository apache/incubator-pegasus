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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "gtest/gtest.h"
#include "metadata_types.h"
#include "test_util/test_util.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"

using namespace dsn::utils::filesystem;

namespace dsn {
namespace replication {

TEST(dir_node, replica_dir)
{
    dir_node dn("tag", "path");
    ASSERT_EQ("path/1.0.test", dn.replica_dir("test", gpid(1, 0)));
}

class fs_manager_test : public pegasus::encrypt_data_test_base
{
};

INSTANTIATE_TEST_CASE_P(, fs_manager_test, ::testing::Values(false, true));

TEST_P(fs_manager_test, initialize)
{
    fail::setup();
    struct broken_disk_test
    {
        std::string create_dir_ok;
        std::string check_dir_rw_ok;
        // Regardless of the status of the disk, the number of dir_nodes should be 3.
        int32_t dir_node_size;
    } tests[]{{"true", "true", 3}, {"true", "false", 3}, {"false", "false", 3}};
    int i = 0;
    for (const auto &test : tests) {
        fail::cfg("filesystem_create_directory", "return(" + test.create_dir_ok + ")");
        fail::cfg("filesystem_check_dir_rw", "return(" + test.check_dir_rw_ok + ")");
        fs_manager fm;
        fm.initialize({"disk1", "disk2", "disk3"}, {"tag1", "tag2", "tag3"});
        ASSERT_EQ(test.dir_node_size, fm.get_dir_nodes().size()) << i;
        i++;
    }
    fail::teardown();
}

TEST_P(fs_manager_test, dir_update_disk_status)
{
    struct update_disk_status
    {
        bool mock_insufficient;
        disk_status::type old_disk_status;
        disk_status::type new_disk_status;
    } tests[] = {{false, disk_status::NORMAL, disk_status::NORMAL},
                 {false, disk_status::SPACE_INSUFFICIENT, disk_status::NORMAL},
                 {true, disk_status::NORMAL, disk_status::SPACE_INSUFFICIENT},
                 {true, disk_status::SPACE_INSUFFICIENT, disk_status::SPACE_INSUFFICIENT}};
    for (const auto &test : tests) {
        auto node = std::make_shared<dir_node>("tag", "path", 0, 0, 0, test.old_disk_status);
        fail::setup();
        if (test.mock_insufficient) {
            fail::cfg("filesystem_get_disk_space_info", "return(insufficient)");
        } else {
            fail::cfg("filesystem_get_disk_space_info", "return(normal)");
        }
        node->update_disk_stat();
        ASSERT_EQ(test.new_disk_status, node->status);
        fail::teardown();
    }
}

TEST_P(fs_manager_test, get_dir_node)
{
    fs_manager fm;
    fm.initialize({"./data1"}, {"data1"});
    const auto &dns = fm.get_dir_nodes();
    ASSERT_EQ(1, dns.size());
    const auto &base_dir =
        dns[0]->full_dir.substr(0, dns[0]->full_dir.size() - std::string("/data1").size());

    ASSERT_EQ(nullptr, fm.get_dir_node(""));
    ASSERT_EQ(nullptr, fm.get_dir_node("/"));

    ASSERT_NE(nullptr, fm.get_dir_node(base_dir + "/data1"));
    ASSERT_NE(nullptr, fm.get_dir_node(base_dir + "/data1/"));
    ASSERT_NE(nullptr, fm.get_dir_node(base_dir + "/data1/replica1"));

    ASSERT_EQ(nullptr, fm.get_dir_node(base_dir + "/data2"));
    ASSERT_EQ(nullptr, fm.get_dir_node(base_dir + "/data2/"));
    ASSERT_EQ(nullptr, fm.get_dir_node(base_dir + "/data2/replica1"));
}

TEST_P(fs_manager_test, find_replica_dir)
{
    fs_manager fm;
    fm.initialize({"./data1", "./data2", "./data3"}, {"data1", "data2", "data3"});

    const char *app_type = "find_replica_dir";
    gpid test_pid(1, 0);

    // Clear up the remaining directories if exist.
    for (const auto &dn : fm.get_dir_nodes()) {
        remove_path(dn->replica_dir(app_type, test_pid));
    }

    ASSERT_EQ(nullptr, fm.find_replica_dir(app_type, test_pid));
    auto dn = fm.create_replica_dir_if_necessary(app_type, test_pid);
    ASSERT_NE(nullptr, dn);
    const auto dir = dn->replica_dir(app_type, test_pid);
    ASSERT_TRUE(directory_exists(dir));
    auto dn1 = fm.find_replica_dir(app_type, test_pid);
    ASSERT_EQ(dn, dn1);
}

TEST_P(fs_manager_test, create_replica_dir_if_necessary)
{
    fs_manager fm;

    const char *app_type = "create_replica_dir_if_necessary";
    gpid test_pid(1, 0);

    // Could not find a valid dir_node.
    ASSERT_EQ(nullptr, fm.create_replica_dir_if_necessary(app_type, test_pid));

    // It's able to create a dir for the replica after a valid dir_node has been added.
    fm.add_new_dir_node("./data1", "data1");
    dir_node *dn = fm.create_replica_dir_if_necessary(app_type, test_pid);
    ASSERT_NE(nullptr, dn);
    ASSERT_EQ("data1", dn->tag);
}

TEST_P(fs_manager_test, create_child_replica_dir)
{
    fs_manager fm;
    fm.initialize({"./data1", "./data2", "./data3"}, {"data1", "data2", "data3"});

    const char *app_type = "create_child_replica_dir";
    gpid test_pid(1, 0);
    gpid test_child_pid(1, 0);

    dir_node *dn = fm.create_replica_dir_if_necessary(app_type, test_pid);
    ASSERT_NE(nullptr, dn);
    const auto dir = dn->replica_dir(app_type, test_pid);

    auto child_dn = fm.create_child_replica_dir(app_type, test_child_pid, dir);
    ASSERT_EQ(dn, child_dn);
    const auto child_dir = child_dn->replica_dir(app_type, test_child_pid);
    ASSERT_TRUE(directory_exists(child_dir));
    ASSERT_EQ(dir, child_dir);
}

TEST_P(fs_manager_test, find_best_dir_for_new_replica)
{
    // dn1 | 1.0,  1.1 +1.6
    // dn2 | 1.2,  1.3 +1.7   2.0
    // dn3 | 1.4  +1.5 +1.7   2.1
    auto dn1 = std::make_shared<dir_node>("./data1", "data1");
    dn1->holding_replicas[1] = {gpid(1, 0), gpid(1, 1)};
    auto dn2 = std::make_shared<dir_node>("./data2", "data2");
    dn2->holding_replicas[1] = {gpid(1, 2), gpid(1, 3)};
    dn2->holding_replicas[2] = {gpid(2, 0)};
    auto dn3 = std::make_shared<dir_node>("./data3", "data3");
    dn3->holding_replicas[1] = {gpid(1, 4)};
    dn3->holding_replicas[2] = {gpid(2, 1)};
    fs_manager fm;
    fm._dir_nodes = {dn1, dn2, dn3};

    gpid pid_1_5(1, 5);
    auto dn = fm.find_best_dir_for_new_replica(pid_1_5);
    ASSERT_EQ(dn3.get(), dn);
    dn->holding_replicas[pid_1_5.get_app_id()].emplace(pid_1_5);

    gpid pid_1_6(1, 6);
    dn = fm.find_best_dir_for_new_replica(pid_1_6);
    ASSERT_EQ(dn1.get(), dn);
    dn->holding_replicas[pid_1_6.get_app_id()].emplace(pid_1_6);

    gpid pid_1_7(1, 7);
    dn = fm.find_best_dir_for_new_replica(pid_1_7);
    ASSERT_TRUE(dn == dn2.get() || dn == dn3.get());
    dn->holding_replicas[pid_1_7.get_app_id()].emplace(pid_1_7);
}
} // namespace replication
} // namespace dsn
