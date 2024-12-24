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
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/ports.h"

DSN_DECLARE_uint64(max_replicas_on_load_for_each_disk);

namespace dsn::replication {

class mock_load_replica : public replica_stub
{
public:
    mock_load_replica() = default;

    ~mock_load_replica() override = default;

    void initialize(const std::map<std::string, std::string> &dirs_by_tag,
                    const std::map<std::string, std::vector<gpid>> &replicas_by_tag)
    {
        // Get dirs and tags to initialize fs_manager.
        std::vector<std::string> dirs;
        std::vector<std::string> tags;
        for (const auto &[tag, dir] : dirs_by_tag) {
            dirs.push_back(dir);
            tags.push_back(tag);
        }

        // Initialize fs_manager by the tag and dir of each disk.
        _fs_manager.initialize(dirs, tags);

        // Generate the replicas which are expected after loading.
        for (const auto &[tag, reps] : replicas_by_tag) {
            for (const auto &pid : reps) {
                ASSERT_TRUE(_expected_loaded_replica_pids.insert(pid).second);
            }
        }

        _disk_tags_for_order.clear();
        _disk_dirs_for_order.clear();
        _disk_replicas_for_order.clear();
        _disk_loaded_replicas_for_order.assign(replicas_by_tag.size(), 0);

        // Ensure that the disks are scanned in the order returned by `get_dir_node()`.
        for (const auto &dn : _fs_manager.get_dir_nodes()) {
            for (const auto &pid : replicas_by_tag.at(dn->tag)) {
                _fs_manager.specify_dir_for_new_replica_for_test(dn.get(), "pegasus", pid);
            }

            _disk_tags_for_order.push_back(dn->tag);
            _disk_dirs_for_order.push_back(dn->full_dir);
            _disk_replicas_for_order.push_back(replicas_by_tag.at(dn->tag).size());
        }

        ASSERT_EQ(_disk_tags_for_order.size(), _disk_dirs_for_order.size());
    }

    void test_load_replicas(bool test_load_order, uint64_t max_replicas_on_load_for_each_disk)
    {
        PRESERVE_VAR(allow_inline, dsn::task_spec::get(LPC_REPLICATION_INIT_LOAD)->allow_inline);
        dsn::task_spec::get(LPC_REPLICATION_INIT_LOAD)->allow_inline = test_load_order;

        PRESERVE_FLAG(max_replicas_on_load_for_each_disk);
        FLAGS_max_replicas_on_load_for_each_disk = max_replicas_on_load_for_each_disk;

        // Check if all loaded replicas are matched.
        replica_stub::replica_map_by_gpid actual_loaded_replicas;
        load_replicas(actual_loaded_replicas);
        ASSERT_EQ(_expected_loaded_replicas, actual_loaded_replicas);

        // Check if all replicas have been loaded.
        std::set<gpid> actual_loaded_replica_pids;
        for (const auto &[pid, _] : actual_loaded_replicas) {
            ASSERT_TRUE(actual_loaded_replica_pids.insert(pid).second);
        }
        ASSERT_EQ(_expected_loaded_replica_pids, actual_loaded_replica_pids);
    }

    void remove_disk_dirs()
    {
        for (const auto &dn : _fs_manager.get_dir_nodes()) {
            ASSERT_TRUE(utils::filesystem::remove_path(dn->full_dir));
        }
    }

private:
    void load_replica_for_test(dir_node *dn, const std::string &replica_dir, replica_ptr &rep)
    {
        ASSERT_TRUE(utils::filesystem::directory_exists(replica_dir));

        const auto &dir_name = get_replica_dir_name(replica_dir);

        gpid pid;
        std::string app_type;
        ASSERT_TRUE(parse_replica_dir_name(dir_name, pid, app_type));
        ASSERT_STREQ("pegasus", app_type.c_str());

        ASSERT_EQ(LPC_REPLICATION_INIT_LOAD, task::get_current_task()->spec().code);

        if (task::get_current_task()->spec().allow_inline) {
            // Once the task is `allow_inline`, it would be executed in place immediately rather
            // than pushed into the queue. Thus we could test the expected order in which the
            // tasks are pushed into the queue.

            // Find the first disk where there is still some replica that has not been loaded.
            size_t finished_disks = 0;
            while (_disk_loaded_replicas_for_order[_disk_index_for_order] >=
                   _disk_replicas_for_order[_disk_index_for_order]) {
                // Since current task has not been executed, it is not possible that all disks
                // are finished.
                ++finished_disks;
                ASSERT_GT(_disk_tags_for_order.size(), finished_disks);

                // Skip to next disk since all of the replicas of this disk have been loaded.
                _disk_index_for_order = (_disk_index_for_order + 1) % _disk_tags_for_order.size();
            }

            // Only check if the processed order of the disk the replica belongs to, rather than
            // the order of the replica itself, for the reason that the order of the dirs returned
            // by the underlying call might vary.
            ASSERT_EQ(_disk_tags_for_order[_disk_index_for_order], dn->tag);
            ASSERT_EQ(_disk_dirs_for_order[_disk_index_for_order], dn->full_dir);

            // Current replica has been loaded, move forward to the next replica of this disk.
            ++_disk_loaded_replicas_for_order[_disk_index_for_order];

            // Turn to next disks if some of them still have some replicas that are not loaded.
            _disk_index_for_order = (_disk_index_for_order + 1) % _disk_tags_for_order.size();
        }

        // Check the absolute dir of this replica.
        ASSERT_EQ(dn->replica_dir("pegasus", pid), replica_dir);

        app_info ai;
        ai.app_type = "pegasus";
        rep = new replica(this, pid, ai, dn, false);
        rep->_app = std::make_unique<replication::mock_replication_app_base>(rep);

        std::lock_guard<std::mutex> guard(_mtx);

        ASSERT_TRUE(_expected_loaded_replicas.find(pid) == _expected_loaded_replicas.end());

        _expected_loaded_replicas[pid] = rep;
    }

    // Mock the process of loading a replica.
    replica_ptr load_replica(dir_node *dn, const std::string &replica_dir) override
    {
        replica_ptr rep;
        load_replica_for_test(dn, replica_dir, rep);
        return rep;
    }

    std::set<gpid> _expected_loaded_replica_pids;

    // The variables with postfix `_for_order` are only for testing the order of the loading
    // tasks.
    size_t _disk_index_for_order{0};
    std::vector<std::string> _disk_tags_for_order;
    std::vector<std::string> _disk_dirs_for_order;
    std::vector<size_t> _disk_replicas_for_order;
    std::vector<size_t> _disk_loaded_replicas_for_order;

    mutable std::mutex _mtx;
    replica_stub::replica_map_by_gpid _expected_loaded_replicas;

    DISALLOW_COPY_AND_ASSIGN(mock_load_replica);
    DISALLOW_MOVE_AND_ASSIGN(mock_load_replica);
};

struct load_replicas_case
{
    // Each disk tag => dir of this disk.
    std::map<std::string, std::string> dirs_by_tag;

    // Each disk tag => replicas (specified by <app_id, partition_id>) on this disk.
    std::map<std::string, std::vector<gpid>> replicas_by_tag;
};

class LoadReplicasTest : public testing::TestWithParam<load_replicas_case>
{
public:
    LoadReplicasTest()
    {
        // Remove all dirs of all disks to prevent each test from being disturbed.
        _stub.remove_disk_dirs();

        // Use test cases to initialize the replica stub.
        const auto &load_case = GetParam();
        _stub.initialize(load_case.dirs_by_tag, load_case.replicas_by_tag);
    }

    ~LoadReplicasTest() override { _stub.remove_disk_dirs(); }

    void test_load_replicas(bool test_load_order, uint64_t max_replicas_on_load_for_each_disk)
    {
        _stub.test_load_replicas(test_load_order, max_replicas_on_load_for_each_disk);
    }

private:
    mock_load_replica _stub;

    DISALLOW_COPY_AND_ASSIGN(LoadReplicasTest);
    DISALLOW_MOVE_AND_ASSIGN(LoadReplicasTest);
};

TEST_P(LoadReplicasTest, LoadReplicas) { test_load_replicas(false, 256); }

TEST_P(LoadReplicasTest, LoadOrder) { test_load_replicas(true, 256); }

TEST_P(LoadReplicasTest, LoadThrottling) { test_load_replicas(false, 5); }

// Generate a test case for loading replicas. Each element in `disk_replicas` is corresponding
// to the number of replicas on a disk.
load_replicas_case generate_load_replicas_case(const std::vector<size_t> &disk_replicas)
{
    std::map<std::string, std::string> dirs_by_tag;
    for (size_t disk_index = 0; disk_index < disk_replicas.size(); ++disk_index) {
        dirs_by_tag.emplace(fmt::format("data{}", disk_index), fmt::format("disk{}", disk_index));
    }

    static const uint32_t kNumBitsPartitions = 3;
    static const uint32_t kNumPartitions = 1U << kNumBitsPartitions;

    uint32_t app_id = 1;
    uint32_t partition_id = 0;

    std::map<std::string, std::vector<gpid>> replicas_by_tag;

    while (true) {
        size_t finished_disks = 0;

        for (size_t disk_index = 0; disk_index < disk_replicas.size(); ++disk_index) {
            auto &replica_list = replicas_by_tag[fmt::format("data{}", disk_index)];
            if (replica_list.size() >= disk_replicas[disk_index]) {
                // All replicas on this disk have been generated, just skip to next disk.
                ++finished_disks;
                continue;
            }

            // Generate a replica with current app id and partition index.
            replica_list.emplace_back(static_cast<int32_t>(app_id),
                                      static_cast<int32_t>(partition_id));

            // Once next partition index is found 0, increment app id to turn to next table.
            app_id += ((partition_id + 1) & kNumPartitions) >> kNumBitsPartitions;

            // Increment index to turn to next partition.
            partition_id = (partition_id + 1) & (kNumPartitions - 1);
        }

        if (finished_disks >= disk_replicas.size()) {
            // All disks have been done.
            break;
        }
    }

    return {dirs_by_tag, replicas_by_tag};
}

std::vector<load_replicas_case> generate_load_replicas_cases()
{
    // At least 1 disk should be included (otherwise it would lead to core dump), thus do
    // not generate the empty case (i.e. {}).
    return std::vector<load_replicas_case>({
        // There is only one disk which has none of replica.
        generate_load_replicas_case({0}),
        // There are two disks both of which have none of replica.
        generate_load_replicas_case({0, 0}),
        // There is only one disk which has one replica.
        generate_load_replicas_case({1}),
        // There are two disks one of which has one replica, and another has none.
        generate_load_replicas_case({1, 0}),
        generate_load_replicas_case({0, 1}),
        // There is only one disk which has two replicas.
        generate_load_replicas_case({2}),
        // There are two disks one of which has two replicas, and another has none.
        generate_load_replicas_case({2, 0}),
        generate_load_replicas_case({0, 2}),
        // There are at least three disks.
        generate_load_replicas_case({1, 0, 2}),
        generate_load_replicas_case({8, 25, 16}),
        generate_load_replicas_case({17, 96, 56, 127}),
        generate_load_replicas_case({22, 38, 0, 16}),
        generate_load_replicas_case({82, 75, 36, 118, 65}),
        generate_load_replicas_case({0, 92, 17, 68, 25}),
        // There are many replicas for some disks.
        generate_load_replicas_case({156, 367, 309, 58, 404, 298, 512, 82}),
        generate_load_replicas_case({167, 28, 898, 516, 389, 422, 682, 265, 596}),
    });
}

INSTANTIATE_TEST_SUITE_P(ReplicaStubTest,
                         LoadReplicasTest,
                         testing::ValuesIn(generate_load_replicas_cases()));

} // namespace dsn::replication
