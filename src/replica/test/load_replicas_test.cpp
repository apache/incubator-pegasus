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

#include <map>
#include <string>
#include <vector>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "replica/replica_stub.h"
#include "replica/test/mock_utils.h"
#include "utils/filesystem.h"

namespace dsn {
namespace replication {

struct load_replicas_case
{
    std::map<std::string, std::string> dirs_by_tag;
    std::map<std::string, std::vector<gpid>> replicas_by_tag;
};

class LoadReplicasTest : public replica_stub, public testing::TestWithParam<load_replicas_case>
{
public:
    LoadReplicasTest() {}

    ~LoadReplicasTest() override = default;

    void initialize(const std::map<std::string, std::string> &dirs_by_tag,
                    const std::map<std::string, std::vector<gpid>> &replicas_by_tag)
    {
        //
        std::vector<std::string> dirs;
        std::vector<std::string> tags;
        for (const auto &[tag, dir] : dirs_by_tag) {
            dirs.push_back(dir);
            tags.push_back(tag);
        }

        for (const auto &[tag, reps] : replicas_by_tag) {
            for (const auto &pid : reps) {
                ASSERT_TRUE(_expected_pids.insert(pid).second);
            }
        }

        //
        _fs_manager.initialize(dirs, tags);

        for (const auto &dn : _fs_manager.get_dir_nodes()) {
            for (const auto &pid : replicas_by_tag.at(dn->tag)) {
                _fs_manager.specify_dir_for_new_replica_for_test(dn.get(), "pegasus", pid);
            }
        }
    }

    void test_load_replicas()
    {
        replicas reps;
        load_replicas(reps);
        ASSERT_EQ(_loaded_replicas, reps);

        std::set<gpid> actual_pids;
        for (const auto &[pid, _] : reps) {
            ASSERT_TRUE(actual_pids.insert(pid).second);
        }
        ASSERT_EQ(_expected_pids, actual_pids);
    }

private:
    void load_replica_for_test(dir_node *dn, const char *dir, replica_ptr &rep)
    {
        ASSERT_TRUE(utils::filesystem::directory_exists(dir));

        const auto &dir_name = get_replica_dir_name(dir);

        gpid pid;
        std::string app_type;
        ASSERT_TRUE(parse_replica_dir_name(dir_name, pid, app_type));
        ASSERT_STREQ("pegasus", app_type.c_str());

        // Check full dir.
        ASSERT_EQ(dn->replica_dir("pegasus", pid), dir);

        std::lock_guard<std::mutex> guard(_mtx);

        ASSERT_TRUE(_loaded_replicas.find(pid) == _loaded_replicas.end());

        app_info ai;
        ai.app_type = "pegasus";
        rep = new replica(this, pid, ai, dn, false);
        rep->_app = std::make_unique<replication::mock_replication_app_base>(rep);
        _loaded_replicas[pid] = rep;
    }

    replica_ptr load_replica(dir_node *dn, const char *dir) override
    {
        replica_ptr rep;
        load_replica_for_test(dn, dir, rep);
        return rep;
    }

    std::set<gpid> _expected_pids;

    mutable std::mutex _mtx;
    replicas _loaded_replicas;
};

TEST_P(LoadReplicasTest, LoadReplicas)
{
    const auto &load_case = GetParam();
    initialize(load_case.dirs_by_tag, load_case.replicas_by_tag);
    test_load_replicas();
}

const std::vector<load_replicas_case> load_replicas_tests = {
    {{{"data0", "disk0"}}, {{"data0", {{1, 2}, {2, 5}}}}}};

INSTANTIATE_TEST_SUITE_P(ReplicaStubTest, LoadReplicasTest, testing::ValuesIn(load_replicas_tests));

} // namespace replication
} // namespace dsn
