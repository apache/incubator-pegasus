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

#include "replica/replica.h"
#include "mock_utils.h"
#include "meta/meta_data.h"
#include "replica_test_base.h"

namespace dsn {
namespace replication {

class open_replica_test : public replica_test_base
{
public:
    open_replica_test() = default;
    ~open_replica_test() { dsn::utils::filesystem::remove_path("./tmp_dir"); }

    void test_open_replica()
    {
        app_info app_info;
        app_info.app_type = "replica";
        app_info.is_stateful = true;
        app_info.max_replica_count = 3;
        app_info.partition_count = 8;
        app_info.app_id = 1;

        struct test_data
        {
            ballot b;
            decree last_committed_decree;
            bool is_in_dir_nodes;
            bool exec_failed;
        } tests[] = {
            {0, 0, true, true}, {0, 0, false, false}, {5, 5, true, true}, {5, 5, false, true},
        };
        int i = 0;
        for (auto tt : tests) {
            gpid gpid(app_info.app_id, i);
            stub->_opening_replicas[gpid] = task_ptr(nullptr);

            dsn::rpc_address node;
            node.assign_ipv4("127.0.0.11", static_cast<uint16_t>(12321 + i + 1));

            if (!tt.is_in_dir_nodes) {
                dir_node *node_disk = new dir_node("tag_" + std::to_string(i), "tmp_dir");
                stub->_fs_manager._dir_nodes.emplace_back(node_disk);
                stub->_fs_manager._available_data_dirs.emplace_back("tmp_dir");
            }

            _replica->register_service();
            mock_mutation_log_shared_ptr shared_log_mock =
                new mock_mutation_log_shared("./tmp_dir");
            stub->set_log(shared_log_mock);
            partition_configuration config;
            config.pid = gpid;
            config.ballot = tt.b;
            config.last_committed_decree = tt.last_committed_decree;
            std::shared_ptr<app_state> _the_app = app_state::create(app_info);

            configuration_update_request fake_request;
            fake_request.info = *_the_app;
            fake_request.config = config;
            fake_request.type = config_type::CT_ASSIGN_PRIMARY;
            fake_request.node = node;

            std::shared_ptr<configuration_update_request> req2(new configuration_update_request);
            *req2 = fake_request;
            if (tt.exec_failed) {
                ASSERT_DEATH(stub->open_replica(app_info, gpid, nullptr, req2), "");
            } else {
                stub->open_replica(app_info, gpid, nullptr, req2);
            }
            ++i;
        }
    }
};

TEST_F(open_replica_test, open_replica_add_decree_and_ballot_check) { test_open_replica(); }

} // namespace replication
} // namespace dsn
