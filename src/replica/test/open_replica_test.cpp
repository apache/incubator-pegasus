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

#include <fmt/core.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/gpid.h"
#include "common/replication_common.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_data.h"
#include "meta_admin_types.h"
#include "mock_utils.h"
#include "replica/replica_stub.h"
#include "replica_test_base.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "task/task.h"
#include "utils/filesystem.h"

namespace dsn {
namespace replication {

class open_replica_test : public replica_test_base
{
public:
    open_replica_test() = default;
    ~open_replica_test() { dsn::utils::filesystem::remove_path("./tmp_dir"); }
};

INSTANTIATE_TEST_SUITE_P(, open_replica_test, ::testing::Values(false, true));

TEST_P(open_replica_test, open_replica_add_decree_and_ballot_check)
{
    app_info ai;
    ai.app_type = replication_options::kReplicaAppType;
    ai.is_stateful = true;
    ai.max_replica_count = 3;
    ai.partition_count = 8;
    ai.app_id = 11;

    struct test_data
    {
        ballot b;
        decree last_committed_decree;
        bool expect_crash;
    } tests[] = {{0, 0, false}, {5, 5, true}};
    uint16_t i = 0;
    for (auto test : tests) {
        gpid pid(ai.app_id, i);
        stub->_opening_replicas[pid] = task_ptr(nullptr);

        const auto node = host_port::from_string(fmt::format("127.0.0.11:{}", 12321 + i + 1));

        _replica->register_service();

        partition_configuration pc;
        pc.pid = pid;
        pc.ballot = test.b;
        pc.last_committed_decree = test.last_committed_decree;
        auto as = app_state::create(ai);

        auto req = std::make_shared<configuration_update_request>();
        req->info = *as;
        req->config = pc;
        req->type = config_type::CT_ASSIGN_PRIMARY;
        SET_IP_AND_HOST_PORT_BY_DNS(*req, node, node);
        if (test.expect_crash) {
            ASSERT_DEATH(stub->open_replica(ai, pid, nullptr, req), "");
        } else {
            stub->open_replica(ai, pid, nullptr, req);
        }
        // Both of the tests will fail, the replica is not exist in the stub.
        ASSERT_EQ(nullptr, stub->get_replica(pid));
        ++i;
    }
}
} // namespace replication
} // namespace dsn
