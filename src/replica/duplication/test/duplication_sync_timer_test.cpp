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

#include "replica/duplication/duplication_sync_timer.h"

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/replication.codes.h"
#include "duplication_test_base.h"
#include "gtest/gtest.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/duplication/replica_duplicator_manager.h"
#include "replica/test/mock_utils.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "utils/error_code.h"

namespace dsn {
namespace replication {

class duplication_sync_timer_test : public duplication_test_base
{
public:
    void SetUp() override { dup_sync = std::make_unique<duplication_sync_timer>(stub.get()); }

    void TearDown() override { stub.reset(); }

    void test_on_duplication_sync_reply()
    {
        static const std::string kTestRemoteClusterName = "slave-cluster";
        static const std::string kTestRemoteAppName = "temp";

        // replica: {app_id:2, partition_id:1, duplications:{}}
        auto *rep = stub->add_primary_replica(2, 1);
        rep->init_private_log(rep->dir());
        ASSERT_NE(stub->find_replica(2, 1), nullptr);

        // appid:2 -> dupid:1
        duplication_entry ent;
        ent.dupid = 1;
        ent.remote = kTestRemoteClusterName;
        ent.__set_remote_app_name(kTestRemoteAppName);
        ent.status = duplication_status::DS_PAUSE;
        ent.progress[1] = 1000; // partition 1 => confirmed 1000
        duplication_sync_response resp;
        resp.dup_map[2] = {{ent.dupid, ent}};

        dup_sync->_rpc_task = new raw_task(LPC_TEST, []() {});
        dup_sync->on_duplication_sync_reply(ERR_OK, resp);
        replica_duplicator *dup =
            stub->find_replica(2, 1)->get_replica_duplicator_manager()._duplications[1].get();

        ASSERT_TRUE(dup);
        ASSERT_EQ(kTestRemoteClusterName, dup->_remote_cluster_name);
        ASSERT_EQ(kTestRemoteAppName, dup->_remote_app_name);
        ASSERT_EQ(duplication_status::DS_PAUSE, dup->_status);
        ASSERT_EQ(1000, dup->_progress.confirmed_decree);
        ASSERT_EQ(nullptr, dup_sync->_rpc_task);
    }

    void test_duplication_sync()
    {
        int total_app_num = 4;
        for (int appid = 1; appid <= total_app_num; appid++) {
            auto *rep = stub->add_non_primary_replica(appid, 1);
            rep->init_private_log(rep->dir());

            // trigger duplication sync on partition 1
            duplication_entry ent;
            ent.dupid = 1;
            ent.progress[rep->get_gpid().get_partition_index()] = 1000;
            ent.status = duplication_status::DS_PAUSE;
            auto dup = std::make_unique<replica_duplicator>(ent, rep);
            add_dup(rep, std::move(dup));
        }

        RPC_MOCKING(duplication_sync_rpc)
        {
            {
                // replica server should not sync to meta when it's disconnected
                dup_sync->run();
                ASSERT_EQ(0, duplication_sync_rpc::mail_box().size());
            }
            {
                // never collects confirm points from non-primaries
                stub->set_state_connected();
                dup_sync->run();
                ASSERT_EQ(1, duplication_sync_rpc::mail_box().size());

                auto &req = duplication_sync_rpc::mail_box().back().request();
                ASSERT_EQ(0, req.confirm_list.size());
            }
        }

        RPC_MOCKING(duplication_sync_rpc)
        {
            for (auto &e : stub->mock_replicas) {
                e.second->as_primary();
            }
            dup_sync->run();
            ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 1);

            auto &req = duplication_sync_rpc::mail_box().back().request();
            ASSERT_IP_AND_HOST_PORT(req, node, stub->primary_address(), stub->primary_host_port());

            // ensure confirm list is empty when no progress
            ASSERT_EQ(0, req.confirm_list.size());

            // ensure this rpc has timeout set.
            auto &rpc = duplication_sync_rpc::mail_box().back();
            ASSERT_GT(rpc.dsn_request()->header->client.timeout_ms, 0);
        }

        RPC_MOCKING(duplication_sync_rpc)
        {
            for (int appid = 1; appid <= total_app_num; appid++) {
                auto &dup = stub->mock_replicas[gpid(appid, 1)]
                                ->get_replica_duplicator_manager()
                                ._duplications[1];
                dup->update_progress(duplication_progress().set_last_decree(1500));
            }

            dup_sync->run();
            ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 1);

            auto &req = *duplication_sync_rpc::mail_box().back().mutable_request();
            ASSERT_IP_AND_HOST_PORT(req, node, stub->primary_address(), stub->primary_host_port());
            ASSERT_EQ(req.confirm_list.size(), total_app_num);

            for (int appid = 1; appid <= total_app_num; appid++) {
                ASSERT_TRUE(req.confirm_list.find(gpid(appid, 1)) != req.confirm_list.end());

                auto dup_list = req.confirm_list[gpid(appid, 1)];
                ASSERT_EQ(1, dup_list.size());

                auto dup = dup_list[0];
                ASSERT_EQ(1, dup.dupid);
                ASSERT_EQ(1500, dup.confirmed_decree);
            }
        }
    }

    void test_update_duplication_map()
    {
        std::map<int32_t, std::map<dupid_t, duplication_entry>> dup_map;
        for (int32_t appid = 1; appid <= 10; appid++) {
            for (int partition_id = 0; partition_id < 3; partition_id++) {
                auto *rep = stub->add_primary_replica(appid, partition_id);
                rep->init_private_log(rep->dir());
            }
        }

        { // Ensure update_duplication_map adds new duplications if they are not existed.
            duplication_entry ent;
            ent.dupid = 2;
            ent.status = duplication_status::DS_PAUSE;
            for (int i = 0; i < 3; i++) {
                ent.progress[i] = 0;
            }

            // add duplication 2 for app 1, 3, 5 (of course in real world cases duplication
            // will not be the same for different tables)
            dup_map[1][ent.dupid] = ent;
            dup_map[3][ent.dupid] = ent;
            dup_map[5][ent.dupid] = ent;

            dup_sync->update_duplication_map(dup_map);

            for (int32_t appid : {1, 3, 5}) {
                for (int partition_id : {0, 1, 2}) {
                    auto dup = find_dup(stub->find_replica(appid, partition_id), 2);
                    ASSERT_TRUE(dup);
                }
            }

            // update duplicated decree of 1, 3, 5 to 2
            auto dup = find_dup(stub->find_replica(1, 1), 2);
            dup->update_progress(dup->progress().set_last_decree(2));

            dup = find_dup(stub->find_replica(3, 1), 2);
            dup->update_progress(dup->progress().set_last_decree(2));

            dup = find_dup(stub->find_replica(5, 1), 2);
            dup->update_progress(dup->progress().set_last_decree(2));
        }

        RPC_MOCKING(duplication_sync_rpc)
        {
            stub->set_state_connected();
            dup_sync->run();
            ASSERT_EQ(1, duplication_sync_rpc::mail_box().size());

            auto &req = duplication_sync_rpc::mail_box().back().request();
            ASSERT_EQ(3, req.confirm_list.size());

            ASSERT_TRUE(req.confirm_list.find(gpid(1, 1)) != req.confirm_list.end());
            ASSERT_TRUE(req.confirm_list.find(gpid(3, 1)) != req.confirm_list.end());
            ASSERT_TRUE(req.confirm_list.find(gpid(5, 1)) != req.confirm_list.end());
        }

        {
            dup_map.erase(3);
            dup_sync->update_duplication_map(dup_map);
            ASSERT_TRUE(find_dup(stub->find_replica(1, 1), 2) != nullptr);
            ASSERT_TRUE(find_dup(stub->find_replica(3, 1), 2) == nullptr);
            ASSERT_TRUE(find_dup(stub->find_replica(5, 1), 2) != nullptr);
        }

        {
            dup_map.clear();
            dup_sync->update_duplication_map(dup_map);
            ASSERT_TRUE(find_dup(stub->find_replica(1, 1), 2) == nullptr);
            ASSERT_TRUE(find_dup(stub->find_replica(3, 1), 2) == nullptr);
            ASSERT_TRUE(find_dup(stub->find_replica(5, 1), 2) == nullptr);
        }
    }

    void test_update_on_non_primary()
    {
        stub->add_non_primary_replica(2, 1);

        duplication_entry ent;
        ent.dupid = 1;
        ent.status = duplication_status::DS_PAUSE;

        std::map<int32_t, std::map<dupid_t, duplication_entry>> dup_map;
        dup_map[2][ent.dupid] = ent; // app 2 doesn't have a primary replica

        dup_sync->update_duplication_map(dup_map);

        ASSERT_TRUE(stub->mock_replicas[gpid(2, 1)]
                        ->get_replica_duplicator_manager()
                        ._duplications.empty());
    }

    void test_update_confirmed_points()
    {
        for (int32_t appid = 1; appid <= 10; appid++) {
            auto *rep = stub->add_primary_replica(appid, 1);
            rep->init_private_log(rep->dir());
        }

        for (int appid = 1; appid <= 3; appid++) {
            auto *rep = stub->find_replica(appid, 1);

            duplication_entry ent;
            ent.dupid = 1;
            ent.status = duplication_status::DS_PAUSE;
            ent.progress[rep->get_gpid().get_partition_index()] = 0;
            auto dup = std::make_unique<replica_duplicator>(ent, rep);
            dup->update_progress(dup->progress().set_last_decree(3).set_confirmed_decree(1));
            add_dup(rep, std::move(dup));
        }

        duplication_entry ent;
        ent.dupid = 1;
        ent.progress[1] = 3; // app=[1,2,3], partition=1, confirmed=3
        duplication_sync_response resp;
        resp.dup_map[1][ent.dupid] = ent;
        resp.dup_map[2][ent.dupid] = ent;
        resp.dup_map[3][ent.dupid] = ent;

        dup_sync->on_duplication_sync_reply(ERR_OK, resp);

        for (int appid = 1; appid <= 3; appid++) {
            auto *rep = stub->find_replica(appid, 1);
            auto *dup = find_dup(rep, 1);

            ASSERT_EQ(3, dup->progress().confirmed_decree);
        }
    }

    // ensure dup-sync behaves correctly regardless
    // replica status transition (PRIMARY->SECONDARY/SECONDARY->PRIMARY)
    void test_replica_status_transition()
    {
        // 10 primaries
        int appid = 1;
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            auto *r = stub->add_primary_replica(appid, partition_id);
            r->init_private_log(r->dir());
        }

        duplication_entry ent;
        ent.dupid = 2;
        ent.status = duplication_status::DS_PAUSE;
        for (int i = 0; i < 10; i++) {
            ent.progress[i] = 0;
        }
        std::map<int32_t, std::map<dupid_t, duplication_entry>> dup_map;
        dup_map[appid][ent.dupid] = ent;

        dup_sync->update_duplication_map(dup_map);
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            ASSERT_NE(nullptr, find_dup(stub->find_replica(1, partition_id), 2)) << partition_id;
            ASSERT_EQ(2, find_dup(stub->find_replica(1, partition_id), 2)->id());
        }

        // primary -> secondary
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            stub->find_replica(1, partition_id)->as_secondary();
        }
        dup_sync->update_duplication_map(dup_map);
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            ASSERT_TRUE(stub->find_replica(1, partition_id)
                            ->get_duplication_manager()
                            ->_duplications.empty());
        }

        // secondary back to primary
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            stub->find_replica(1, partition_id)->as_primary();
        }
        dup_sync->update_duplication_map(dup_map);
        for (int partition_id = 0; partition_id < 10; partition_id++) {
            ASSERT_EQ(2, find_dup(stub->find_replica(1, partition_id), 2)->id());
        }

        // on meta's perspective, only 3 partitions are hosted on this server
        ent.progress.clear();
        for (int i = 0; i < 3; i++) {
            ent.progress[i] = 0;
        }
        dup_map[appid][ent.dupid] = ent;
        dup_sync->update_duplication_map(dup_map);
        for (int partition_id = 0; partition_id < 3; partition_id++) {
            ASSERT_EQ(2, find_dup(stub->find_replica(1, partition_id), 2)->id());
        }
        for (int partition_id = 3; partition_id < 10; partition_id++) {
            ASSERT_TRUE(stub->find_replica(1, partition_id)
                            ->get_duplication_manager()
                            ->_duplications.empty());
        }
    }

    // meta server doesn't suppose to sync duplication that's INIT or REMOVED
    // there must be some internal problems.
    void test_receive_illegal_duplication_status()
    {
        auto *rep = stub->add_primary_replica(1, 0);
        rep->init_private_log(rep->dir());

        duplication_entry ent;
        ent.dupid = 2;
        ent.status = duplication_status::DS_PAUSE;
        for (int i = 0; i < 16; i++) {
            ent.progress[i] = 0;
        }
        std::map<int32_t, std::map<dupid_t, duplication_entry>> dup_map;
        dup_map[1][ent.dupid] = ent;
        dup_sync->update_duplication_map(dup_map);
        ASSERT_EQ(find_dup(stub->find_replica(1, 0), 2)->_status, duplication_status::DS_PAUSE);

        ent.status = duplication_status::DS_INIT;
        dup_map[1][ent.dupid] = ent;
        dup_sync->update_duplication_map(dup_map);
        ASSERT_EQ(find_dup(stub->find_replica(1, 0), 2)->_status, duplication_status::DS_PAUSE);

        ent.status = duplication_status::DS_REMOVED;
        dup_map[1][ent.dupid] = ent;
        dup_sync->update_duplication_map(dup_map);
        ASSERT_EQ(find_dup(stub->find_replica(1, 0), 2)->_status, duplication_status::DS_PAUSE);
    }

protected:
    std::unique_ptr<duplication_sync_timer> dup_sync;
};

INSTANTIATE_TEST_SUITE_P(, duplication_sync_timer_test, ::testing::Values(false, true));

TEST_P(duplication_sync_timer_test, duplication_sync) { test_duplication_sync(); }

TEST_P(duplication_sync_timer_test, update_duplication_map) { test_update_duplication_map(); }

TEST_P(duplication_sync_timer_test, update_on_non_primary) { test_update_on_non_primary(); }

TEST_P(duplication_sync_timer_test, update_confirmed_points) { test_update_confirmed_points(); }

TEST_P(duplication_sync_timer_test, on_duplication_sync_reply) { test_on_duplication_sync_reply(); }

TEST_P(duplication_sync_timer_test, replica_status_transition) { test_replica_status_transition(); }

TEST_P(duplication_sync_timer_test, receive_illegal_duplication_status)
{
    test_receive_illegal_duplication_status();
}

} // namespace replication
} // namespace dsn
