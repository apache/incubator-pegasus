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

#include <gtest/gtest.h>
#include <dsn/utility/fail_point.h>

#include "replica/test/replica_disk_test_base.h"

namespace dsn {
namespace replication {

using query_disk_info_rpc = rpc_holder<query_disk_info_request, query_disk_info_response>;

class replica_disk_test : public replica_disk_test_base
{
public:
    query_disk_info_rpc fake_query_disk_rpc;

public:
    void SetUp() override { generate_fake_rpc(); }

private:
    void generate_fake_rpc()
    {
        // create RPC_QUERY_DISK_INFO fake request
        auto query_request = dsn::make_unique<query_disk_info_request>();
        fake_query_disk_rpc = query_disk_info_rpc(std::move(query_request), RPC_QUERY_DISK_INFO);
    }
};

TEST_F(replica_disk_test, on_query_disk_info_all_app)
{
    stub->on_query_disk_info(fake_query_disk_rpc);

    query_disk_info_response &disk_info_response = fake_query_disk_rpc.response();
    // test response disk_info
    ASSERT_EQ(disk_info_response.total_capacity_mb, 2500);
    ASSERT_EQ(disk_info_response.total_available_mb, 750);

    auto &disk_infos = disk_info_response.disk_infos;
    ASSERT_EQ(disk_infos.size(), 6);

    size_t info_size = disk_infos.size();
    for (int i = 0; i < info_size; i++) {
        if (disk_infos[i].tag == "tag_empty_1") {
            continue;
        }
        ASSERT_EQ(disk_infos[i].tag, "tag_" + std::to_string(info_size - i));
        ASSERT_EQ(disk_infos[i].full_dir, "full_dir_" + std::to_string(info_size - i));
        ASSERT_EQ(disk_infos[i].disk_capacity_mb, 500);
        ASSERT_EQ(disk_infos[i].disk_available_mb, (info_size - i) * 50);
        // `holding_primary_replicas` and `holding_secondary_replicas` is std::map<app_id,
        // std::set<::dsn::gpid>>
        ASSERT_EQ(disk_infos[i].holding_primary_replicas.size(), 2);
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas.size(), 2);

        // test the gpid of app_id_1
        // test primary
        int app_id_1_partition_index = 0;
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_1.app_id].size(),
                  app_id_1_primary_count_for_disk);
        for (auto it = disk_infos[i].holding_primary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_1.app_id].size(),
                  app_id_1_secondary_count_for_disk);
        for (auto it = disk_infos[i].holding_secondary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_secondary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }

        // test the gpid of app_id_2
        // test primary
        int app_id_2_partition_index = 0;
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_2.app_id].size(),
                  app_id_2_primary_count_for_disk);
        for (auto it = disk_infos[i].holding_primary_replicas[app_info_2.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_2.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_2.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_2_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_2.app_id].size(),
                  app_id_2_secondary_count_for_disk);
        for (auto it = disk_infos[i].holding_secondary_replicas[app_info_2.app_id].begin();
             it != disk_infos[i].holding_secondary_replicas[app_info_2.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_2.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_2_partition_index++);
        }
    }
}

TEST_F(replica_disk_test, on_query_disk_info_app_not_existed)
{
    query_disk_info_request &request = *fake_query_disk_rpc.mutable_request();

    request.app_name = "not_existed_app";
    stub->on_query_disk_info(fake_query_disk_rpc);
    ASSERT_EQ(fake_query_disk_rpc.response().err, ERR_OBJECT_NOT_FOUND);
}

TEST_F(replica_disk_test, on_query_disk_info_one_app)
{
    query_disk_info_request &request = *fake_query_disk_rpc.mutable_request();

    request.app_name = app_info_1.app_name;
    stub->on_query_disk_info(fake_query_disk_rpc);

    auto &disk_infos_with_app_1 = fake_query_disk_rpc.response().disk_infos;
    size_t info_size = disk_infos_with_app_1.size();
    for (int i = 0; i < info_size; i++) {
        if (disk_infos_with_app_1[i].tag == "tag_empty_1") {
            continue;
        }
        // `holding_primary_replicas` and `holding_secondary_replicas` is std::map<app_id,
        // std::set<::dsn::gpid>>
        ASSERT_EQ(disk_infos_with_app_1[i].holding_primary_replicas.size(), 1);
        ASSERT_EQ(disk_infos_with_app_1[i].holding_secondary_replicas.size(), 1);
        ASSERT_EQ(disk_infos_with_app_1[i].holding_primary_replicas[app_info_1.app_id].size(),
                  app_id_1_primary_count_for_disk);
        ASSERT_EQ(disk_infos_with_app_1[i].holding_secondary_replicas[app_info_1.app_id].size(),
                  app_id_1_secondary_count_for_disk);
        ASSERT_TRUE(disk_infos_with_app_1[i].holding_primary_replicas.find(app_info_2.app_id) ==
                    disk_infos_with_app_1[i].holding_primary_replicas.end());
        ASSERT_TRUE(disk_infos_with_app_1[i].holding_secondary_replicas.find(app_info_2.app_id) ==
                    disk_infos_with_app_1[i].holding_secondary_replicas.end());
    }
}

} // namespace replication
} // namespace dsn
