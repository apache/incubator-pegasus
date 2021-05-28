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

#include "replica_disk_test_base.h"
#include "replica/disk_cleaner.h"

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

    int info_size = disk_infos.size();
    int app_id_1_partition_index = 1;
    int app_id_2_partition_index = 1;
    for (int i = 0; i < info_size; i++) {
        if (disk_infos[i].tag == "tag_empty_1") {
            continue;
        }
        ASSERT_EQ(disk_infos[i].tag, "tag_" + std::to_string(i + 1));
        ASSERT_EQ(disk_infos[i].full_dir, "./tag_" + std::to_string(i + 1));
        ASSERT_EQ(disk_infos[i].disk_capacity_mb, 500);
        ASSERT_EQ(disk_infos[i].disk_available_mb, (i + 1) * 50);
        // `holding_primary_replicas` and `holding_secondary_replicas` is std::map<app_id,
        // std::set<::dsn::gpid>>
        ASSERT_EQ(disk_infos[i].holding_primary_replicas.size(), 2);
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas.size(), 2);

        // test the gpid of app_id_1
        // test primary
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_1.app_id].size(),
                  app_id_1_primary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_primary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_1.app_id].size(),
                  app_id_1_secondary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_secondary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_secondary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }

        // test the gpid of app_id_2
        // test primary
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_2.app_id].size(),
                  app_id_2_primary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_primary_replicas[app_info_2.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_2.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_2.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_2_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_2.app_id].size(),
                  app_id_2_secondary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_secondary_replicas[app_info_2.app_id].begin();
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
    int info_size = disk_infos_with_app_1.size();
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

TEST_F(replica_disk_test, gc_disk_useless_dir)
{
    FLAGS_gc_disk_error_replica_interval_seconds = 1;
    FLAGS_gc_disk_garbage_replica_interval_seconds = 1;
    FLAGS_gc_disk_migration_origin_replica_interval_seconds = 1;
    FLAGS_gc_disk_migration_tmp_replica_interval_seconds = 1;

    std::vector<std::string> tests{
        "./replica1.err",
        "./replica2.err",
        "./replica.gar",
        "./replica.tmp",
        "./replica.ori",
        "./replica.bak",
        "./replica.1.1",
    };

    for (const auto &test : tests) {
        utils::filesystem::create_directory(test);
        ASSERT_TRUE(utils::filesystem::directory_exists(test));
    }

    sleep(5);

    std::vector<std::string> data_dirs{"./"};
    disk_cleaning_report report{};
    dsn::replication::disk_remove_useless_dirs(data_dirs, report);

    for (const auto &test : tests) {
        if (!dsn::replication::is_data_dir_removable(test)) {
            ASSERT_TRUE(utils::filesystem::directory_exists(test));
            continue;
        }
        ASSERT_FALSE(utils::filesystem::directory_exists(test));
    }

    ASSERT_EQ(report.remove_dir_count, 5);
    ASSERT_EQ(report.disk_migrate_origin_count, 1);
    ASSERT_EQ(report.disk_migrate_tmp_count, 1);
    ASSERT_EQ(report.garbage_replica_count, 1);
    ASSERT_EQ(report.error_replica_count, 2);
}

TEST_F(replica_disk_test, disk_status_test)
{
    int32_t node_index = 0;
    struct disk_status_test
    {
        disk_status::type old_status;
        disk_status::type new_status;
    } tests[]{{disk_status::NORMAL, disk_status::NORMAL},
              {disk_status::NORMAL, disk_status::SPACE_INSUFFICIENT},
              {disk_status::SPACE_INSUFFICIENT, disk_status::SPACE_INSUFFICIENT},
              {disk_status::SPACE_INSUFFICIENT, disk_status::NORMAL}};
    for (const auto &test : tests) {
        auto node = get_dir_nodes()[node_index];
        mock_node_status(node_index, test.old_status, test.new_status);
        update_disks_status();
        for (auto &kv : node->holding_replicas) {
            for (auto &pid : kv.second) {
                bool flag;
                ASSERT_EQ(replica_disk_space_insufficient(pid, flag), ERR_OK);
                ASSERT_EQ(flag, test.new_status == disk_status::SPACE_INSUFFICIENT);
            }
        }
    }
    mock_node_status(node_index, disk_status::NORMAL, disk_status::NORMAL);
}

} // namespace replication
} // namespace dsn
