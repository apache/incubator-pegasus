// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"

namespace dsn {
namespace replication {

class replica_disk_test : public replica_test_base
{
public:
    int dir_nodes_count = 5;

    dsn::app_info app_info_1;
    int app_id_1_primary_count_for_disk = 1;
    int app_id_1_secondary_count_for_disk = 2;

    dsn::app_info app_info_2;
    int app_id_2_primary_count_for_disk = 2;
    int app_id_2_secondary_count_for_disk = 4;

public:
    void SetUp() override
    {
        generate_mock_app_info();
        generate_mock_dir_nodes(dir_nodes_count);
        stub->generate_replicas_base_dir_nodes_for_app(
            app_info_1, app_id_1_primary_count_for_disk, app_id_1_secondary_count_for_disk);

        stub->generate_replicas_base_dir_nodes_for_app(
            app_info_2, app_id_2_primary_count_for_disk, app_id_2_secondary_count_for_disk);
        stub->on_disk_stat();
    }

    std::vector<std::shared_ptr<dir_node>> get_fs_manager_nodes()
    {
        return stub->_fs_manager._dir_nodes;
    }

private:
    void generate_mock_app_info()
    {
        app_info_1.app_id = 1;
        app_info_1.app_name = "disk_test_1";
        app_info_1.app_type = "replica";
        app_info_1.is_stateful = true;
        app_info_1.max_replica_count = 3;
        app_info_1.partition_count = 8;

        app_info_2.app_id = 2;
        app_info_2.app_name = "disk_test_2";
        app_info_2.app_type = "replica";
        app_info_2.is_stateful = true;
        app_info_2.max_replica_count = 3;
        app_info_2.partition_count = 16;
    }

    void generate_mock_dir_nodes(int num)
    {
        int64_t disk_capacity_mb = num * 100;
        while (num > 0) {
            int64_t disk_available_mb = num * 50;
            int disk_available_ratio =
                static_cast<int>(std::round((double)100 * disk_available_mb / disk_capacity_mb));
            // create one mock dir_node and make sure disk_capacity_mb_ > disk_available_mb_
            dir_node *node_disk = new dir_node("tag_" + std::to_string(num),
                                               "full_dir_" + std::to_string(num),
                                               disk_capacity_mb,
                                               disk_available_mb,
                                               disk_available_ratio);

            int app_id_1_disk_holding_replica_count =
                app_id_1_primary_count_for_disk + app_id_1_secondary_count_for_disk;
            while (app_id_1_disk_holding_replica_count-- > 0) {
                node_disk->holding_replicas[app_info_1.app_id].emplace(
                    gpid(app_info_1.app_id, app_id_1_disk_holding_replica_count));
            }

            int app_id_2_disk_holding_replica_count =
                app_id_2_primary_count_for_disk + app_id_2_secondary_count_for_disk;
            while (app_id_2_disk_holding_replica_count-- > 0) {
                node_disk->holding_replicas[app_info_2.app_id].emplace(
                    gpid(app_info_2.app_id, app_id_2_disk_holding_replica_count));
            }

            stub->_fs_manager._dir_nodes.emplace_back(node_disk);
            num--;
        }
    }
};

TEST_F(replica_disk_test, on_query_disk_info_all_app)
{
    // disk_info_request.app_id default value = 0 means test query all apps' replica_count
    // create fake request
    dsn::message_ptr fake_request = dsn::message_ex::create_request(RPC_QUERY_DISK_INFO);
    query_disk_info_request request;
    ::dsn::marshall(fake_request, request);

    // get received request and query disk info
    dsn::message_ex *recvd_request = fake_request->copy(true, true);
    auto rpc =
        rpc_holder<query_disk_info_request, query_disk_info_response>::auto_reply(recvd_request);
    stub->on_query_disk_info(rpc);

    query_disk_info_response &disk_info_response = rpc.response();
    // test response disk_info
    ASSERT_EQ(disk_info_response.total_capacity_mb, 2500);
    ASSERT_EQ(disk_info_response.total_available_mb, 750);

    auto &disk_infos = disk_info_response.disk_infos;
    ASSERT_EQ(disk_infos.size(), 5);

    int info_size = disk_infos.size();
    for (int i = 0; i < info_size; i++) {
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
        int app_id_2_partition_index = 0;
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
    // test app_id not existed
    // create fake request
    dsn::message_ptr fake_request = dsn::message_ex::create_request(RPC_QUERY_DISK_INFO);
    query_disk_info_request tmp_request;
    ::dsn::marshall(fake_request, tmp_request);

    // get received request and query disk info
    dsn::message_ex *recvd_request = fake_request->copy(true, true);
    auto rpc =
        rpc_holder<query_disk_info_request, query_disk_info_response>::auto_reply(recvd_request);
    query_disk_info_request &request = const_cast<query_disk_info_request &>(rpc.request());
    request.app_name = "not_existed_app";
    stub->on_query_disk_info(rpc);

    ASSERT_EQ(rpc.response().err, ERR_OBJECT_NOT_FOUND);
}

TEST_F(replica_disk_test, on_query_disk_info_one_app)
{
    // test app_name = "disk_test_1"
    // create fake request
    dsn::message_ptr fake_request = dsn::message_ex::create_request(RPC_QUERY_DISK_INFO);
    query_disk_info_request tmp_request;
    ::dsn::marshall(fake_request, tmp_request);

    // get received request and query disk info
    dsn::message_ex *recvd_request = fake_request->copy(true, true);
    auto rpc =
        rpc_holder<query_disk_info_request, query_disk_info_response>::auto_reply(recvd_request);
    query_disk_info_request &request = const_cast<query_disk_info_request &>(rpc.request());
    request.app_name = app_info_1.app_name;
    stub->on_query_disk_info(rpc);

    auto &disk_infos_with_app_1 = rpc.response().disk_infos;
    int info_size = disk_infos_with_app_1.size();
    for (int i = 0; i < info_size; i++) {
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
