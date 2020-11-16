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

#include "replica/test/replica_test_base.h"

namespace dsn {
namespace replication {

class replica_disk_test_base : public replica_test_base
{
public:
    // create `dir_nodes_count`(tag_1~tag_5) mock disk:
    // capacity info
    // node_disk     disk_capacity  disk_available_mb  disk_available_ratio
    //  tag_1            100*5             50*1              10%
    //  tag_2            100*5             50*2              20%
    //  tag_3            100*5             50*3              30%
    //  tag_4            100*5             50*4              40%
    //  tag_5            100*5             50*5              50%
    //  total            2500              750               30%
    // replica info, for example, tag_1(other disk same with it)
    // primary         secondary
    //   1.0            1.1,1.2
    // 2.0,2.1       2.2,2.3,2.4,2.5
    replica_disk_test_base()
    {
        generate_mock_app_info();

        generate_mock_empty_dir_node(empty_dir_nodes_count);
        generate_mock_dir_nodes(dir_nodes_count);

        stub->generate_replicas_base_dir_nodes_for_app(
            app_info_1, app_id_1_primary_count_for_disk, app_id_1_secondary_count_for_disk);

        stub->generate_replicas_base_dir_nodes_for_app(
            app_info_2, app_id_2_primary_count_for_disk, app_id_2_secondary_count_for_disk);
        stub->on_disk_stat();
    }

public:
    int empty_dir_nodes_count = 1;
    int dir_nodes_count = 5;

    dsn::app_info app_info_1;
    int app_id_1_primary_count_for_disk = 1;
    int app_id_1_secondary_count_for_disk = 2;

    dsn::app_info app_info_2;
    int app_id_2_primary_count_for_disk = 2;
    int app_id_2_secondary_count_for_disk = 4;

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

    void generate_mock_empty_dir_node(int num)
    {
        while (num > 0) {
            dir_node *node_disk = new dir_node(fmt::format("tag_empty_{}", num),
                                               fmt::format("full_dir_empty_{}", num));
            stub->_fs_manager._dir_nodes.emplace_back(node_disk);
            num--;
        }
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

} // namespace replication
} // namespace dsn
