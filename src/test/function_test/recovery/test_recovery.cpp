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

#include <fmt/core.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client/replication_ddl_client.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "rpc/rpc_host_port.h"
#include "test/function_test/utils/test_util.h"
#include "utils/error_code.h"
#include "utils/rand.h"
#include "utils/test_macros.h"

namespace pegasus {

// TODO(yingchun): add a check for it, get config by curl
// NOTE: THREAD_POOL_META_SERVER worker count should be greater than 1
// This function test update FLAGS_distributed_lock_service_type to
// 'distributed_lock_service_simple', which executes in threadpool THREAD_POOL_META_SERVER
// As a result, failure detection lock executes in this pool
// if worker count = 1, it will lead to ERR_TIMEOUT when execute 'ddl_client_->do_recovery'
class recovery_test : public test_util
{
protected:
    void SetUp() override
    {
        TRICKY_CODE_TO_AVOID_LINK_ERROR;
        SET_UP_BASE(test_util);
        for (int i = 0; i < dataset_count; ++i) {
            const std::string hash_key(fmt::format("{}{}", key_prefix, i));
            const std::string value(fmt::format("{}{}", value_prefix, i));

            pegasus_client::internal_info info;

            // Use hash key as the sort key.
            ASSERT_EQ(0, client_->set(hash_key, hash_key, value, 5000, 0, &info));
            ASSERT_GT(partition_count_, info.partition_index);
        }
    }

    // The cluster name "single_master_cluster" (see src/test/function_test/config.ini) means the
    // cluster has only one meta server, while "onebox" means the cluster has 3 meta servers.
    recovery_test() : test_util(std::map<std::string, std::string>(), "single_master_cluster") {}

    std::vector<dsn::host_port> get_rpc_host_port_list(const std::vector<int> ports)
    {
        std::vector<dsn::host_port> result;
        result.reserve(ports.size());
        for (const auto &port : ports) {
            char hostname[1024];
            gethostname(hostname, 1024);
            dsn::host_port hp(hostname, port);
            result.push_back(hp);
        }
        return result;
    }

    void stop_replica(int id)
    {
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("./run.sh stop_onebox_instance -r " + std::to_string(id)));
    }

    void stop_meta(int id)
    {
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("./run.sh stop_onebox_instance -m " + std::to_string(id)));
    }

    void start_meta(int id)
    {
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("./run.sh start_onebox_instance -m " + std::to_string(id)));
    }

    void start_replica(int id)
    {
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("./run.sh start_onebox_instance -r " + std::to_string(id)));
    }

    void clear_remote_storage()
    {
        ASSERT_NO_FATAL_FAILURE(
            run_cmd_from_project_root("rm -rf onebox/meta1/data/meta/meta_state_service.log"));
    }

    void config_meta_to_do_cold_recovery()
    {
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
            "sed -i \"/^\\s*recover_from_replica_server/c recover_from_replica_server = true\" "
            "onebox/meta1/config.ini"));
    }

    void delete_replica(int replica_id, int app_id, int partition_id)
    {
        std::string cmd = fmt::format("rm -rf onebox/replica{}/data/replica/reps/{}.{}.pegasus",
                                      replica_id,
                                      app_id,
                                      partition_id);
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(cmd));
    }

    void delete_replicas_for_app_id(int replica_id, int app_id)
    {
        std::string cmd = fmt::format(
            "rm -rf onebox/replica{}/data/replica/reps/{}.*.pegasus", replica_id, app_id);
        ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(cmd));
    }

    // 1. stop replicas
    // 2. clear the remote storage and set meta to recover mode
    void prepare_recovery()
    {
        // then stop all jobs
        ASSERT_NO_FATAL_FAILURE(stop_meta(1));
        for (int i = 1; i <= 3; ++i) {
            ASSERT_NO_FATAL_FAILURE(stop_replica(i));
        }

        ASSERT_NO_FATAL_FAILURE(clear_remote_storage());
        ASSERT_NO_FATAL_FAILURE(config_meta_to_do_cold_recovery());
        // sleep some time, in case that the socket is time-wait
        std::cout << "sleep for a while to wait the socket to destroy" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    void verify_data(int count)
    {
        // then check to read all keys
        for (int i = 0; i < count; ++i) {
            const std::string hash_key(fmt::format("{}{}", key_prefix, i));
            const std::string expected_value(fmt::format("{}{}", value_prefix, i));

            std::string actual_value;

            // Use hash key as the sort key.
            ASSERT_EQ(PERR_OK, client_->get(hash_key, hash_key, actual_value));
            ASSERT_EQ(expected_value, actual_value);
        }
    }

    const std::string key_prefix = "hello_key";
    const std::string value_prefix = "world_key";
    static const int dataset_count = 2048;
};

TEST_F(recovery_test, recovery)
{
    // first test the basic recovery
    {
        ASSERT_NO_FATAL_FAILURE(prepare_recovery());
        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            ASSERT_NO_FATAL_FAILURE(start_replica(i));
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        ASSERT_NO_FATAL_FAILURE(start_meta(1));

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_host_port_list({34801, 34802, 34803});
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->do_recovery(nodes, 30, false, false, std::string()));

        // send another recovery command
        ASSERT_EQ(dsn::ERR_SERVICE_ALREADY_RUNNING,
                  ddl_client_->do_recovery(nodes, 30, false, false, std::string()));

        // then wait the apps to ready
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(table_name_, "pegasus", partition_count_, 3, {}));

        ASSERT_NO_FATAL_FAILURE(verify_data(dataset_count));
    }

    // recover from subset of all nodes
    std::cout << ">>>>> test recovery from subset of all nodes <<<<<" << std::endl;
    {
        ASSERT_NO_FATAL_FAILURE(prepare_recovery());
        for (int i = 1; i <= 3; ++i) {
            ASSERT_NO_FATAL_FAILURE(start_replica(i));
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        ASSERT_NO_FATAL_FAILURE(start_meta(1));

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // recovery only from 1 & 2
        auto nodes = get_rpc_host_port_list({34801, 34802});
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->do_recovery(nodes, 30, false, false, std::string()));

        // then wait the app to ready
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(table_name_, "pegasus", partition_count_, 3, {}));

        ASSERT_NO_FATAL_FAILURE(verify_data(dataset_count));
    }

    // recovery from whole, but some partitions has been removed
    std::cout << ">>>>> test recovery, some partitions have been lost <<<<<" << std::endl;
    {
        ASSERT_NO_FATAL_FAILURE(prepare_recovery());
        for (int i = 0; i < partition_count_; ++i) {
            int replica_id = dsn::rand::next_u32(1, 3);
            delete_replica(replica_id, 2, i);
        }

        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            ASSERT_NO_FATAL_FAILURE(start_replica(i));
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        ASSERT_NO_FATAL_FAILURE(start_meta(1));

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_host_port_list({34801, 34802, 34803});
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->do_recovery(nodes, 30, false, false, std::string()));

        ASSERT_NO_FATAL_FAILURE(wait_table_healthy(table_name_));

        // then wait the apps to ready
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(table_name_, "pegasus", partition_count_, 3, {}));

        ASSERT_NO_FATAL_FAILURE(verify_data(dataset_count));
    }

    // some apps has been totally removed
    std::cout << ">>>>> test recovery, app 1 is removed <<<<<" << std::endl;
    {
        ASSERT_NO_FATAL_FAILURE(prepare_recovery());
        for (int i = 1; i < 4; ++i) {
            delete_replicas_for_app_id(i, 1);
        }

        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            ASSERT_NO_FATAL_FAILURE(start_replica(i));
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        ASSERT_NO_FATAL_FAILURE(start_meta(1));

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_host_port_list({34801, 34802, 34803});
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->do_recovery(nodes, 30, false, false, std::string()));

        // then wait the apps to ready
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(table_name_, "pegasus", partition_count_, 3, {}));

        ASSERT_NO_FATAL_FAILURE(verify_data(dataset_count));
    }
}

} // namespace pegasus
