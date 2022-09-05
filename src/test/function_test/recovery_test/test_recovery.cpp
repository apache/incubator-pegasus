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

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>
#include <memory>
#include <boost/lexical_cast.hpp>

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/utility/rand.h>

#include "include/pegasus/client.h"
#include <gtest/gtest.h>

#include "base/pegasus_const.h"
#include "test/function_test/utils/global_env.h"

using namespace dsn::replication;
using namespace pegasus;

class recovery_test : public testing::Test
{
protected:
    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        // NOTE: THREAD_POOL_META_SERVER worker count should be greater than 1
        // This function test update 'distributed_lock_service_type' to
        // 'distributed_lock_service_simple', which executes in threadpool THREAD_POOL_META_SERVER
        // As a result, failure detection lock executes in this pool
        // if worker count = 1, it will lead to ERR_TIMEOUT when execute 'ddl_client->do_recovery'

        // 2. initialize the clients
        std::vector<dsn::rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster"));
        ASSERT_FALSE(meta_list.empty());

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(ddl_client != nullptr);
        pg_client = pegasus::pegasus_client_factory::get_client("single_master_cluster",
                                                                table_name.c_str());
        ASSERT_TRUE(pg_client != nullptr);

        // 3. write some data to the app
        dsn::error_code err;
        // first create the app
        err = ddl_client->create_app(table_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        // then write keys
        std::cerr << "write " << set_count << " keys" << std::endl;
        for (int i = 0; i < set_count; ++i) {
            std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
            std::string sort_key = hash_key;
            std::string value = value_prefix + boost::lexical_cast<std::string>(i);

            pegasus::pegasus_client::internal_info info;
            int ans = pg_client->set(hash_key, sort_key, value, 5000, 0, &info);
            ASSERT_EQ(0, ans);
            ASSERT_TRUE(info.partition_index < default_partitions);
        }
    }

public:
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;

public:
    std::vector<dsn::rpc_address> get_rpc_address_list(const std::vector<int> ports)
    {
        std::vector<dsn::rpc_address> result;
        result.reserve(ports.size());
        for (const int &p : ports) {
            dsn::rpc_address address(global_env::instance()._host_ip.c_str(), p);
            result.push_back(address);
        }
        return result;
    }

    void stop_replica(int id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s && ./run.sh stop_onebox_instance -r %d",
                 global_env::instance()._pegasus_root.c_str(),
                 id);
        system(command);
    }

    void stop_meta(int id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s && ./run.sh stop_onebox_instance -m %d",
                 global_env::instance()._pegasus_root.c_str(),
                 id);
        system(command);
    }

    void start_meta(int id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s && ./run.sh start_onebox_instance -m %d",
                 global_env::instance()._pegasus_root.c_str(),
                 id);
        system(command);
    }

    void start_replica(int id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s && ./run.sh start_onebox_instance -r %d",
                 global_env::instance()._pegasus_root.c_str(),
                 id);
        system(command);
    }

    void clear_remote_storage()
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s && rm -rf onebox/meta1/data/meta/meta_state_service.log",
                 global_env::instance()._pegasus_root.c_str());
        system(command);
    }

    void config_meta_to_do_cold_recovery()
    {
        char command[512];
        snprintf(
            command,
            512,
            "cd %s && sed -i \"/^\\s*recover_from_replica_server/c recover_from_replica_server = "
            "true\" onebox/meta1/config.ini",
            global_env::instance()._pegasus_root.c_str());
        system(command);
    }

    void delete_replica(int replica_id, int app_id, int partition_id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s/onebox/replica%d/data/replica/reps && rm -rf %d.%d.pegasus",
                 global_env::instance()._pegasus_root.c_str(),
                 replica_id,
                 app_id,
                 partition_id);
        std::cout << command << std::endl;
        system(command);
    }

    void delete_replicas_for_app_id(int replica_id, int app_id)
    {
        char command[512];
        snprintf(command,
                 512,
                 "cd %s/onebox/replica%d/data/replica/reps && rm -rf %d.*.pegasus",
                 global_env::instance()._pegasus_root.c_str(),
                 replica_id,
                 app_id);
        std::cout << command << std::endl;
        system(command);
    }

    // 1. stop replicas
    // 2. clear the remote storage and set meta to recover mode
    void prepare_recovery()
    {
        // then stop all jobs
        stop_meta(1);
        for (int i = 1; i <= 3; ++i) {
            stop_replica(i);
        }

        clear_remote_storage();
        config_meta_to_do_cold_recovery();
        // sleep some time, in case that the socket is time-wait
        std::cout << "sleep for a while to wait the socket to destroy" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    void verify_data(int count)
    {
        // then check to read all keys
        for (int i = 0; i < count; ++i) {
            std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
            std::string sort_key = hash_key;
            std::string exp_value = value_prefix + boost::lexical_cast<std::string>(i);

            std::string act_value;
            int ans = pg_client->get(hash_key, sort_key, act_value);
            ASSERT_EQ(0, ans);
            ASSERT_EQ(exp_value, act_value);
        }
    }

    static const std::string table_name;
    static const std::string key_prefix;
    static const std::string value_prefix;
    static const int default_partitions = 4;
    static const int set_count = 2048;
};

const std::string recovery_test::table_name = "test_table";
const std::string recovery_test::key_prefix = "hello_key";
const std::string recovery_test::value_prefix = "world_key";

TEST_F(recovery_test, recovery)
{
    dsn::error_code err;

    // first test the basic recovery
    std::cout << ">>>>> test basic recovery <<<<<" << std::endl;
    {
        prepare_recovery();
        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            start_replica(i);
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        start_meta(1);

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_address_list({34801, 34802, 34803});
        err = ddl_client->do_recovery(nodes, 30, false, false, std::string());
        ASSERT_EQ(dsn::ERR_OK, err);

        // send another recovery command
        err = ddl_client->do_recovery(nodes, 30, false, false, std::string());
        ASSERT_EQ(dsn::ERR_SERVICE_ALREADY_RUNNING, err);

        // then wait the apps to ready
        err = ddl_client->create_app(table_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        verify_data(set_count);
    }

    // recover from subset of all nodes
    std::cout << ">>>>> test recovery from subset of all nodes <<<<<" << std::endl;
    {
        prepare_recovery();
        for (int i = 1; i <= 3; ++i)
            start_replica(i);
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        start_meta(1);

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // recovery only from 1 & 2
        std::vector<dsn::rpc_address> nodes = get_rpc_address_list({34801, 34802});
        ddl_client->do_recovery(nodes, 30, false, false, std::string());
        ASSERT_EQ(dsn::ERR_OK, err);

        // then wait the app to ready
        err = ddl_client->create_app(table_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        verify_data(set_count);
    }

    // recovery from whole, but some partitions has been removed
    std::cout << ">>>>> test recovery, some partitions have been lost <<<<<" << std::endl;
    {
        prepare_recovery();
        for (int i = 0; i < default_partitions; ++i) {
            int replica_id = dsn::rand::next_u32(1, 3);
            delete_replica(replica_id, 2, i);
        }

        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            start_replica(i);
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        start_meta(1);

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_address_list({34801, 34802, 34803});
        err = ddl_client->do_recovery(nodes, 30, false, false, std::string());
        ASSERT_EQ(dsn::ERR_OK, err);

        // then wait the apps to ready
        err = ddl_client->create_app(table_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        verify_data(set_count);
    }

    // some apps has been totally removed
    std::cout << ">>>>> test recovery, app 1 is removed <<<<<" << std::endl;
    {
        prepare_recovery();
        for (int i = 1; i < 4; ++i) {
            delete_replicas_for_app_id(i, 1);
        }

        // start all jobs again
        for (int i = 1; i <= 3; ++i) {
            start_replica(i);
        }
        std::cout << "sleep for a while to wait the replica to start" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(20));
        start_meta(1);

        std::cout << "sleep for a while to wait the meta to come to alive" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // then do recovery
        auto nodes = get_rpc_address_list({34801, 34802, 34803});
        err = ddl_client->do_recovery(nodes, 30, false, false, std::string());
        ASSERT_EQ(dsn::ERR_OK, err);

        // then wait the apps to ready
        err = ddl_client->create_app(table_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        verify_data(set_count);
    }
}
