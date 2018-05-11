// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>
#include <memory>
#include <boost/lexical_cast.hpp>

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>

#include <pegasus/client.h>
#include <gtest/gtest.h>

#include "integration_test_base.h"

using namespace dsn::replication;

int32_t integration_test_base::test_app_id = 0;
int32_t integration_test_base::meta_count = 2;
int32_t integration_test_base::replica_count = 3;
const std::string integration_test_base::pegasus_root = "/home/mi/dev/my_pegasus";
const std::string integration_test_base::test_app_name = "integration_test";
const std::string integration_test_base::key_prefix = "hello_key";
const std::string integration_test_base::value_prefix = "world_key";

void integration_test_base::SetUp() {
    clear_onebox();
    start_onebox(meta_count, replica_count);

    init_ddl_client();
    wait_util_app_full_health("temp", 30);
    test_app_id = create_table(test_app_name);
    init_pg_client(test_app_name);
}

void integration_test_base::TearDown() {
    //clear_onebox();
}

void integration_test_base::clear_onebox() {
    chdir(pegasus_root.c_str());
    system("./run.sh clear_onebox");
}

void integration_test_base::start_onebox(int meta_count,
                                         int replica_count) {
    chdir(pegasus_root.c_str());
    std::stringstream ss;
    ss << "./run.sh start_onebox -m " << meta_count << " -r " << replica_count;
    system(ss.str().c_str());
}

void integration_test_base::init_ddl_client() {
    std::vector<dsn::rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list,
                                      "uri-resolver.dsn://mycluster",
                                      "arguments");
    ddl_client = std::make_shared<replication_ddl_client>(meta_list);
}

void integration_test_base::init_pg_client(const std::string &table_name) {
    pg_client = pegasus::pegasus_client_factory::get_client("mycluster",
                                                            table_name.c_str());
}

int32_t integration_test_base::create_table(const std::string &table_name) {
    int ret = ddl_client->create_app(table_name,
                                     "pegasus",
                                     partition_count,
                                     3,
                                     {},
                                     false);
    assert(dsn::ERR_OK == ret);

    int32_t ret_app_id;
    int32_t ret_partition_count;
    std::vector<dsn::partition_configuration> ret_partitions;
    ret = ddl_client->list_app(table_name,
                               ret_app_id,
                               ret_partition_count,
                               ret_partitions);
    assert(dsn::ERR_OK == ret);
    assert(ret_partition_count == partition_count);

    return ret_app_id;
}

void integration_test_base::write_data(int count) {
    for (int i = 0; i < count; ++i) {
        const std::vector<std::string>& key_value = gen_key_value(i);

        pegasus::pegasus_client::internal_info info;
        int err = pg_client->set(key_value[0], key_value[1], key_value[2], 5000, 0, &info);
        ASSERT_EQ(dsn::ERR_OK, err);
        ASSERT_TRUE(info.partition_index < partition_count);
    }
}

void integration_test_base::verify_data(int count) {
    for (int i = 0; i < count; ++i) {
        const std::vector<std::string>& key_value = gen_key_value(i);

        std::string value;
        int err = pg_client->get(key_value[0], key_value[1], value);
        ASSERT_EQ(dsn::ERR_OK, err);
        ASSERT_EQ(key_value[2], value);
    }
}

void integration_test_base::restart_all_meta() {
    for (int i = 1; i <= meta_count; ++i) {
        operate_on_server("meta", "restart", i);
    }
}

void integration_test_base::restart_all_replica() {
    for (int i = 1; i <= replica_count; ++i) {
        operate_on_server("replica", "restart", i);
    }
}

void integration_test_base::wait_util_app_full_health(const std::string &table_name,
                                                      int max_wait_seconds) {
    bool health = true;
    do
    {
        health = true;
        int32_t ret_app_id;
        int32_t ret_partition_count;
        std::vector <dsn::partition_configuration> ret_partitions;
        int ret = ddl_client->list_app(table_name,
                                       ret_app_id,
                                       ret_partition_count,
                                       ret_partitions);
        if (ret != dsn::ERR_OK) {
            std::cout << "ret: " << ret << std::endl;
            health = false;
            sleep(1);
            continue;
        }

        int total_fully_healthy_partition = 0;
        for (int i = 0; i < ret_partitions.size(); i++) {
            const dsn::partition_configuration &pc = ret_partitions[i];
            int rc = (!pc.primary.is_invalid() ? 1 : 0) +
                     pc.secondaries.size();
            if (rc != replica_count) {
                health = false;
                sleep(1);
                break;
            }
        }
    } while(--max_wait_seconds > 0 && !health);
    ASSERT_TRUE(health);
}

std::vector<std::string> integration_test_base::gen_key_value(int index) {
    std::string hash_key = key_prefix + boost::lexical_cast<std::string>(index);
    std::string sort_key = hash_key;
    std::string value = value_prefix + boost::lexical_cast<std::string>(index);

    return std::vector<std::string>({hash_key, sort_key, value});
}

void integration_test_base::operate_on_server(const std::string& server,
                                              const std::string& operate,
                                              int index) {
    ASSERT_TRUE(server == "meta" || server == "replica") << server;
    ASSERT_TRUE(operate == "stop" || operate == "start" || operate == "restart") << operate;
    ASSERT_GT(index, 0);
    if (server == "meta") {
        ASSERT_LE(index, meta_count);
    } else {
        ASSERT_LE(index, replica_count);
    }
    chdir(pegasus_root.c_str());
    std::stringstream cmd;
    cmd << "./run.sh " << operate << "_onebox_instance -" << server[0] << " " << index;
    system(cmd.str().c_str());
}

