// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>
#include <boost/lexical_cast.hpp>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>

#include <dsn/dist/replication/replication_ddl_client.h>

#include "base/pegasus_const.h"
#include "utils.h"

using namespace dsn::replication;
using namespace pegasus;

TEST(drop_and_recall, simple)
{
    const std::string simple_table = "simple_table";
    const std::string key_prefix = "hello";
    const std::string value_prefix = "world";
    const int number = 10000;
    const int partition_count = 4;

    std::vector<dsn::rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");
    replication_ddl_client *ddl_client = new replication_ddl_client(meta_list);

    // first create table
    std::cerr << "create app " << simple_table << std::endl;
    dsn::error_code error =
        ddl_client->create_app(simple_table, "pegasus", partition_count, 3, {}, false);
    ASSERT_EQ(dsn::ERR_OK, error);

    // list table
    std::vector<::dsn::app_info> apps;
    error = ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
    ASSERT_EQ(dsn::ERR_OK, error);
    int app_id = 0;
    for (auto &app : apps) {
        if (app.app_name == simple_table) {
            app_id = app.app_id;
            break;
        }
    }
    ASSERT_NE(0, app_id);
    std::cerr << "app_id = " << app_id << std::endl;

    pegasus::pegasus_client *pg_client =
        pegasus::pegasus_client_factory::get_client("mycluster", simple_table.c_str());

    std::vector<std::string> hash_for_gpid(partition_count, "");

    // then write keys
    std::cerr << "write " << number << " keys" << std::endl;
    for (int i = 0; i < number; ++i) {
        std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
        std::string sort_key = hash_key;
        std::string value = value_prefix + boost::lexical_cast<std::string>(i);

        pegasus::pegasus_client::internal_info info;
        int ans;
        RETRY_OPERATION(pg_client->set(hash_key, sort_key, value, 5000, 0, &info), ans);
        ASSERT_EQ(0, ans);
        ASSERT_TRUE(info.partition_index < partition_count);
        if (hash_for_gpid[info.partition_index] == "") {
            hash_for_gpid[info.partition_index] = hash_key;
        }
    }

    for (const auto &key : hash_for_gpid) {
        ASSERT_TRUE(key != "");
    }

    // then drop the table
    std::cerr << "drop table " << simple_table << std::endl;
    error = ddl_client->drop_app(simple_table, 0);
    ASSERT_EQ(0, error);

    // wait for all elements to be dropped
    for (int i = 0; i < partition_count; ++i) {
        int j;
        for (j = 0; j < 60; ++j) {
            pegasus::pegasus_client::internal_info info;
            pg_client->set(hash_for_gpid[i], "", "", 1000, 0, &info);
            if (info.app_id == -1) {
                std::cerr << "partition " << i << " is removed from server" << std::endl;
                break;
            } else {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        ASSERT_TRUE(j < 60);
    }

    // then recall table
    std::cerr << "start to recall table " << std::endl;
    error = ddl_client->recall_app(app_id, "");
    ASSERT_EQ(dsn::ERR_OK, error);

    // then read all keys
    for (int i = 0; i < number; ++i) {
        std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
        std::string sort_key = hash_key;
        std::string exp_value = value_prefix + boost::lexical_cast<std::string>(i);

        std::string act_value;
        int ans;
        RETRY_OPERATION(pg_client->get(hash_key, sort_key, act_value), ans);
        ASSERT_EQ(0, ans);
        ASSERT_EQ(exp_value, act_value);
    }
}
