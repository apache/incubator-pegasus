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
#include <unistd.h>

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include "base/pegasus_const.h"
#include "global_env.h"
#include "utils.h"

using namespace dsn::replication;
using namespace pegasus;

static const std::string table_for_lost_log = "table_for_lost_log";

static void truncate_recent_file(const std::string &path)
{
    char command[512];
    snprintf(command, 512, "ls -lcrt %s | tail -n 1 | awk \'{print $5,$9}\'", path.c_str());
    std::cout << command << std::endl;
    std::stringstream ss;
    assert(dsn::utils::pipe_execute(command, ss) == 0);
    size_t file_length;
    std::string file_name;
    ss >> file_length >> file_name;

    std::cout << "truncate file with size: (" << file_name << ", " << file_length << ")"
              << std::endl;

    snprintf(
        command, 512, "truncate -s %lu %s/%s", file_length / 3, path.c_str(), file_name.c_str());
    std::cout << command << std::endl;
    system(command);

    snprintf(command, 512, "ls -l %s/%s | awk '{print $5}'", path.c_str(), file_name.c_str());
    std::stringstream ss2;
    assert(dsn::utils::pipe_execute(command, ss2) == 0);
    size_t new_file_length;
    ss2 >> new_file_length;

    ASSERT_LT(new_file_length, file_length);
    std::cout << "after truncated file size: " << new_file_length << std::endl;
}

TEST(lost_log, slog)
{
    const std::string key_prefix = "lost_log";
    const std::string value_prefix = "slog";
    const int number = 10000;
    const int partition_count = 4;

    std::vector<dsn::rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");
    std::shared_ptr<replication_ddl_client> ddl_client(new replication_ddl_client(meta_list));

    // first create table
    std::cerr << "create app " << table_for_lost_log << std::endl;
    dsn::error_code error =
        ddl_client->create_app(table_for_lost_log, "pegasus", partition_count, 3, {}, false);
    ASSERT_EQ(dsn::ERR_OK, error);

    pegasus::pegasus_client *pg_client =
        pegasus::pegasus_client_factory::get_client("mycluster", table_for_lost_log.c_str());

    // write some keys
    for (int i = 0; i < number; ++i) {
        std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
        std::string sort_key = hash_key;
        std::string value = value_prefix + boost::lexical_cast<std::string>(i);

        pegasus::pegasus_client::internal_info info;
        int ans;
        RETRY_OPERATION(pg_client->set(hash_key, sort_key, value, 5000, 0, &info), ans);
        ASSERT_EQ(0, ans);
        ASSERT_TRUE(info.partition_index < partition_count);
    }

    chdir(global_env::instance()._pegasus_root.c_str());

    std::cout << "first stop the cluster" << std::endl;
    system("./run.sh stop_onebox");

    std::cout << "truncate slog for replica1" << std::endl;
    truncate_recent_file("onebox/replica1/data/replica/slog");

    std::cout << "restart onebox again" << std::endl;
    system("./run.sh start_onebox");
    chdir(global_env::instance()._working_dir.c_str());

    ddl_client->wait_app_ready(table_for_lost_log, partition_count, 3);

    std::cout << "check keys wrote before" << std::endl;
    for (int i = 0; i < number; ++i) {
        std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
        std::string sort_key = hash_key;
        std::string expect_value = value_prefix + boost::lexical_cast<std::string>(i);
        std::string got_value;

        pegasus::pegasus_client::internal_info info;
        int ans;
        RETRY_OPERATION(pg_client->get(hash_key, sort_key, got_value, 5000, &info), ans);

        ASSERT_EQ(0, ans);
        ASSERT_TRUE(info.partition_index < partition_count);
        ASSERT_EQ(expect_value, got_value);
    }
}
