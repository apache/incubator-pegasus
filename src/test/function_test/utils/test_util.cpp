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

#include "test_util.h"

#include <nlohmann/json.hpp>
#include <unistd.h>
#include <fstream>
#include <initializer_list>
#include <utility>
#include <vector>

#include "base/pegasus_const.h"
#include "client/replication_ddl_client.h"
#include "common/replication_other_types.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "nlohmann/detail/iterators/iter_impl.hpp"
#include "nlohmann/json_fwd.hpp"
#include "runtime/rpc/rpc_address.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/utils.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/rand.h"

using dsn::partition_configuration;
using dsn::replication::replica_helper;
using dsn::replication::replication_ddl_client;
using dsn::rpc_address;
using nlohmann::json;
using std::map;
using std::string;
using std::vector;

namespace pegasus {

test_util::test_util(map<string, string> create_envs)
    : cluster_name_("onebox"), app_name_("temp"), create_envs_(std::move(create_envs))
{
}

test_util::~test_util() {}

void test_util::SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

void test_util::SetUp()
{
    ASSERT_TRUE(replica_helper::load_meta_servers(
        meta_list_, PEGASUS_CLUSTER_SECTION_NAME.c_str(), cluster_name_.c_str()));
    ASSERT_FALSE(meta_list_.empty());

    ddl_client_ = std::make_shared<replication_ddl_client>(meta_list_);
    ASSERT_TRUE(ddl_client_ != nullptr);
    ddl_client_->set_max_wait_app_ready_secs(120);

    dsn::error_code ret =
        ddl_client_->create_app(app_name_, "pegasus", partition_count_, 3, create_envs_, false);
    if (ret == dsn::ERR_INVALID_PARAMETERS) {
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(app_name_, 0));
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(
                      app_name_, "pegasus", partition_count_, 3, create_envs_, false));
    } else {
        ASSERT_EQ(dsn::ERR_OK, ret);
    }
    client_ = pegasus_client_factory::get_client(cluster_name_.c_str(), app_name_.c_str());
    ASSERT_TRUE(client_ != nullptr);

    int32_t partition_count;
    ASSERT_EQ(dsn::ERR_OK, ddl_client_->list_app(app_name_, app_id_, partition_count, partitions_));
    ASSERT_NE(0, app_id_);
    ASSERT_EQ(partition_count_, partition_count);
    ASSERT_EQ(partition_count_, partitions_.size());
}

void test_util::run_cmd_from_project_root(const string &cmd)
{
    ASSERT_EQ(0, ::chdir(global_env::instance()._pegasus_root.c_str()));
    ASSERT_NO_FATAL_FAILURE(run_cmd_no_error(cmd));
}

int test_util::get_alive_replica_server_count()
{
    const auto json_filename = fmt::format("test_json_file.{}", dsn::rand::next_u32());
    auto cleanup =
        dsn::defer([json_filename]() { dsn::utils::filesystem::remove_path(json_filename); });
    run_cmd_from_project_root(fmt::format("echo 'nodes -djo {}' | ./run.sh shell", json_filename));
    std::ifstream f(json_filename);
    const auto data = json::parse(f);
    vector<string> rs_addrs;
    for (const auto &rs : data["details"]) {
        if (rs["status"] == "UNALIVE") {
            continue;
        }
        rs_addrs.push_back(rs["address"]);
    }

    int replica_server_count = 0;
    for (const auto &rs_addr : rs_addrs) {
        int ret = run_cmd(fmt::format("curl {}/version", rs_addr));
        if (ret == 0) {
            replica_server_count++;
        }
    }
    return replica_server_count;
}

int test_util::get_leader_count(const string &table_name, int replica_server_index)
{
    const auto json_filename = fmt::format("test_json_file.{}", dsn::rand::next_u32());
    auto cleanup =
        dsn::defer([json_filename]() { dsn::utils::filesystem::remove_path(json_filename); });
    run_cmd_from_project_root(
        fmt::format("echo 'app {} -djo {}' | ./run.sh shell", table_name, json_filename));
    std::ifstream f(json_filename);
    const auto data = json::parse(f);
    int leader_count = 0;
    for (const auto &replica : data["replicas"]) {
        const auto &primary = to_string(replica["primary"]);
        if (primary.find(fmt::format("3480{}", replica_server_index)) != string::npos) {
            leader_count++;
        }
    }
    return leader_count;
}

} // namespace pegasus
