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

#pragma once

#include <gtest/gtest.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "dsn.layer2_types.h"
#include "runtime/rpc/rpc_address.h"

// TODO(yingchun): it's too tricky, but I don't know how does it happen, we can fix it later.
#define TRICKY_CODE_TO_AVOID_LINK_ERROR                                                            \
    do {                                                                                           \
        ddl_client_->create_app("", "pegasus", 0, 0, {}, false);                                   \
        pegasus_client_factory::get_client("", "");                                                \
    } while (false)

namespace dsn {
namespace replication {
class replication_ddl_client;
} // namespace replication
} // namespace dsn

namespace pegasus {
class pegasus_client;

class test_util : public ::testing::Test
{
public:
    test_util(std::map<std::string, std::string> create_envs = {});
    virtual ~test_util();

    static void SetUpTestCase();

    void SetUp() override;

    static void run_cmd_from_project_root(const std::string &cmd);

    static int get_alive_replica_server_count();

    // Get the leader replica count of the 'replica_server_index' (based on 1) replica server
    // on the 'table_name'.
    static int get_leader_count(const std::string &table_name, int replica_server_index);

protected:
    const std::string cluster_name_;
    std::string app_name_;
    const std::map<std::string, std::string> create_envs_;
    int32_t app_id_;
    int32_t partition_count_ = 8;
    std::vector<dsn::partition_configuration> partitions_;
    pegasus_client *client_ = nullptr;
    std::vector<dsn::rpc_address> meta_list_;
    std::shared_ptr<dsn::replication::replication_ddl_client> ddl_client_;
};
} // namespace pegasus
