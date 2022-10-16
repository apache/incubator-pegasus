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

#include <vector>

#include "base/pegasus_const.h"
#include "client/replication_ddl_client.h"
#include "common/replication_other_types.h"
#include "utils/rpc_address.h"
#include "include/pegasus/client.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/utils.h"

using dsn::partition_configuration;
using dsn::replication::replica_helper;
using dsn::replication::replication_ddl_client;
using dsn::rpc_address;
using std::vector;

namespace pegasus {

test_util::test_util(std::map<std::string, std::string> create_envs)
    : cluster_name_("mycluster"), app_name_("temp"), create_envs_(std::move(create_envs))
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

void test_util::run_cmd_from_project_root(const std::string &cmd)
{
    ASSERT_EQ(0, chdir(global_env::instance()._pegasus_root.c_str()));
    ASSERT_NO_FATAL_FAILURE(run_cmd(cmd));
}

} // namespace pegasus
