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

#include<vector>
#include<string>

#include <base/pegasus_const.h>
#include <base/pegasus_key_schema.h>
#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <gtest/gtest.h>
#include <pegasus/client.h>
#include <rrdb/rrdb_types.h>
#include <rrdb/rrdb.client.h>
#include <rocksdb/status.h>

using namespace ::pegasus;
using namespace ::dsn;
using namespace ::replication;

extern pegasus_client *client;
extern std::shared_ptr<replication_ddl_client> ddl_client;


TEST(batch_get, set_and_then_batch_get)
{
    std::vector<rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");
    auto rrdb_client = new ::dsn::apps::rrdb_client("mycluster", meta_list, client->get_app_name());

    int test_data_count = 100;
    int test_timeout_milliseconds = 3000;
    uint64_t test_partition_hash = 0;

    apps::batch_get_request batch_request;
    std::vector<std::pair<std::string, std::string>> key_pair_list;
    std::vector<std::string> value_list;

    for (int i = 0; i < test_data_count; ++i) {
        std::string hash_key = "hash_key_prefix_" + std::to_string(i);
        std::string sort_key = "sort_key_prefix_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);

        apps::update_request one_request;
        one_request.__isset.key = true;
        pegasus_generate_key(one_request.key, hash_key, sort_key);
        one_request.__isset.value = true;
        one_request.value.assign(value.c_str(), 0, value.size());
        auto put_result = rrdb_client->put_sync(one_request, std::chrono::milliseconds(test_timeout_milliseconds), test_partition_hash);
        ASSERT_EQ(ERR_OK,  put_result.first);
        ASSERT_EQ(rocksdb::Status::kOk, put_result.second.error);

        apps::full_key one_full_key;
        one_full_key.__isset.hash_key = true;
        one_full_key.hash_key.assign(hash_key.c_str(), 0, hash_key.size());
        one_full_key.__isset.sort_key = true;
        one_full_key.sort_key.assign(sort_key.c_str(), 0, sort_key.size());
        batch_request.keys.emplace_back(one_full_key);

        key_pair_list.emplace_back(std::make_pair(hash_key, sort_key));
        value_list.push_back(value);
    }

    auto batch_get_result = rrdb_client->batch_get_sync(batch_request, std::chrono::milliseconds(test_timeout_milliseconds), test_partition_hash);
    ASSERT_EQ(ERR_OK, batch_get_result.first);
    auto &response = batch_get_result.second;
    ASSERT_EQ(rocksdb::Status::kOk, response.error);
    ASSERT_EQ(test_data_count, response.data.size());
    for (int i = 0; i < test_data_count; ++i) {
        ASSERT_EQ(response.data[i].hash_key.to_string(), key_pair_list[i].first);
        ASSERT_EQ(response.data[i].sort_key.to_string(), key_pair_list[i].second);
        ASSERT_EQ(response.data[i].value.to_string(), value_list[i]);
        ASSERT_EQ(response.data[i].exists, true);
    }
}


