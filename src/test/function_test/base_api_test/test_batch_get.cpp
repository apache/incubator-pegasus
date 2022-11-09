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

#include <vector>
#include <string>

#include "base/pegasus_const.h"
#include "base/pegasus_key_schema.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "client/replication_ddl_client.h"
#include <gtest/gtest.h>
#include "include/pegasus/client.h"
#include <rrdb/rrdb_types.h>
#include "include/rrdb/rrdb.client.h"
#include <rocksdb/status.h>

#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;
using namespace ::dsn;
using namespace ::replication;

class batch_get : public test_util
{
};

TEST_F(batch_get, set_and_then_batch_get)
{
    auto rrdb_client =
        new ::dsn::apps::rrdb_client(cluster_name_.c_str(), meta_list_, app_name_.c_str());

    int test_data_count = 100;
    int test_timeout_milliseconds = 3000;
    uint64_t test_partition_hash = 0;

    apps::batch_get_request batch_request;
    std::vector<std::string> test_data_hash_keys(test_data_count);
    std::vector<std::string> test_data_sort_keys(test_data_count);
    std::vector<std::string> test_data_values(test_data_count);
    for (int i = 0; i < test_data_count; ++i) {
        test_data_hash_keys[i] = "hash_key_prefix_" + std::to_string(i);
        test_data_sort_keys[i] = "sort_key_prefix_" + std::to_string(i);
        test_data_values[i] = "value_" + std::to_string(i);

        apps::update_request one_request;
        one_request.__isset.key = true;
        pegasus_generate_key(one_request.key, test_data_hash_keys[i], test_data_sort_keys[i]);
        one_request.__isset.value = true;
        one_request.value.assign(test_data_values[i].c_str(), 0, test_data_values[i].size());
        auto put_result = rrdb_client->put_sync(
            one_request, std::chrono::milliseconds(test_timeout_milliseconds), test_partition_hash);
        ASSERT_EQ(ERR_OK, put_result.first);
        ASSERT_EQ(rocksdb::Status::kOk, put_result.second.error);

        apps::full_key one_full_key;
        one_full_key.__isset.hash_key = true;
        one_full_key.hash_key.assign(
            test_data_hash_keys[i].c_str(), 0, test_data_hash_keys[i].size());
        one_full_key.__isset.sort_key = true;
        one_full_key.sort_key.assign(
            test_data_sort_keys[i].c_str(), 0, test_data_sort_keys[i].size());
        batch_request.keys.emplace_back(std::move(one_full_key));
    }

    int test_no_exist_data_count = 6;
    std::vector<std::string> no_exist_data_hash_keys(test_no_exist_data_count);
    std::vector<std::string> no_exist_data_sort_keys(test_no_exist_data_count);
    std::vector<std::string> no_exist_data_values(test_no_exist_data_count);
    for (int i = 0; i < test_no_exist_data_count; ++i) {
        no_exist_data_hash_keys[i] = "hash_key_prefix_no_exist_" + std::to_string(i);
        no_exist_data_sort_keys[i] = "sort_key_prefix_no_exist_" + std::to_string(i);

        apps::full_key one_full_key;
        one_full_key.__isset.hash_key = true;
        one_full_key.hash_key.assign(
            no_exist_data_hash_keys[i].c_str(), 0, no_exist_data_hash_keys[i].size());
        one_full_key.__isset.sort_key = true;
        one_full_key.sort_key.assign(
            no_exist_data_sort_keys[i].c_str(), 0, no_exist_data_sort_keys[i].size());
        batch_request.keys.emplace_back(std::move(one_full_key));
    }

    auto batch_get_result = rrdb_client->batch_get_sync(
        batch_request, std::chrono::milliseconds(test_timeout_milliseconds), test_partition_hash);
    ASSERT_EQ(ERR_OK, batch_get_result.first);
    auto &response = batch_get_result.second;
    ASSERT_EQ(rocksdb::Status::kOk, response.error);
    ASSERT_EQ(test_data_count, response.data.size());
    for (int i = 0; i < test_data_count; ++i) {
        ASSERT_EQ(response.data[i].hash_key.to_string(), test_data_hash_keys[i]);
        ASSERT_EQ(response.data[i].sort_key.to_string(), test_data_sort_keys[i]);
        ASSERT_EQ(response.data[i].value.to_string(), test_data_values[i]);
    }
}
