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
#include <boost/lexical_cast.hpp>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include <unistd.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>

#include "client/replication_ddl_client.h"

#include "base/pegasus_const.h"
#include "test/function_test/utils/utils.h"
#include "test/function_test/utils/test_util.h"

using namespace dsn::replication;
using namespace pegasus;

class drop_and_recall : public test_util
{
protected:
    const std::string key_prefix = "hello";
    const std::string value_prefix = "world";

    const int kv_count = 10000;
};

TEST_F(drop_and_recall, simple)
{
    std::vector<std::string> hashkeys_for_gpid(partition_count_, "");

    // then write keys
    for (int i = 0; i < kv_count; ++i) {
        std::string hash_key = key_prefix + boost::lexical_cast<std::string>(i);
        std::string sort_key = hash_key;
        std::string value = value_prefix + boost::lexical_cast<std::string>(i);

        pegasus::pegasus_client::internal_info info;
        int ans;
        RETRY_OPERATION(client_->set(hash_key, sort_key, value, 5000, 0, &info), ans);
        ASSERT_EQ(PERR_OK, ans);
        ASSERT_TRUE(info.partition_index < partition_count_);
        if (hashkeys_for_gpid[info.partition_index].empty()) {
            hashkeys_for_gpid[info.partition_index] = hash_key;
        }
    }

    for (const auto &hashkey : hashkeys_for_gpid) {
        ASSERT_FALSE(hashkey.empty());
    }

    // drop the table
    ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(app_name_, 0));

    // wait for all elements to be dropped
    for (int i = 0; i < partition_count_; ++i) {
        int j;
        for (j = 0; j < 60; ++j) {
            pegasus::pegasus_client::internal_info info;
            client_->set(hashkeys_for_gpid[i], "", "", 1000, 0, &info);
            if (info.app_id == -1) {
                std::cout << "partition " << i << " is removed from server" << std::endl;
                break;
            } else {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        ASSERT_LT(j, 60);
    }

    // then recall table
    ASSERT_EQ(dsn::ERR_OK, ddl_client_->recall_app(app_id_, ""));

    // then read all keys
    for (int i = 0; i < kv_count; ++i) {
        std::string hash_key = key_prefix + std::to_string(i);
        std::string sort_key = hash_key;
        std::string exp_value = value_prefix + std::to_string(i);

        std::string act_value;
        int ans;
        RETRY_OPERATION(client_->get(hash_key, sort_key, act_value), ans);
        ASSERT_EQ(PERR_OK, ans);
        ASSERT_EQ(exp_value, act_value);
    }
}
