// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <stdint.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "backup_types.h"
#include "base/pegasus_const.h"
#include "client/replication_ddl_client.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "include/pegasus/error.h"
#include "runtime/rpc/rpc_address.h"
#include "test/function_test/utils/test_util.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/utils.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

class backup_restore_test : public test_util
{
public:
    void TearDown() override
    {
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(app_name_, 0));
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(s_new_app_name, 0));
    }

    bool write_data()
    {
        for (int i = 0; i < s_num_of_rows; ++i) {
            int ret = client_->set("hashkey_" + std::to_string(i),
                                   "sortkey_" + std::to_string(i),
                                   "value_" + std::to_string(i));
            if (ret != pegasus::PERR_OK) {
                std::cout << "write data failed. " << std::endl;
                return false;
            }
        }
        return true;
    }

    bool verify_data(const std::string &app_name)
    {
        for (int i = 0; i < s_num_of_rows; ++i) {
            const std::string &expected_value = "value_" + std::to_string(i);
            std::string value;
            int ret =
                client_->get("hashkey_" + std::to_string(i), "sortkey_" + std::to_string(i), value);
            if (ret != pegasus::PERR_OK) {
                return false;
            }
            if (value != expected_value) {
                return false;
            }
        }
        return true;
    }

    start_backup_app_response start_backup(const std::string &user_specified_path = "")
    {
        return ddl_client_->backup_app(app_id_, s_provider_type, user_specified_path).get_value();
    }

    query_backup_status_response query_backup(int64_t backup_id)
    {
        return ddl_client_->query_backup(app_id_, backup_id).get_value();
    }

    error_code start_restore(int64_t backup_id, const std::string &user_specified_path = "")
    {
        return ddl_client_->do_restore(s_provider_type,
                                       cluster_name_,
                                       /*policy_name=*/"",
                                       backup_id,
                                       app_name_,
                                       app_id_,
                                       s_new_app_name,
                                       /*skip_bad_partition=*/false,
                                       user_specified_path);
    }

    bool wait_backup_complete(int64_t backup_id, int max_sleep_seconds)
    {
        int sleep_sec = 0;
        bool is_backup_complete = false;
        while (!is_backup_complete && sleep_sec <= max_sleep_seconds) {
            std::cout << "sleep a while to wait backup complete." << std::endl;
            sleep(s_check_interval_sec);
            sleep_sec += s_check_interval_sec;

            auto resp = query_backup(backup_id);
            if (resp.err != ERR_OK) {
                return false;
            }
            // we got only one backup_item for a certain app_id and backup_id.
            auto item = resp.backup_items[0];
            is_backup_complete = (item.end_time_ms > 0);
        }
        return is_backup_complete;
    }

    bool wait_app_become_healthy(const std::string &app_name, uint32_t max_sleep_seconds)
    {
        int sleep_sec = 0;
        bool is_app_healthy = false;
        while (!is_app_healthy && sleep_sec <= max_sleep_seconds) {
            std::cout << "sleep a while to wait app become healthy." << std::endl;
            sleep(s_check_interval_sec);
            sleep_sec += s_check_interval_sec;

            int32_t partition_count;
            std::vector<partition_configuration> partitions;
            auto err = ddl_client_->list_app(app_name, app_id_, partition_count, partitions);
            if (err != ERR_OK) {
                std::cout << "list app " + app_name + " failed" << std::endl;
                return false;
            }
            int32_t healthy_partition_count = 0;
            for (const auto &partition : partitions) {
                if (partition.primary.is_invalid()) {
                    break;
                }
                if (partition.secondaries.size() + 1 < partition.max_replica_count) {
                    break;
                }
                healthy_partition_count++;
            }
            is_app_healthy = (healthy_partition_count == partition_count);
        }
        return is_app_healthy;
    }

    void test_backup_and_restore(const std::string &user_specified_path = "")
    {
        ASSERT_TRUE(wait_app_become_healthy(app_name_, 180));

        ASSERT_TRUE(write_data());
        ASSERT_TRUE(verify_data(app_name_));

        auto resp = start_backup(user_specified_path);
        ASSERT_EQ(ERR_OK, resp.err);
        int64_t backup_id = resp.backup_id;
        ASSERT_TRUE(wait_backup_complete(backup_id, 180));
        ASSERT_EQ(ERR_OK, start_restore(backup_id, user_specified_path));
        ASSERT_TRUE(wait_app_become_healthy(s_new_app_name, 180));

        ASSERT_TRUE(verify_data(s_new_app_name));
    }

private:
    static const uint32_t s_num_of_rows = 1000;
    static const uint8_t s_check_interval_sec = 10;
    static const std::string s_new_app_name;
    static const std::string s_provider_type;
};

const std::string backup_restore_test::s_new_app_name = "new_app";
const std::string backup_restore_test::s_provider_type = "local_service";

TEST_F(backup_restore_test, test_backup_and_restore) { test_backup_and_restore(); }

TEST_F(backup_restore_test, test_backup_and_restore_with_user_specified_path)
{
    test_backup_and_restore("test/path");
}
