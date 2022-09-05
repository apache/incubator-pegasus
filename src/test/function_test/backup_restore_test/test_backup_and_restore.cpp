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

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
#include <dsn/utility/filesystem.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include "include/pegasus/client.h"
#include "include/pegasus/error.h"

#include "base/pegasus_const.h"
#include "test/function_test/utils/test_util.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

class backup_restore_test : public test_util
{
public:
    backup_restore_test()
        : _num_of_rows(1000),
          _check_interval_sec(10),
          _old_app_name("test_app"),
          _new_app_name("new_app"),
          _provider("local_service")
    {
    }

    void SetUp() override
    {
        test_util::SetUp();
        ASSERT_EQ(ERR_OK, ddl_client_->create_app(_old_app_name, "pegasus", 4, 3, {}, false));
        ASSERT_NO_FATAL_FAILURE(wait_app_become_healthy(_old_app_name, 180));
    }

    void TearDown() override
    {
        test_util::TearDown();
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(_old_app_name, 0));
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(_new_app_name, 0));
    }

    void write_data()
    {
        pegasus::pegasus_client *client = pegasus::pegasus_client_factory::get_client(
            cluster_name_.c_str(), _old_app_name.c_str());
        ASSERT_NE(client, nullptr);

        for (int i = 0; i < _num_of_rows; ++i) {
            ASSERT_EQ(pegasus::PERR_OK,
                      client->set("hashkey_" + std::to_string(i),
                                  "sortkey_" + std::to_string(i),
                                  "value_" + std::to_string(i)));
        }
    }

    void verify_data(const std::string &app_name)
    {
        pegasus::pegasus_client *client =
            pegasus::pegasus_client_factory::get_client(cluster_name_.c_str(), app_name.c_str());
        ASSERT_NE(client, nullptr);

        for (int i = 0; i < _num_of_rows; ++i) {
            std::string value;
            ASSERT_EQ(
                pegasus::PERR_OK,
                client->get("hashkey_" + std::to_string(i), "sortkey_" + std::to_string(i), value));
            ASSERT_EQ("value_" + std::to_string(i), value);
        }
    }

    void wait_backup_complete(int64_t backup_id, int max_sleep_seconds)
    {
        int sleep_sec = 0;
        bool is_backup_complete = false;
        while (!is_backup_complete && sleep_sec <= max_sleep_seconds) {
            std::cout << "sleep a while to wait backup complete." << std::endl;
            sleep(_check_interval_sec);
            sleep_sec += _check_interval_sec;

            auto resp = ddl_client_->query_backup(_old_app_id, backup_id).get_value();
            ASSERT_EQ(ERR_OK, resp.err);
            // we got only one backup_item for a certain app_id and backup_id.
            auto item = resp.backup_items[0];
            is_backup_complete = (item.end_time_ms > 0);
        }
        ASSERT_TRUE(is_backup_complete);
    }

    void wait_app_become_healthy(const std::string &app_name, uint32_t max_sleep_seconds)
    {
        int sleep_sec = 0;
        bool is_app_healthy = false;
        while (!is_app_healthy && sleep_sec <= max_sleep_seconds) {
            std::cout << "sleep a while to wait app become healthy." << std::endl;
            sleep(_check_interval_sec);
            sleep_sec += _check_interval_sec;

            int32_t new_app_id;
            int32_t partition_count;
            std::vector<partition_configuration> partitions;
            ASSERT_EQ(ERR_OK,
                      ddl_client_->list_app(app_name, _old_app_id, partition_count, partitions));
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
        ASSERT_TRUE(is_app_healthy);
    }

    void test_backup_and_restore(const std::string &user_specified_path = "")
    {
        ASSERT_NO_FATAL_FAILURE(write_data());
        ASSERT_NO_FATAL_FAILURE(verify_data(_old_app_name));

        auto resp =
            ddl_client_->backup_app(_old_app_id, _provider, user_specified_path).get_value();
        ASSERT_EQ(ERR_OK, resp.err);
        int64_t backup_id = resp.backup_id;
        ASSERT_NO_FATAL_FAILURE(wait_backup_complete(backup_id, 180));
        ASSERT_EQ(ERR_OK,
                  ddl_client_->do_restore(_provider,
                                          cluster_name_,
                                          /*policy_name=*/"",
                                          backup_id,
                                          _old_app_name,
                                          _old_app_id,
                                          _new_app_name,
                                          /*skip_bad_partition=*/false,
                                          user_specified_path));
        ASSERT_NO_FATAL_FAILURE(wait_app_become_healthy(_new_app_name, 180));
        ASSERT_NO_FATAL_FAILURE(verify_data(_new_app_name));
    }

private:
    const uint32_t _num_of_rows;
    const uint8_t _check_interval_sec;
    const std::string _old_app_name;
    const std::string _new_app_name;
    const std::string _provider;

    int32_t _old_app_id;
};

TEST_F(backup_restore_test, test_backup_and_restore_basic) { test_backup_and_restore(); }

TEST_F(backup_restore_test, test_backup_and_restore_with_user_specified_path)
{
    test_backup_and_restore("test/path");
}

