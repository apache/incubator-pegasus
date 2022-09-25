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
#include "test/function_test/utils/global_env.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

// TODO(yingchun): backup & restore festure is on refactoring, we can refactor the related function
// test later.
class backup_restore_test : public testing::Test
{
public:
    backup_restore_test()
        : _ddl_client(nullptr),
          _num_of_rows(1000),
          _check_interval_sec(10),
          _cluster_name("onebox"),
          _old_app_name("test_app"),
          _new_app_name("new_app"),
          _provider("local_service")
    {
    }

    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        // initialize ddl_client
        std::vector<rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), _cluster_name.c_str()));
        ASSERT_FALSE(meta_list.empty());
        _ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(_ddl_client != nullptr);
    }

    void TearDown() override
    {
        ASSERT_EQ(ERR_OK, _ddl_client->drop_app(_old_app_name, 0));
        ASSERT_EQ(ERR_OK, _ddl_client->drop_app(_new_app_name, 0));
    }

    bool write_data()
    {
        pegasus::pegasus_client *client = pegasus::pegasus_client_factory::get_client(
            _cluster_name.c_str(), _old_app_name.c_str());
        if (client == nullptr) {
            std::cout << "get pegasus client failed" << std::endl;
            return false;
        }

        for (int i = 0; i < _num_of_rows; ++i) {
            int ret = client->set("hashkey_" + std::to_string(i),
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
        pegasus::pegasus_client *client =
            pegasus::pegasus_client_factory::get_client(_cluster_name.c_str(), app_name.c_str());
        if (client == nullptr) {
            std::cout << "get pegasus client failed" << std::endl;
            return false;
        }

        for (int i = 0; i < _num_of_rows; ++i) {
            const std::string &expected_value = "value_" + std::to_string(i);
            std::string value;
            int ret =
                client->get("hashkey_" + std::to_string(i), "sortkey_" + std::to_string(i), value);
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
        return _ddl_client->backup_app(_old_app_id, _provider, user_specified_path).get_value();
    }

    query_backup_status_response query_backup(int64_t backup_id)
    {
        return _ddl_client->query_backup(_old_app_id, backup_id).get_value();
    }

    error_code start_restore(int64_t backup_id, const std::string &user_specified_path = "")
    {
        return _ddl_client->do_restore(_provider,
                                       _cluster_name,
                                       /*policy_name=*/"",
                                       backup_id,
                                       _old_app_name,
                                       _old_app_id,
                                       _new_app_name,
                                       /*skip_bad_partition=*/false,
                                       user_specified_path);
    }

    bool wait_backup_complete(int64_t backup_id, int max_sleep_seconds)
    {
        int sleep_sec = 0;
        bool is_backup_complete = false;
        while (!is_backup_complete && sleep_sec <= max_sleep_seconds) {
            std::cout << "sleep a while to wait backup complete." << std::endl;
            sleep(_check_interval_sec);
            sleep_sec += _check_interval_sec;

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
            sleep(_check_interval_sec);
            sleep_sec += _check_interval_sec;

            int32_t new_app_id;
            int32_t partition_count;
            std::vector<partition_configuration> partitions;
            auto err = _ddl_client->list_app(app_name, _old_app_id, partition_count, partitions);
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
        error_code err = _ddl_client->create_app(_old_app_name, "pegasus", 4, 3, {}, false);
        ASSERT_EQ(ERR_OK, err);
        ASSERT_TRUE(wait_app_become_healthy(_old_app_name, 180));

        ASSERT_TRUE(write_data());
        ASSERT_TRUE(verify_data(_old_app_name));

        auto resp = start_backup(user_specified_path);
        ASSERT_EQ(ERR_OK, resp.err);
        int64_t backup_id = resp.backup_id;
        ASSERT_TRUE(wait_backup_complete(backup_id, 180));
        err = start_restore(backup_id, user_specified_path);
        ASSERT_EQ(ERR_OK, err);
        ASSERT_TRUE(wait_app_become_healthy(_new_app_name, 180));

        ASSERT_TRUE(verify_data(_new_app_name));
    }

private:
    std::shared_ptr<replication_ddl_client> _ddl_client;

    const uint32_t _num_of_rows;
    const uint8_t _check_interval_sec;
    const std::string _cluster_name;
    const std::string _old_app_name;
    const std::string _new_app_name;
    const std::string _provider;

    int32_t _old_app_id;
};

TEST_F(backup_restore_test, test_backup_and_restore) { test_backup_and_restore(); }

TEST_F(backup_restore_test, test_backup_and_restore_with_user_specified_path)
{
    test_backup_and_restore("test/path");
}
