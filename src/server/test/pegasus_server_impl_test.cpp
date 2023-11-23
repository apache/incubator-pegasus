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

#include <base/pegasus_key_schema.h>
#include <fmt/core.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "pegasus_const.h"
#include "pegasus_server_test_base.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/serverlet.h"
#include "server/pegasus_read_service.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"

namespace pegasus {
namespace server {

class pegasus_server_impl_test : public pegasus_server_test_base
{
public:
    pegasus_server_impl_test() : pegasus_server_test_base() {}

    void test_table_level_slow_query()
    {
        // the on_get function will sleep 10ms for unit test,
        // so when we set slow_query_threshold <= 10ms, the perf counter will be incr by 1
        struct test_case
        {
            bool is_multi_get; // false-on_get, true-on_multi_get
            uint64_t slow_query_threshold_ms;
            uint8_t expect_perf_counter_incr;
        } tests[] = {{false, 10, 1}, {false, 300, 0}, {true, 10, 1}, {true, 300, 0}};

        // test key
        std::string test_hash_key = "test_hash_key";
        std::string test_sort_key = "test_sort_key";
        dsn::blob test_key;
        pegasus_generate_key(test_key, test_hash_key, test_sort_key);

        // do all of the tests
        for (auto test : tests) {
            // set table level slow query threshold
            std::map<std::string, std::string> envs;
            _server->query_app_envs(envs);
            envs[ROCKSDB_ENV_SLOW_QUERY_THRESHOLD] = std::to_string(test.slow_query_threshold_ms);
            _server->update_app_envs(envs);

            // do on_get/on_multi_get operation,
            long before_count = _server->_pfc_recent_abnormal_count->get_integer_value();
            if (!test.is_multi_get) {
                get_rpc rpc(std::make_unique<dsn::blob>(test_key), dsn::apps::RPC_RRDB_RRDB_GET);
                _server->on_get(rpc);
            } else {
                ::dsn::apps::multi_get_request request;
                request.__set_hash_key(dsn::blob(test_hash_key.data(), 0, test_hash_key.size()));
                request.__set_sort_keys({dsn::blob(test_sort_key.data(), 0, test_sort_key.size())});
                ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
                multi_get_rpc rpc(std::make_unique<::dsn::apps::multi_get_request>(request),
                                  dsn::apps::RPC_RRDB_RRDB_MULTI_GET);
                _server->on_multi_get(rpc);
            }
            long after_count = _server->_pfc_recent_abnormal_count->get_integer_value();

            ASSERT_EQ(before_count + test.expect_perf_counter_incr, after_count);
        }
    }

    void test_open_db_with_rocksdb_envs(bool is_restart)
    {
        struct create_test
        {
            std::string env_key;
            std::string env_value;
            std::string expect_value;
        } tests[] = {
            {"rocksdb.num_levels", "5", "5"}, {"rocksdb.write_buffer_size", "33554432", "33554432"},
        };

        std::map<std::string, std::string> all_test_envs;
        {
            // Make sure all rocksdb options of ROCKSDB_DYNAMIC_OPTIONS and ROCKSDB_STATIC_OPTIONS
            // are tested.
            for (const auto &test : tests) {
                all_test_envs[test.env_key] = test.env_value;
            }
            for (const auto &option : pegasus::ROCKSDB_DYNAMIC_OPTIONS) {
                ASSERT_TRUE(all_test_envs.find(option) != all_test_envs.end());
            }
            for (const auto &option : pegasus::ROCKSDB_STATIC_OPTIONS) {
                ASSERT_TRUE(all_test_envs.find(option) != all_test_envs.end());
            }
        }

        ASSERT_EQ(dsn::ERR_OK, start(all_test_envs));
        if (is_restart) {
            ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
            ASSERT_EQ(dsn::ERR_OK, start());
        }

        std::map<std::string, std::string> query_envs;
        _server->query_app_envs(query_envs);
        for (const auto &test : tests) {
            const auto &iter = query_envs.find(test.env_key);
            if (iter != query_envs.end()) {
                ASSERT_EQ(iter->second, test.expect_value);
            } else {
                ASSERT_TRUE(false) << fmt::format("query_app_envs not supported {}", test.env_key);
            }
        }
    }
};

INSTANTIATE_TEST_CASE_P(, pegasus_server_impl_test, ::testing::Values(false, true));

TEST_P(pegasus_server_impl_test, test_table_level_slow_query)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    test_table_level_slow_query();
}

TEST_P(pegasus_server_impl_test, default_data_version)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(_server->_pegasus_data_version, 1);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_latest_options)
{
    // open a new db with no app env.
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_NORMAL, _server->_usage_scenario);
    // set bulk_load scenario for the db.
    ASSERT_TRUE(_server->set_usage_scenario(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD));
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    rocksdb::Options opts = _server->_db->GetOptions();
    ASSERT_EQ(1000000000, opts.level0_file_num_compaction_trigger);
    ASSERT_EQ(true, opts.disable_auto_compactions);
    // reopen the db.
    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    ASSERT_EQ(opts.level0_file_num_compaction_trigger,
              _server->_db->GetOptions().level0_file_num_compaction_trigger);
    ASSERT_EQ(opts.disable_auto_compactions, _server->_db->GetOptions().disable_auto_compactions);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_app_envs)
{
    std::map<std::string, std::string> envs;
    envs[ROCKSDB_ENV_USAGE_SCENARIO_KEY] = ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;
    ASSERT_EQ(dsn::ERR_OK, start(envs));
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
}

TEST_P(pegasus_server_impl_test, test_open_db_with_rocksdb_envs)
{
    // Hint: Verify the set_rocksdb_options_before_creating function by boolean is_restart=false.
    test_open_db_with_rocksdb_envs(false);
}

TEST_P(pegasus_server_impl_test, test_restart_db_with_rocksdb_envs)
{
    // Hint: Verify the reset_rocksdb_options function by boolean is_restart=true.
    test_open_db_with_rocksdb_envs(true);
}

TEST_P(pegasus_server_impl_test, test_stop_db_twice)
{
    ASSERT_EQ(dsn::ERR_OK, start());
    ASSERT_TRUE(_server->_is_open);
    ASSERT_TRUE(_server->_db != nullptr);

    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);

    // stop again
    ASSERT_EQ(dsn::ERR_OK, _server->stop(false));
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);
}

TEST_P(pegasus_server_impl_test, test_update_user_specified_compaction)
{
    _server->_user_specified_compaction = "";
    std::map<std::string, std::string> envs;

    _server->update_user_specified_compaction(envs);
    ASSERT_EQ("", _server->_user_specified_compaction);

    std::string user_specified_compaction = "test";
    envs[USER_SPECIFIED_COMPACTION] = user_specified_compaction;
    _server->update_user_specified_compaction(envs);
    ASSERT_EQ(user_specified_compaction, _server->_user_specified_compaction);
}

TEST_P(pegasus_server_impl_test, test_load_from_duplication_data)
{
    auto origin_file = fmt::format("{}/{}", _server->duplication_dir(), "checkpoint");
    dsn::utils::filesystem::create_directory(_server->duplication_dir());
    dsn::utils::filesystem::create_file(origin_file);
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(origin_file));

    EXPECT_CALL(*_server, is_duplication_follower()).WillRepeatedly(testing::Return(true));

    auto tempFolder = "invalid";
    dsn::utils::filesystem::rename_path(_server->data_dir(), tempFolder);
    ASSERT_EQ(start(), dsn::ERR_FILE_OPERATION_FAILED);

    dsn::utils::filesystem::rename_path(tempFolder, _server->data_dir());
    auto rdb_path = fmt::format("{}/rdb/", _server->data_dir());
    auto new_file = fmt::format("{}/{}", rdb_path, "checkpoint");
    ASSERT_EQ(start(), dsn::ERR_LOCAL_APP_FAILURE);
    ASSERT_TRUE(dsn::utils::filesystem::directory_exists(rdb_path));
    ASSERT_FALSE(dsn::utils::filesystem::file_exists(origin_file));
    ASSERT_TRUE(dsn::utils::filesystem::file_exists(new_file));
    dsn::utils::filesystem::remove_file_name(new_file);
}

} // namespace server
} // namespace pegasus
