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
#include "pegasus_server_test_base.h"

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
                get_rpc rpc(dsn::make_unique<dsn::blob>(test_key), dsn::apps::RPC_RRDB_RRDB_GET);
                _server->on_get(rpc);
            } else {
                ::dsn::apps::multi_get_request request;
                request.__set_hash_key(dsn::blob(test_hash_key.data(), 0, test_hash_key.size()));
                request.__set_sort_keys({dsn::blob(test_sort_key.data(), 0, test_sort_key.size())});
                ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
                multi_get_rpc rpc(dsn::make_unique<::dsn::apps::multi_get_request>(request),
                                  dsn::apps::RPC_RRDB_RRDB_MULTI_GET);
                _server->on_multi_get(rpc);
            }
            long after_count = _server->_pfc_recent_abnormal_count->get_integer_value();

            ASSERT_EQ(before_count + test.expect_perf_counter_incr, after_count);
        }
    }
};

TEST_F(pegasus_server_impl_test, test_table_level_slow_query)
{
    start();
    test_table_level_slow_query();
}

TEST_F(pegasus_server_impl_test, default_data_version)
{
    start();
    ASSERT_EQ(_server->_pegasus_data_version, 1);
}

TEST_F(pegasus_server_impl_test, test_open_db_with_latest_options)
{
    // open a new db with no app env.
    start();
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_NORMAL, _server->_usage_scenario);
    // set bulk_load scenario for the db.
    ASSERT_TRUE(_server->set_usage_scenario(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD));
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    rocksdb::Options opts = _server->_db->GetOptions();
    ASSERT_EQ(1000000000, opts.level0_file_num_compaction_trigger);
    ASSERT_EQ(true, opts.disable_auto_compactions);
    // reopen the db.
    _server->stop(false);
    start();
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
    ASSERT_EQ(opts.level0_file_num_compaction_trigger,
              _server->_db->GetOptions().level0_file_num_compaction_trigger);
    ASSERT_EQ(opts.disable_auto_compactions, _server->_db->GetOptions().disable_auto_compactions);
}

TEST_F(pegasus_server_impl_test, test_open_db_with_app_envs)
{
    std::map<std::string, std::string> envs;
    envs[ROCKSDB_ENV_USAGE_SCENARIO_KEY] = ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;
    start(envs);
    ASSERT_EQ(ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD, _server->_usage_scenario);
}

TEST_F(pegasus_server_impl_test, test_stop_db_twice)
{
    start();
    ASSERT_TRUE(_server->_is_open);
    ASSERT_TRUE(_server->_db != nullptr);

    _server->stop(false);
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);

    // stop again
    _server->stop(false);
    ASSERT_FALSE(_server->_is_open);
    ASSERT_TRUE(_server->_db == nullptr);
}

} // namespace server
} // namespace pegasus
