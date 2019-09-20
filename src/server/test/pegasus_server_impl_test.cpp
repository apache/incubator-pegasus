// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"

namespace pegasus {
namespace server {

class pegasus_server_impl_test : public pegasus_server_test_base
{
public:
    pegasus_server_impl_test() : pegasus_server_test_base() { start(); }

    void test_table_level_slow_query()
    {
        struct test_case
        {
            uint64_t threshold;
            uint8_t perf_counter_incr;

        } tests[] = {{0, 1}, {LONG_MAX, 0}};

        for (auto test : tests) {
            // set table level slow query threshold
            std::map<std::string, std::string> envs;
            _server->query_app_envs(envs);
            envs[ROCKSDB_ENV_SLOW_QUERY_THRESHOLD] = std::to_string(test.threshold);
            _server->update_app_envs(envs);

            // do get operation, and assert whether the perf counter is incremented or not
            std::string hash_key = "hash_key";
            ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
            long before_count =
                _server->_pfc_recent_table_level_slow_query_count->get_integer_value();
            _server->on_get(dsn::blob(hash_key.data(), 0, hash_key.size()), reply);
            long after_count =
                _server->_pfc_recent_table_level_slow_query_count->get_integer_value();

            ASSERT_EQ(before_count + test.perf_counter_incr, after_count);
        }
    }
};

TEST_F(pegasus_server_impl_test, test_table_level_slow_query) { test_table_level_slow_query(); }

} // namespace server
} // namespace pegasus
