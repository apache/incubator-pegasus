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

    void test_table_level_latency() {
        // set table level latency threshold to a very small num,
        // in order to ensure the get operation will exceed the threshold
        std::map<std::string, std::string> envs;
        _server->query_app_envs(envs);
        envs[ROCKSDB_ENV_TABLE_LEVEL_GET_LATENCY] = std::to_string(20);
        _server->update_app_envs(envs);

        // do get operation, and assert that the perf counter is incremented by 1
        std::string hash_key = "hash_key";
        dsn::blob key(hash_key.data(), 0, hash_key.size());
        ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
        long before_count = _server->_pfc_recent_table_level_abnormal_count->get_integer_value();
        _server->on_get(key, reply);
        long after_count = _server->_pfc_recent_table_level_abnormal_count->get_integer_value();
        ASSERT_EQ(before_count + 1, after_count);

        // set table level latency threshold to 0,
        // which means don't check whether it exceed table level latency threshold or not
        envs[ROCKSDB_ENV_TABLE_LEVEL_GET_LATENCY] = std::to_string(0);
        _server->update_app_envs(envs);

        // do get operation, and assert that the perf counter doesn't change
        before_count = _server->_pfc_recent_table_level_abnormal_count->get_integer_value();
        _server->on_get(key, reply);
        after_count = _server->_pfc_recent_table_level_abnormal_count->get_integer_value();
        ASSERT_EQ(before_count, after_count);
    }
};

TEST_F(pegasus_server_impl_test, table_level_latency) { test_table_level_latency(); }

} // namespace server
} // namespace pegasus
