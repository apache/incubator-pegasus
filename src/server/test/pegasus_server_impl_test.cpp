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
            bool is_multi_get; // false-on_get, true-on_multi_get
            uint64_t slow_query_threshold;
            uint8_t perf_counter_incr;
        } tests[] = {{false, 0, 1}, {false, LONG_MAX, 0}, {true, 0, 1}, {true, LONG_MAX, 0}};

        for (auto test : tests) {
            // set table level slow query threshold
            std::map<std::string, std::string> envs;
            _server->query_app_envs(envs);
            envs[ROCKSDB_ENV_SLOW_QUERY_THRESHOLD] = std::to_string(test.slow_query_threshold);
            _server->update_app_envs(envs);

            // do on_get/on_multi_get operation,
            // and assert whether the perf counter is incremented or not
            long before_count = _server->_pfc_recent_abnormal_count->get_integer_value();
            if (!test.is_multi_get) {
                std::string test_key = "test_key";
                ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
                _server->on_get(dsn::blob(test_key.data(), 0, test_key.size()), reply);
            } else {
                std::string hash_key = "test_hash_key";
                std::string sort_key = "test_sort_key";

                ::dsn::apps::multi_get_request request;
                request.__set_hash_key(dsn::blob(hash_key.data(), 0, hash_key.size()));
                request.__set_sort_keys({dsn::blob(sort_key.data(), 0, sort_key.size())});
                ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
                _server->on_multi_get(request, reply);
            }
            long after_count = _server->_pfc_recent_abnormal_count->get_integer_value();

            ASSERT_EQ(before_count + test.perf_counter_incr, after_count);
        }
    }
};

TEST_F(pegasus_server_impl_test, test_table_level_slow_query) { test_table_level_slow_query(); }

} // namespace server
} // namespace pegasus
