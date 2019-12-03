// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <base/pegasus_key_schema.h>
#include <dsn/utility/defer.h>
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "pegasus_server_test_base.h"
#include "message_utils.h"

namespace pegasus {
namespace server {

class pegasus_server_impl_test : public pegasus_server_test_base
{
public:
    pegasus_server_impl_test() : pegasus_server_test_base() { start(); }

    rocksdb::DB *get_server_db() { return _server->_db; }

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
                ::dsn::rpc_replier<::dsn::apps::read_response> reply(nullptr);
                _server->on_get(test_key, reply);
            } else {
                ::dsn::apps::multi_get_request request;
                request.__set_hash_key(dsn::blob(test_hash_key.data(), 0, test_hash_key.size()));
                request.__set_sort_keys({dsn::blob(test_sort_key.data(), 0, test_sort_key.size())});
                ::dsn::rpc_replier<::dsn::apps::multi_get_response> reply(nullptr);
                _server->on_multi_get(request, reply);
            }
            long after_count = _server->_pfc_recent_abnormal_count->get_integer_value();

            ASSERT_EQ(before_count + test.expect_perf_counter_incr, after_count);
        }
    }
};

TEST_F(pegasus_server_impl_test, test_table_level_slow_query) { test_table_level_slow_query(); }

TEST_F(pegasus_server_impl_test, test_rdb_estimate_num_keys)
{
    std::unique_ptr<pegasus_server_write> server_write =
        dsn::make_unique<pegasus_server_write>(_server.get(), true);

    dsn::apps::update_request req;
    pegasus_generate_key(req.key, std::string("hash"), std::string("sort"));
    req.value.assign("value", 0, 5);

    const int put_cnt = 5;
    auto writes = new dsn::message_ex *[put_cnt];
    // generate same five kv, and the kEstimateNumKeys will be count five time
    for (int i = 0; i < put_cnt; i++) {
        writes[i] = pegasus::create_put_request(req);
    }
    server_write->on_batched_write_requests(writes, put_cnt, 0, 0);
    auto cleanup = dsn::defer([=]() { delete[] writes; });

    std::string str_val;
    get_server_db()->GetProperty(rocksdb::DB::Properties::kEstimateNumKeys, &str_val);
    ASSERT_EQ(atoi(str_val.c_str()), put_cnt);
}

} // namespace server
} // namespace pegasus
