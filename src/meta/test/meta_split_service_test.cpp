/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "meta/meta_split_service.h"

namespace dsn {
namespace replication {
class meta_split_service_test : public meta_test_base
{
public:
    meta_split_service_test() {}

    void SetUp() override
    {
        meta_test_base::SetUp();
        create_app(NAME, PARTITION_COUNT);
    }

    error_code start_partition_split(const std::string &app_name, int new_partition_count)
    {
        auto request = dsn::make_unique<start_partition_split_request>();
        request->app_name = app_name;
        request->new_partition_count = new_partition_count;

        start_split_rpc rpc(std::move(request), RPC_CM_START_PARTITION_SPLIT);
        split_svc().start_partition_split(rpc);
        wait_all();
        return rpc.response().err;
    }

    register_child_response
    register_child(ballot req_parent_ballot, ballot child_ballot, bool wait_zk = false)
    {
        // mock local app info
        auto app = find_app(NAME);
        app->partition_count *= 2;
        app->partitions.resize(app->partition_count);
        app->helpers->contexts.resize(app->partition_count);
        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            app->partitions[i].pid = dsn::gpid(app->app_id, i);
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
            } else {
                app->partitions[i].ballot = PARENT_BALLOT;
            }
        }
        app->partitions[CHILD_INDEX].ballot = child_ballot;

        // mock node state
        node_state node;
        node.put_partition(dsn::gpid(app->app_id, PARENT_INDEX), true);
        mock_node_state(dsn::rpc_address("127.0.0.1", 10086), node);

        // mock register_child_request
        partition_configuration parent_config;
        parent_config.ballot = req_parent_ballot;
        parent_config.last_committed_decree = 5;
        parent_config.max_replica_count = 3;
        parent_config.pid = dsn::gpid(app->app_id, PARENT_INDEX);

        dsn::partition_configuration child_config;
        child_config.ballot = PARENT_BALLOT + 1;
        child_config.last_committed_decree = 5;
        child_config.pid = dsn::gpid(app->app_id, CHILD_INDEX);

        // register_child_request request;
        auto request = dsn::make_unique<register_child_request>();
        request->app.app_id = app->app_id;
        request->parent_config = parent_config;
        request->child_config = child_config;
        request->primary_address = dsn::rpc_address("127.0.0.1", 10086);

        register_child_rpc rpc(std::move(request), RPC_CM_REGISTER_CHILD_REPLICA);
        split_svc().register_child_on_meta(rpc);
        wait_all();
        if (wait_zk) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return rpc.response();
    }

    const std::string NAME = "split_table";
    const int32_t PARTITION_COUNT = 4;
    const int32_t NEW_PARTITION_COUNT = 8;
    const int32_t PARENT_BALLOT = 3;
    const int32_t PARENT_INDEX = 0;
    const int32_t CHILD_INDEX = 4;
};

// start split unit tests
TEST_F(meta_split_service_test, start_split_test)
{
    // Test case:
    // - app not existed
    // - wrong partition_count
    // - app already splitting
    // - start split succeed
    struct start_test
    {
        std::string app_name;
        int32_t new_partition_count;
        bool need_mock_splitting;
        error_code expected_err;
        int32_t expected_partition_count;
    } tests[] = {{"table_not_exist", PARTITION_COUNT, false, ERR_APP_NOT_EXIST, PARTITION_COUNT},
                 {NAME, PARTITION_COUNT, false, ERR_INVALID_PARAMETERS, PARTITION_COUNT},
                 {NAME, NEW_PARTITION_COUNT, true, ERR_BUSY, PARTITION_COUNT},
                 {NAME, NEW_PARTITION_COUNT, false, ERR_OK, NEW_PARTITION_COUNT}};

    for (auto test : tests) {
        auto app = find_app(NAME);
        app->helpers->split_states.splitting_count = test.need_mock_splitting ? PARTITION_COUNT : 0;
        ASSERT_EQ(start_partition_split(test.app_name, test.new_partition_count),
                  test.expected_err);
        ASSERT_EQ(app->partition_count, test.expected_partition_count);
    }
}

// TODO(heyuchen): refactor register unit tests
TEST_F(meta_split_service_test, register_child_with_wrong_ballot)
{
    auto resp = register_child(PARENT_BALLOT - 1, invalid_ballot);
    ASSERT_EQ(resp.err, ERR_INVALID_VERSION);
}

TEST_F(meta_split_service_test, register_child_with_child_registered)
{
    auto resp = register_child(PARENT_BALLOT, PARENT_BALLOT + 1);
    ASSERT_EQ(resp.err, ERR_CHILD_REGISTERED);
}

TEST_F(meta_split_service_test, register_child_succeed)
{
    auto resp = register_child(PARENT_BALLOT, invalid_ballot, true);
    ASSERT_EQ(resp.err, ERR_OK);
}

} // namespace replication
} // namespace dsn
