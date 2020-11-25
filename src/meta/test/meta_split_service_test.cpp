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
        app = find_app(NAME);
    }

    void TearDown()
    {
        app.reset();
        meta_test_base::TearDown();
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

    error_code register_child(int32_t parent_index, ballot req_parent_ballot, bool wait_zk)
    {
        partition_configuration parent_config;
        parent_config.ballot = req_parent_ballot;
        parent_config.last_committed_decree = 5;
        parent_config.max_replica_count = 3;
        parent_config.pid = gpid(app->app_id, parent_index);

        partition_configuration child_config;
        child_config.ballot = PARENT_BALLOT + 1;
        child_config.last_committed_decree = 5;
        child_config.pid = gpid(app->app_id, parent_index + PARTITION_COUNT);

        // mock node state
        node_state node;
        node.put_partition(gpid(app->app_id, PARENT_INDEX), true);
        mock_node_state(NODE, node);

        auto request = dsn::make_unique<register_child_request>();
        request->app.app_name = app->app_name;
        request->app.app_id = app->app_id;
        request->parent_config = parent_config;
        request->child_config = child_config;
        request->primary_address = NODE;

        register_child_rpc rpc(std::move(request), RPC_CM_REGISTER_CHILD_REPLICA);
        split_svc().register_child_on_meta(rpc);
        wait_all();
        if (wait_zk) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return rpc.response().err;
    }

    int32_t on_config_sync(configuration_query_by_node_request req)
    {
        auto request = make_unique<configuration_query_by_node_request>(req);
        configuration_query_by_node_rpc rpc(std::move(request), RPC_CM_CONFIG_SYNC);
        _ss->on_config_sync(rpc);
        wait_all();
        int32_t splitting_count = 0;
        for (auto p : rpc.response().partitions) {
            if (p.__isset.meta_split_status) {
                ++splitting_count;
            }
        }
        return splitting_count;
    }

    void mock_app_partition_split_context()
    {
        app->partition_count = NEW_PARTITION_COUNT;
        app->partitions.resize(app->partition_count);
        app->helpers->contexts.resize(app->partition_count);
        app->helpers->split_states.splitting_count = app->partition_count / 2;
        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            app->partitions[i].pid = gpid(app->app_id, i);
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
            } else {
                app->partitions[i].ballot = PARENT_BALLOT;
                app->helpers->contexts[i].stage = config_status::not_pending;
                app->helpers->split_states.status[i] = split_status::SPLITTING;
            }
        }
    }

    void mock_child_registered()
    {
        app->partitions[CHILD_INDEX].ballot = PARENT_BALLOT;
        app->helpers->split_states.splitting_count--;
        app->helpers->split_states.status.erase(PARENT_INDEX);
    }

    const std::string NAME = "split_table";
    const int32_t PARTITION_COUNT = 4;
    const int32_t NEW_PARTITION_COUNT = 8;
    const int32_t PARENT_BALLOT = 3;
    const int32_t PARENT_INDEX = 0;
    const int32_t CHILD_INDEX = 4;
    const rpc_address NODE = rpc_address("127.0.0.1", 10086);
    std::shared_ptr<app_state> app;
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

// register child unit tests
TEST_F(meta_split_service_test, register_child_test)
{
    // Test case:
    // - request is out-dated
    // - child has been registered
    // - TODO(heyuchen): parent partition has been paused splitting
    // - parent partition is sync config to remote storage
    // - register child succeed
    struct register_test
    {
        int32_t parent_ballot;
        bool mock_child_registered;
        bool mock_parent_paused;
        bool mock_pending;
        error_code expected_err;
        bool wait_zk;
    } tests[] = {
        {PARENT_BALLOT - 1, false, false, false, ERR_INVALID_VERSION, false},
        {PARENT_BALLOT, true, false, false, ERR_CHILD_REGISTERED, false},
        {PARENT_BALLOT, false, false, true, ERR_IO_PENDING, false},
        {PARENT_BALLOT, false, false, false, ERR_OK, true},
    };

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        if (test.mock_parent_paused) {
            // TODO(heyuchen): mock split paused
        }
        if (test.mock_pending) {
            app->helpers->contexts[PARENT_INDEX].stage = config_status::pending_remote_sync;
        }
        ASSERT_EQ(register_child(PARENT_INDEX, test.parent_ballot, test.wait_zk),
                  test.expected_err);
    }
}

// config sync unit tests
TEST_F(meta_split_service_test, on_config_sync_test)
{
    create_app("not_splitting_app");
    auto not_splitting_app = find_app("not_splitting_app");
    gpid pid1 = gpid(app->app_id, PARENT_INDEX);
    gpid pid2 = gpid(not_splitting_app->app_id, 0);
    // mock meta server node state
    node_state node;
    node.put_partition(pid1, true);
    node.put_partition(pid2, true);
    mock_node_state(NODE, node);
    // mock request
    replica_info info1, info2;
    info1.pid = pid1;
    info2.pid = pid2;
    configuration_query_by_node_request req;
    req.node = NODE;
    req.__isset.stored_replicas = true;
    req.stored_replicas.emplace_back(info1);
    req.stored_replicas.emplace_back(info2);

    // Test case:
    // - partition is splitting
    // - partition is not splitting
    // - TODO(heyuchen): partition split is paused({false, true, 1})
    struct config_sync_test
    {
        bool mock_child_registered;
        bool mock_parent_paused;
        int32_t expected_count;
    } tests[] = {{false, false, 1}, {true, false, 0}};

    for (const auto &test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        if (test.mock_parent_paused) {
            // TODO(heyuchen): TBD
        }
        ASSERT_EQ(on_config_sync(req), test.expected_count);
    }

    drop_app("not_splitting_app");
}

} // namespace replication
} // namespace dsn
