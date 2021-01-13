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

    error_code control_partition_split(const std::string &app_name,
                                       split_control_type::type type,
                                       const int32_t pidx,
                                       const int32_t old_partition_count = 0)
    {
        auto req = make_unique<control_split_request>();
        req->__set_app_name(app_name);
        req->__set_control_type(type);
        req->__set_parent_pidx(pidx);
        req->__set_old_partition_count(old_partition_count);

        control_split_rpc rpc(std::move(req), RPC_CM_CONTROL_PARTITION_SPLIT);
        split_svc().control_partition_split(rpc);
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

    void clear_app_partition_split_context()
    {
        app->partition_count = PARTITION_COUNT;
        app->partitions.resize(app->partition_count);
        app->helpers->contexts.resize(app->partition_count);
        app->helpers->split_states.splitting_count = 0;
        app->helpers->split_states.status.clear();
    }

    void mock_child_registered()
    {
        app->partitions[CHILD_INDEX].ballot = PARENT_BALLOT;
        app->helpers->split_states.splitting_count--;
        app->helpers->split_states.status.erase(PARENT_INDEX);
    }

    void mock_split_states(split_status::type status, int32_t parent_index = -1)
    {
        if (parent_index != -1) {
            app->helpers->split_states.status[parent_index] = status;
        } else {
            auto partition_count = app->partition_count;
            for (auto i = 0; i < partition_count / 2; ++i) {
                app->helpers->split_states.status[i] = status;
            }
        }
    }

    bool check_split_status(split_status::type expected_status, int32_t parent_index = -1)
    {
        auto app = find_app(NAME);
        if (parent_index != -1) {
            return (app->helpers->split_states.status[parent_index] == expected_status);
        } else {
            for (const auto kv : app->helpers->split_states.status) {
                if (kv.second != expected_status) {
                    return false;
                }
            }
            return true;
        }
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

/// control split unit tests
TEST_F(meta_split_service_test, pause_or_restart_single_partition_test)
{
    // Test case:
    // - pause with wrong pidx
    // - pause with partition is not splitting
    // - pause with partition split_status = pausing
    // - pause with partition split_status = paused
    // - pause with partition split_status = canceling
    // - pause with partition split_status = splitting
    // - restart with partition is not splitting
    // - restart with partition split_status = pausing
    // - restart with partition split_status = paused
    // - restart with partition split_status = canceling
    // - restart with partition split_status = splitting
    struct control_single_partition_test
    {
        int32_t pidx;
        split_status::type cur_status;
        split_control_type::type control_type;
        error_code expected_err;
        split_status::type expected_status;
    } tests[] = {{NEW_PARTITION_COUNT,
                  split_status::SPLITTING,
                  split_control_type::PAUSE,
                  ERR_INVALID_PARAMETERS,
                  split_status::SPLITTING},
                 {PARENT_INDEX,
                  split_status::NOT_SPLIT,
                  split_control_type::PAUSE,
                  ERR_CHILD_REGISTERED,
                  split_status::NOT_SPLIT},
                 {PARENT_INDEX,
                  split_status::PAUSING,
                  split_control_type::PAUSE,
                  ERR_INVALID_STATE,
                  split_status::PAUSING},
                 {PARENT_INDEX,
                  split_status::PAUSED,
                  split_control_type::PAUSE,
                  ERR_INVALID_STATE,
                  split_status::PAUSED},
                 {PARENT_INDEX,
                  split_status::CANCELING,
                  split_control_type::PAUSE,
                  ERR_INVALID_STATE,
                  split_status::CANCELING},
                 {PARENT_INDEX,
                  split_status::SPLITTING,
                  split_control_type::PAUSE,
                  ERR_OK,
                  split_status::PAUSING},
                 {PARENT_INDEX,
                  split_status::NOT_SPLIT,
                  split_control_type::RESTART,
                  ERR_INVALID_STATE,
                  split_status::NOT_SPLIT},
                 {PARENT_INDEX,
                  split_status::PAUSING,
                  split_control_type::RESTART,
                  ERR_INVALID_STATE,
                  split_status::PAUSING},
                 {PARENT_INDEX,
                  split_status::PAUSED,
                  split_control_type::RESTART,
                  ERR_OK,
                  split_status::SPLITTING},
                 {PARENT_INDEX,
                  split_status::CANCELING,
                  split_control_type::RESTART,
                  ERR_INVALID_STATE,
                  split_status::CANCELING},
                 {PARENT_INDEX,
                  split_status::SPLITTING,
                  split_control_type::RESTART,
                  ERR_INVALID_STATE,
                  split_status::SPLITTING}};

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.cur_status == split_status::NOT_SPLIT) {
            mock_child_registered();
        } else {
            mock_split_states(test.cur_status, PARENT_INDEX);
        }
        ASSERT_EQ(control_partition_split(NAME, test.control_type, test.pidx, PARTITION_COUNT),
                  test.expected_err);
        if (test.expected_err == ERR_OK) {
            ASSERT_TRUE(check_split_status(test.expected_status, test.pidx));
        }
        clear_app_partition_split_context();
    }
}

TEST_F(meta_split_service_test, pause_or_restart_multi_partitions_test)
{
    // Test case:
    // - app not existed
    // - app is not splitting
    // - pausing all splitting partitions succeed
    // - restart all paused partitions succeed
    struct control_multi_partitions_test
    {
        bool mock_split_context;
        std::string app_name;
        split_control_type::type control_type;
        error_code expected_err;
    } tests[] = {{false, "table_not_exist", split_control_type::PAUSE, ERR_APP_NOT_EXIST},
                 {false, NAME, split_control_type::RESTART, ERR_INVALID_STATE},
                 {true, NAME, split_control_type::PAUSE, ERR_OK},
                 {true, NAME, split_control_type::RESTART, ERR_OK}};

    for (auto test : tests) {
        if (test.mock_split_context) {
            mock_app_partition_split_context();
            if (test.control_type == split_control_type::RESTART) {
                mock_split_states(split_status::PAUSED, -1);
            }
        }
        error_code ec =
            control_partition_split(test.app_name, test.control_type, -1, PARTITION_COUNT);
        ASSERT_EQ(ec, test.expected_err);
        if (test.expected_err == ERR_OK) {
            split_status::type expected_status = test.control_type == split_control_type::PAUSE
                                                     ? split_status::PAUSING
                                                     : split_status::SPLITTING;
            ASSERT_TRUE(check_split_status(expected_status, -1));
        }
        if (test.mock_split_context) {
            clear_app_partition_split_context();
        }
    }
}

TEST_F(meta_split_service_test, cancel_split_test)
{
    // Test case:
    // - wrong partition count
    // - cancel split with child registered
    // - cancel succeed
    struct cancel_test
    {
        int32_t old_partition_count;
        bool mock_child_registered;
        error_code expected_err;
        bool check_status;
    } tests[] = {{NEW_PARTITION_COUNT, false, ERR_INVALID_PARAMETERS, false},
                 {PARTITION_COUNT, true, ERR_CHILD_REGISTERED, false},
                 {PARTITION_COUNT, false, ERR_OK, true}};

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }

        ASSERT_EQ(
            control_partition_split(NAME, split_control_type::CANCEL, -1, test.old_partition_count),
            test.expected_err);
        if (test.check_status) {
            auto app = find_app(NAME);
            ASSERT_EQ(app->partition_count, NEW_PARTITION_COUNT);
            ASSERT_EQ(app->helpers->split_states.splitting_count, PARTITION_COUNT);
            check_split_status(split_status::CANCELING, -1);
        }
        clear_app_partition_split_context();
    }
}

} // namespace replication
} // namespace dsn
