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

#include <fmt/core.h>
#include <cstdint>
#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "misc/misc.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "task/task_tracker.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

DSN_DECLARE_int32(max_allowed_replica_count);
DSN_DECLARE_int32(min_allowed_replica_count);
DSN_DECLARE_uint64(min_live_node_count_for_unfreeze);

namespace dsn {
class blob;

namespace replication {

class meta_app_operation_test : public meta_test_base
{
public:
    meta_app_operation_test() = default;

    error_code create_app_test(const std::string &app_name,
                               int32_t partition_count,
                               int32_t replica_count,
                               bool success_if_exist,
                               const std::map<std::string, std::string> &envs,
                               bool atomic_idempotent)
    {
        configuration_create_app_request create_request;
        configuration_create_app_response create_response;
        create_request.app_name = app_name;
        create_request.options.app_type = "simple_kv";
        create_request.options.partition_count = partition_count;
        create_request.options.replica_count = replica_count;
        create_request.options.success_if_exist = success_if_exist;
        create_request.options.is_stateful = true;
        create_request.options.envs = envs;
        create_request.options.__set_atomic_idempotent(atomic_idempotent);

        auto result = fake_create_app(_ss.get(), create_request);
        fake_wait_rpc(result, create_response);
        return create_response.err;
    }

    error_code drop_app_test(const std::string &app_name)
    {
        configuration_drop_app_request drop_request;
        configuration_drop_app_response drop_response;
        drop_request.app_name = app_name;
        drop_request.options.success_if_not_exist = false;

        auto result = fake_drop_app(_ss.get(), drop_request);
        fake_wait_rpc(result, drop_response);
        if (drop_response.err == ERR_OK) {
            _ss->spin_wait_staging(30);
        }
        return drop_response.err;
    }

    error_code recall_app_test(const std::string &new_app_name, int32_t app_id)
    {
        configuration_recall_app_request recall_request;
        configuration_recall_app_response recall_response;

        recall_request.app_id = app_id;
        recall_request.new_app_name = new_app_name;
        auto result = fake_recall_app(_ss.get(), recall_request);
        fake_wait_rpc(result, recall_response);
        if (recall_response.err == ERR_OK) {
            _ss->spin_wait_staging(30);
        }
        return recall_response.err;
    }

    void update_app_status(app_status::type status)
    {
        auto app = find_app(APP_NAME);
        app->status = status;
    }

    void drop_app_with_expired()
    {
        auto app_id = find_app(APP_NAME)->app_id;
        drop_app(APP_NAME);

        // dropped app can only be find by app_id
        auto app = _ss->get_app(app_id);
        // FLAGS_hold_seconds_for_dropped_app = 604800 in unit test config
        // make app expired immediatly
        app->expire_second -= 604800;
    }

    void clear_nodes() { _ss->_nodes.clear(); }

    configuration_get_max_replica_count_response get_max_replica_count(const std::string &app_name)
    {
        auto req = std::make_unique<configuration_get_max_replica_count_request>();
        req->__set_app_name(app_name);

        configuration_get_max_replica_count_rpc rpc(std::move(req), RPC_CM_GET_MAX_REPLICA_COUNT);
        _ss->get_max_replica_count(rpc);
        _ss->wait_all_task();

        return rpc.response();
    }

    void set_partition_max_replica_count(const std::string &app_name,
                                         int32_t partition_index,
                                         int32_t max_replica_count)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        auto &pc = app->pcs[partition_index];
        pc.max_replica_count = max_replica_count;
    }

    void set_max_replica_count_env(const std::string &app_name, const std::string &env)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        if (env.empty()) {
            app->envs.erase(replica_envs::UPDATE_MAX_REPLICA_COUNT);
        } else {
            app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT] = env;
        }

        // Set env on remote storage.
        dsn::task_tracker tracker;
        _ms->get_remote_storage()->set_data(
            _ss->get_app_path(*app),
            json::json_forwarder<app_info>::encode(*app),
            LPC_META_STATE_HIGH,
            [](error_code ec) { ASSERT_EQ(ERR_OK, ec); },
            &tracker);
        tracker.wait_outstanding_tasks();
    }

    configuration_set_max_replica_count_response set_max_replica_count(const std::string &app_name,
                                                                       int32_t max_replica_count)
    {
        auto req = std::make_unique<configuration_set_max_replica_count_request>();
        req->__set_app_name(app_name);
        req->__set_max_replica_count(max_replica_count);

        configuration_set_max_replica_count_rpc rpc(std::move(req), RPC_CM_SET_MAX_REPLICA_COUNT);
        _ss->set_max_replica_count(rpc);
        _ss->wait_all_task();

        return rpc.response();
    }

    configuration_rename_app_response rename_app(const std::string &old_app_name,
                                                 const std::string &new_app_name)
    {
        auto req = std::make_unique<configuration_rename_app_request>();
        req->__set_old_app_name(old_app_name);
        req->__set_new_app_name(new_app_name);

        configuration_rename_app_rpc rpc(std::move(req), RPC_CM_RENAME_APP);
        _ss->rename_app(rpc);
        _ss->wait_all_task();

        return rpc.response();
    }

    void set_app_and_all_partitions_max_replica_count(const std::string &app_name,
                                                      int32_t max_replica_count)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        for (auto &pc : app->pcs) {
            // Set `max_replica_count` of each partition locally.
            pc.max_replica_count = max_replica_count;

            // Set `max_replica_count` of each partition on remote storage.
            dsn::task_tracker tracker;
            _ms->get_remote_storage()->set_data(
                _ss->get_partition_path(pc.pid),
                json::json_forwarder<partition_configuration>::encode(pc),
                LPC_META_STATE_HIGH,
                [](error_code ec) { ASSERT_EQ(ec, ERR_OK); },
                &tracker);
            tracker.wait_outstanding_tasks();
        }

        // Set `max_replica_count` of the table locally.
        app->max_replica_count = max_replica_count;

        // Set `max_replica_count` of the table on remote storage.
        dsn::task_tracker tracker;
        _ms->get_remote_storage()->set_data(
            _ss->get_app_path(*app),
            json::json_forwarder<app_info>::encode(*app),
            LPC_META_STATE_HIGH,
            [](error_code ec) { ASSERT_EQ(ec, ERR_OK); },
            &tracker);
        tracker.wait_outstanding_tasks();
    }

    void verify_all_partitions_max_replica_count(const std::string &app_name,
                                                 int32_t expected_max_replica_count)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        for (auto &pc : app->pcs) {
            // Verify `max_replica_count` of each partition locally.
            ASSERT_EQ(expected_max_replica_count, pc.max_replica_count);

            // Verify `max_replica_count` of each partition on remote storage.
            dsn::task_tracker tracker;
            _ms->get_remote_storage()->get_data(
                _ss->get_partition_path(pc.pid),
                LPC_META_CALLBACK,
                [expected_pid = pc.pid, expected_max_replica_count](error_code ec,
                                                                    const blob &value) {
                    ASSERT_EQ(ERR_OK, ec);

                    partition_configuration pc;
                    dsn::json::json_forwarder<partition_configuration>::decode(value, pc);

                    ASSERT_EQ(expected_pid, pc.pid);
                    ASSERT_EQ(expected_max_replica_count, pc.max_replica_count);
                },
                &tracker);
            tracker.wait_outstanding_tasks();
        }
    }

    void verify_app_max_replica_count(const std::string &app_name,
                                      int32_t expected_max_replica_count)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        // Verify `max_replica_count` of the table locally.
        ASSERT_EQ(expected_max_replica_count, app->max_replica_count);
        // The env of `max_replica_count` should have been removed under normal circumstances.
        ASSERT_FALSE(gutil::ContainsKey(app->envs, replica_envs::UPDATE_MAX_REPLICA_COUNT));

        // Verify `max_replica_count` of the table on remote storage.
        dsn::task_tracker tracker;
        _ms->get_remote_storage()->get_data(
            _ss->get_app_path(*app),
            LPC_META_CALLBACK,
            [app, expected_max_replica_count](error_code ec, const blob &value) {
                ASSERT_EQ(ec, ERR_OK);

                app_info ainfo;
                json::json_forwarder<app_info>::decode(value, ainfo);

                ASSERT_EQ(app->app_name, ainfo.app_name);
                ASSERT_EQ(app->app_id, ainfo.app_id);
                ASSERT_EQ(expected_max_replica_count, ainfo.max_replica_count);

                // The env of `max_replica_count` should have been removed under normal
                // circumstances.
                ASSERT_FALSE(
                    gutil::ContainsKey(ainfo.envs, replica_envs::UPDATE_MAX_REPLICA_COUNT));
            },
            &tracker);
        tracker.wait_outstanding_tasks();
    }

    void verify_app_envs(const std::string &app_name,
                         const std::map<std::string, std::string> &expected_envs)
    {
        auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        dsn::task_tracker tracker;
        _ms->get_remote_storage()->get_data(
            _ss->get_app_path(*app),
            LPC_META_CALLBACK,
            [app_name, expected_envs, app](error_code ec, const blob &value) {
                ASSERT_EQ(ERR_OK, ec);

                app_info ainfo;
                json::json_forwarder<app_info>::decode(value, ainfo);

                ASSERT_EQ(app_name, app->app_name);
                ASSERT_EQ(app_name, ainfo.app_name);
                ASSERT_EQ(app->app_id, ainfo.app_id);
                ASSERT_EQ(expected_envs, app->envs);
                ASSERT_EQ(expected_envs, ainfo.envs);
            },
            &tracker);
        tracker.wait_outstanding_tasks();
    }

    void verify_app_atomic_idempotent(const std::string &app_name, bool expected_atomic_idempotent)
    {
        const auto app = find_app(app_name);
        CHECK(app, "app({}) does not exist", app_name);

        // `app->__isset.atomic_idempotent` must be true since by default it is true
        // (because `app->atomic_idempotent` has default value false).
        ASSERT_TRUE(app->__isset.atomic_idempotent);

        // Verify `atomic_idempotent` of the table locally.
        ASSERT_EQ(expected_atomic_idempotent, app->atomic_idempotent);

        dsn::task_tracker tracker;
        _ms->get_remote_storage()->get_data(
            _ss->get_app_path(*app),
            LPC_META_CALLBACK,
            [app, expected_atomic_idempotent](error_code ec, const blob &value) {
                ASSERT_EQ(ERR_OK, ec);

                app_info ainfo;
                dsn::json::json_forwarder<app_info>::decode(value, ainfo);

                ASSERT_EQ(app->app_name, ainfo.app_name);
                ASSERT_EQ(app->app_id, ainfo.app_id);

                // `ainfo.__isset.atomic_idempotent` must be true since by default it
                // is true (because `ainfo.atomic_idempotent` has default value false).
                ASSERT_TRUE(ainfo.__isset.atomic_idempotent);

                // Verify `atomic_idempotent` on remote storage.
                ASSERT_EQ(expected_atomic_idempotent, ainfo.atomic_idempotent);
            },
            &tracker);
        tracker.wait_outstanding_tasks();
    }

    configuration_get_atomic_idempotent_response get_atomic_idempotent(const std::string &app_name)
    {
        auto req = std::make_unique<configuration_get_atomic_idempotent_request>();
        req->__set_app_name(app_name);

        configuration_get_atomic_idempotent_rpc rpc(std::move(req), RPC_CM_GET_ATOMIC_IDEMPOTENT);
        _ss->get_atomic_idempotent(rpc);
        _ss->wait_all_task();

        return rpc.response();
    }

    void test_get_atomic_idempotent(const std::string &app_name, bool expected_atomic_idempotent)
    {
        const auto resp = get_atomic_idempotent(app_name);
        ASSERT_EQ(ERR_OK, resp.err);
        ASSERT_EQ(expected_atomic_idempotent, resp.atomic_idempotent);
    }

    configuration_set_atomic_idempotent_response set_atomic_idempotent(const std::string &app_name,
                                                                       bool atomic_idempotent)
    {
        auto req = std::make_unique<configuration_set_atomic_idempotent_request>();
        req->__set_app_name(app_name);
        req->__set_atomic_idempotent(atomic_idempotent);

        configuration_set_atomic_idempotent_rpc rpc(std::move(req), RPC_CM_SET_ATOMIC_IDEMPOTENT);
        _ss->set_atomic_idempotent(rpc);
        _ss->wait_all_task();

        return rpc.response();
    }

    void test_set_atomic_idempotent(const std::string &app_name,
                                    bool expected_new_atomic_idempotent,
                                    bool expected_old_atomic_idempotent)
    {
        const auto resp = set_atomic_idempotent(app_name, expected_new_atomic_idempotent);
        ASSERT_EQ(ERR_OK, resp.err);
        ASSERT_EQ(expected_old_atomic_idempotent, resp.old_atomic_idempotent);

        // Ensure that new `atomic_idempotent` has been updated to `server_state`.
        test_get_atomic_idempotent(app_name, expected_new_atomic_idempotent);
    }

    const std::string APP_NAME = "app_operation_test";
    const std::string OLD_APP_NAME = "old_app_operation";
    const std::string DUP_MASTER_APP_NAME = "dup_master_test";
    const std::string DUP_FOLLOWER_APP_NAME = "dup_follower_test";
    const std::string ATOMIC_IDEMPOTENT_APP_NAME = "atomic_idempotent_test";
};

TEST_F(meta_app_operation_test, create_app)
{
    // Test cases: (assert min_allowed_replica_count <= max_allowed_replica_count)
    struct create_test
    {
        std::string app_name;
        int32_t partition_count;
        int32_t replica_count;
        uint64_t min_live_node_count_for_unfreeze;
        int alive_node_count;
        int32_t min_allowed_replica_count;
        bool success_if_exist;
        app_status::type before_status;
        error_code expected_err;
        std::map<std::string, std::string> envs = {};
        bool atomic_idempotent = false;
    } tests[] = {
        // Wrong partition_count (< 0).
        {APP_NAME, -1, 3, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong partition_count (= 0).
        {APP_NAME, 0, 3, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< 0).
        {APP_NAME, 4, -1, 1, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (= 0).
        {APP_NAME, 4, 0, 1, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (> max_allowed_replica_count > alive_node_count).
        {APP_NAME, 4, 6, 2, 4, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (> alive_node_count > max_allowed_replica_count).
        {APP_NAME, 4, 7, 2, 6, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (> alive_node_count = max_allowed_replica_count).
        {APP_NAME, 4, 6, 2, 5, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (= max_allowed_replica_count, and > alive_node_count).
        {APP_NAME, 4, 5, 2, 4, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< max_allowed_replica_count, and > alive_node_count).
        {APP_NAME, 4, 4, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (= alive_node_count, and > max_allowed_replica_count).
        {APP_NAME, 4, 6, 2, 6, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< alive_node_count, and > max_allowed_replica_count).
        {APP_NAME, 4, 6, 2, 7, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Valid replica_count (= max_allowed_replica_count, and = alive_node_count).
        {APP_NAME + "_1", 4, 5, 2, 5, 1, false, app_status::AS_INVALID, ERR_OK},
        // Valid replica_count (= max_allowed_replica_count, and < alive_node_count).
        {APP_NAME + "_2", 4, 5, 2, 6, 1, false, app_status::AS_INVALID, ERR_OK},
        // Valid replica_count (< max_allowed_replica_count, and = alive_node_count).
        {APP_NAME + "_3", 4, 4, 2, 4, 1, false, app_status::AS_INVALID, ERR_OK},
        // Valid replica_count (< max_allowed_replica_count < alive_node_count).
        {APP_NAME + "_4", 4, 4, 2, 6, 1, false, app_status::AS_INVALID, ERR_OK},
        // Valid replica_count (< alive_node_count < max_allowed_replica_count).
        {APP_NAME + "_5", 4, 3, 2, 4, 1, false, app_status::AS_INVALID, ERR_OK},
        // Valid replica_count (< alive_node_count = max_allowed_replica_count).
        {APP_NAME + "_6", 4, 4, 2, 5, 1, false, app_status::AS_INVALID, ERR_OK},
        // Wrong replica_count (< min_allowed_replica_count < alive_node_count).
        {APP_NAME, 4, 3, 2, 5, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< alive_node_count < min_allowed_replica_count).
        {APP_NAME, 4, 3, 2, 4, 5, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< min_allowed_replica_count = alive_node_count).
        {APP_NAME, 4, 3, 2, 4, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< min_allowed_replica_count, and > alive_node_count).
        {APP_NAME, 4, 3, 2, 2, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (< min_allowed_replica_count, and = alive_node_count).
        {APP_NAME, 4, 3, 2, 3, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Wrong replica_count (= min_allowed_replica_count, and > alive_node_count).
        {APP_NAME, 4, 4, 2, 3, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        // Valid replica_count (= min_allowed_replica_count, and < alive_node_count).
        {APP_NAME + "_7", 4, 3, 2, 4, 3, false, app_status::AS_INVALID, ERR_OK},
        // Cluster freezed (alive_node_count = 0).
        {APP_NAME, 4, 1, 1, 0, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
        // Cluster freezed (alive_node_count = 1 < min_live_node_count_for_unfreeze).
        {APP_NAME, 4, 2, 2, 1, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
        // Cluster freezed (alive_node_count = 2 < min_live_node_count_for_unfreeze).
        {APP_NAME, 4, 3, 3, 2, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
        // Cluster not freezed (alive_node_count = min_live_node_count_for_unfreeze).
        {APP_NAME + "_8", 4, 3, 3, 3, 1, false, app_status::AS_INVALID, ERR_OK},
        // Create succeed with single-replica.
        {APP_NAME + "_9", 4, 1, 1, 1, 1, false, app_status::AS_INVALID, ERR_OK},
        // Create succeed with double-replica.
        {APP_NAME + "_10", 4, 2, 1, 2, 2, false, app_status::AS_INVALID, ERR_OK},
        // Create app succeed.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_INVALID, ERR_OK},
        // Create failed with table existed.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_INVALID, ERR_APP_EXIST},
        // Wrong app_status creating.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_CREATING, ERR_BUSY_CREATING},
        // Wrong app_status recalling.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_RECALLING, ERR_BUSY_CREATING},
        // Wrong app_status dropping.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_DROPPING, ERR_BUSY_DROPPING},
        // Create succeed with app_status dropped.
        {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_DROPPED, ERR_OK},
        // Create succeed with success_if_exist=true.
        {APP_NAME, 4, 3, 2, 3, 3, true, app_status::AS_INVALID, ERR_OK},
        // Wrong rocksdb.num_levels (< 1).
        {APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.num_levels", "0"}}},
        // Wrong rocksdb.num_levels (> 10).
        {APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.num_levels", "11"}}},
        // Wrong rocksdb.num_levels (non-digital character).
        {APP_NAME + "_11",
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.num_levels", "5i"}}},
        // Create app with rocksdb.num_levels (= 5) succeed.
        {APP_NAME + "_11",
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_OK,
         {{"rocksdb.num_levels", "5"}}},
        // Wrong rocksdb.write_buffer_size (< (16<<20)).
        {APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.write_buffer_size", "1000"}}},
        // Wrong rocksdb.write_buffer_size (> (512<<20)).
        {APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.write_buffer_size", "1073741824"}}},
        // Wrong rocksdb.write_buffer_size (non-digital character).
        {APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_INVALID_PARAMETERS,
         {{"rocksdb.write_buffer_size", "n33554432"}}},
        // Create app with rocksdb.write_buffer_size (= (32<<20)) succeed.
        {APP_NAME + "_12",
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_OK,
         {{"rocksdb.write_buffer_size", "33554432"}}},
        // Process the first request of creating follower app for duplication from the
        // source cluster.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_OK,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreating}}},
        // Process the request of creating follower app for duplication from the wrong
        // source cluster.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_APP_EXIST,
         {{duplication_constants::kEnvMasterClusterKey, "another_source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreating}}},
        // Process the request of creating follower app for duplication without the env of
        // creating status.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_APP_EXIST,
         {
             {duplication_constants::kEnvMasterClusterKey, "source_cluster"},
             {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
             {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
         }},
        // Process the repeated request of creating follower app for duplication from the
        // source cluster.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_OK,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreating}}},
        // Process the request of marking follower app as created for duplication from the
        // source cluster.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_OK,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreated}}},
        // Process the request of creating follower app for duplication from the source
        // cluster while it has been marked as created.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_APP_EXIST,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreating}}},
        // Process the repeated request of marking follower app as created for duplication
        // from the source cluster.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_OK,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey,
           duplication_constants::kEnvFollowerAppStatusCreated}}},
        // Process the request of creating follower app for duplication with invalid creating
        // status.
        {DUP_FOLLOWER_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_AVAILABLE,
         ERR_INVALID_PARAMETERS,
         {{duplication_constants::kEnvMasterClusterKey, "source_cluster"},
          {duplication_constants::kEnvMasterMetasKey, "10.1.2.3:34601"},
          {duplication_constants::kEnvMasterAppNameKey, DUP_MASTER_APP_NAME},
          {duplication_constants::kEnvFollowerAppStatusKey, "invalid_creating_status"}}},
        // Create a table with idempotence enabled for atomic writes.
        {ATOMIC_IDEMPOTENT_APP_NAME,
         4,
         3,
         2,
         3,
         3,
         false,
         app_status::AS_INVALID,
         ERR_OK,
         {},
         true},
    };

    clear_nodes();

    // Keep the number of all nodes greater than that of alive nodes.
    const int total_node_count = 10;
    auto nodes = ensure_enough_alive_nodes(total_node_count);

    // The meta function level will become freezed once
    // alive_nodes * 100 < total_nodes * _node_live_percentage_threshold_for_update
    // even if alive_nodes >= min_live_node_count_for_unfreeze
    set_node_live_percentage_threshold_for_update(0);

    // Save original FLAGS_min_live_node_count_for_unfreeze
    auto reserved_min_live_node_count_for_unfreeze = FLAGS_min_live_node_count_for_unfreeze;

    // Save original FLAGS_max_allowed_replica_count
    auto reserved_max_allowed_replica_count = FLAGS_max_allowed_replica_count;

    // Keep FLAGS_max_allowed_replica_count fixed in the tests
    auto res = update_flag("max_allowed_replica_count", "5");
    ASSERT_TRUE(res.is_ok());

    // Save original FLAGS_min_allowed_replica_count.
    auto reserved_min_allowed_replica_count = FLAGS_min_allowed_replica_count;

    for (const auto &test : tests) {
        res = update_flag("min_allowed_replica_count",
                          std::to_string(test.min_allowed_replica_count));
        ASSERT_TRUE(res.is_ok());

        set_min_live_node_count_for_unfreeze(test.min_live_node_count_for_unfreeze);

        CHECK_GE(total_node_count, test.alive_node_count);
        for (int i = 0; i < total_node_count - test.alive_node_count; i++) {
            _ms->set_node_state({nodes[i]}, false);
        }

        if (test.before_status == app_status::AS_DROPPED) {
            update_app_status(app_status::AS_AVAILABLE);
            drop_app(APP_NAME);
        } else if (test.before_status != app_status::AS_INVALID) {
            update_app_status(test.before_status);
        }

        const auto err = create_app_test(test.app_name,
                                         test.partition_count,
                                         test.replica_count,
                                         test.success_if_exist,
                                         test.envs,
                                         test.atomic_idempotent);
        ASSERT_EQ(test.expected_err, err);

        if (test.expected_err == ERR_OK) {
            verify_app_envs(test.app_name, test.envs);
            verify_app_atomic_idempotent(test.app_name, test.atomic_idempotent);
        }

        _ms->set_node_state(nodes, true);
    }

    {
        // Make sure all rocksdb options of ROCKSDB_DYNAMIC_OPTIONS and ROCKSDB_STATIC_OPTIONS are
        // tested. Hint: Mainly verify the validate_app_envs function.
        std::map<std::string, std::string> all_test_envs;
        for (const auto &test : tests) {
            all_test_envs.insert(test.envs.begin(), test.envs.end());
        }
        for (const auto &option : replica_envs::ROCKSDB_DYNAMIC_OPTIONS) {
            ASSERT_TRUE(gutil::ContainsKey(all_test_envs, option));
        }
        for (const auto &option : replica_envs::ROCKSDB_STATIC_OPTIONS) {
            ASSERT_TRUE(gutil::ContainsKey(all_test_envs, option));
        }
    }

    // Set FLAGS_min_allowed_replica_count successfully.
    res = update_flag("min_allowed_replica_count", "2");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(2, FLAGS_min_allowed_replica_count);

    // Set FLAGS_max_allowed_replica_count successfully.
    res = update_flag("max_allowed_replica_count", "6");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(6, FLAGS_max_allowed_replica_count);

    // Failed to set FLAGS_min_allowed_replica_count due to individual validation.
    res = update_flag("min_allowed_replica_count", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(2, FLAGS_min_allowed_replica_count);
    std::cout << res.description() << std::endl;

    // Failed to set FLAGS_max_allowed_replica_count due to individual validation.
    res = update_flag("max_allowed_replica_count", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(6, FLAGS_max_allowed_replica_count);
    std::cout << res.description() << std::endl;

    // Failed to set FLAGS_min_allowed_replica_count due to grouped validation.
    res = update_flag("min_allowed_replica_count", "7");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(2, FLAGS_min_allowed_replica_count);
    std::cout << res.description() << std::endl;

    // Failed to set FLAGS_max_allowed_replica_count due to grouped validation.
    res = update_flag("max_allowed_replica_count", "1");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(6, FLAGS_max_allowed_replica_count);
    std::cout << res.description() << std::endl;

    // Recover original FLAGS_min_allowed_replica_count.
    res = update_flag("min_allowed_replica_count",
                      std::to_string(reserved_min_allowed_replica_count));
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_min_allowed_replica_count, reserved_min_allowed_replica_count);

    // Recover original FLAGS_max_allowed_replica_count.
    res = update_flag("max_allowed_replica_count",
                      std::to_string(reserved_max_allowed_replica_count));
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(reserved_max_allowed_replica_count, FLAGS_max_allowed_replica_count);

    // Recover original FLAGS_min_live_node_count_for_unfreeze.
    set_min_live_node_count_for_unfreeze(reserved_min_live_node_count_for_unfreeze);
}

TEST_F(meta_app_operation_test, drop_app)
{
    create_app(APP_NAME);

    // Test cases:
    // - drop app not exist
    // - wrong app_status creating
    // - wrong app_status recalling
    // - wrong app_status dropping
    // - drop app succeed
    struct drop_test
    {
        std::string app_name;
        app_status::type before_status;
        error_code expected_err;
    } tests[] = {{"table_not_exist", app_status::AS_INVALID, ERR_APP_NOT_EXIST},
                 {APP_NAME, app_status::AS_CREATING, ERR_BUSY_CREATING},
                 {APP_NAME, app_status::AS_RECALLING, ERR_BUSY_CREATING},
                 {APP_NAME, app_status::AS_DROPPING, ERR_BUSY_DROPPING},
                 {APP_NAME, app_status::AS_AVAILABLE, ERR_OK}};

    for (auto test : tests) {
        if (test.before_status != app_status::AS_INVALID) {
            update_app_status(test.before_status);
        }
        auto err = drop_app_test(test.app_name);
        ASSERT_EQ(err, test.expected_err);
    }
}

TEST_F(meta_app_operation_test, recall_app)
{
    create_app(OLD_APP_NAME);
    auto old_app = find_app(OLD_APP_NAME);
    auto old_app_id = old_app->app_id;
    auto invalid_app_id = 100;

    // Test cases:
    // - wrong app_id
    // - wrong app_status available
    // - wrong app_status creating
    // - wrong app_status recalling
    // - wrong app_status dropping
    // - recall succeed
    // - recall failed because app is totally dropped
    struct drop_test
    {
        int32_t app_id;
        std::string new_app_name;
        app_status::type before_status;
        bool is_totally_dropped;
        error_code expected_err;
    } tests[] = {{invalid_app_id, APP_NAME, app_status::AS_INVALID, false, ERR_APP_NOT_EXIST},
                 {old_app_id, OLD_APP_NAME, app_status::AS_AVAILABLE, false, ERR_APP_EXIST},
                 {old_app_id, APP_NAME, app_status::AS_CREATING, false, ERR_BUSY_CREATING},
                 {old_app_id, APP_NAME, app_status::AS_RECALLING, false, ERR_BUSY_CREATING},
                 {old_app_id, APP_NAME, app_status::AS_DROPPING, false, ERR_BUSY_DROPPING},
                 {old_app_id, APP_NAME, app_status::AS_DROPPED, false, ERR_OK},
                 {old_app_id, APP_NAME, app_status::AS_DROPPED, true, ERR_APP_NOT_EXIST}};

    for (auto test : tests) {
        if (!test.is_totally_dropped) {
            if (test.before_status == app_status::AS_DROPPED) {
                old_app->status = app_status::AS_AVAILABLE;
                drop_app(OLD_APP_NAME);
            } else if (test.before_status != app_status::AS_INVALID) {
                old_app->status = test.before_status;
            }
        } else {
            drop_app_with_expired();
        }
        auto err = recall_app_test(test.new_app_name, test.app_id);
        ASSERT_EQ(err, test.expected_err);
    }
}

TEST_F(meta_app_operation_test, get_max_replica_count)
{
    const uint32_t partition_count = 4;
    create_app(APP_NAME, partition_count);

    // Test cases:
    // - get max_replica_count from a non-existent table
    // - get max_replica_count from an inconsistent table
    // - get max_replica_count successfully
    struct test_case
    {
        std::string app_name;
        error_code expected_err;
        int32_t expected_max_replica_count;
    } tests[] = {{"abc_xyz", ERR_APP_NOT_EXIST, 0},
                 {APP_NAME, ERR_INCONSISTENT_STATE, 0},
                 {APP_NAME, ERR_OK, 3}};

    for (const auto &test : tests) {
        std::cout << "test get_max_replica_count: "
                  << "app_name=" << test.app_name << ", expected_err=" << test.expected_err
                  << ", expected_max_replica_count=" << test.expected_max_replica_count
                  << std::endl;

        std::function<void()> recover_partition_max_replica_count = []() {};

        if (test.expected_err == ERR_INCONSISTENT_STATE) {
            auto partition_index = static_cast<int32_t>(random32(0, partition_count - 1));
            set_partition_max_replica_count(test.app_name, partition_index, 2);
            recover_partition_max_replica_count =
                [this, app_name = test.app_name, partition_index]() {
                    set_partition_max_replica_count(app_name, partition_index, 3);
                };
        }

        const auto resp = get_max_replica_count(test.app_name);
        ASSERT_EQ(test.expected_err, resp.err);
        ASSERT_EQ(test.expected_max_replica_count, resp.max_replica_count);

        recover_partition_max_replica_count();
    }
}

TEST_F(meta_app_operation_test, set_max_replica_count)
{
    const uint32_t partition_count = 4;
    create_app(APP_NAME, partition_count);

    // Test cases:
    // - set max_replica_count for a non-existent table
    // - set max_replica_count for an inconsistent table
    // - set with wrong max_replica_count (< 0)
    // - set with wrong max_replica_count (= 0)
    // - set with wrong max_replica_count (> max_allowed_replica_count > alive_node_count)
    // - set with wrong max_replica_count (> alive_node_count > max_allowed_replica_count)
    // - set with wrong max_replica_count (> alive_node_count = max_allowed_replica_count)
    // - set with wrong max_replica_count (= max_allowed_replica_count, and > alive_node_count)
    // - set with wrong max_replica_count (< max_allowed_replica_count, and > alive_node_count)
    // - set with wrong max_replica_count (= alive_node_count, and > max_allowed_replica_count)
    // - set with wrong max_replica_count (< alive_node_count, and > max_allowed_replica_count)
    // - set with wrong max_replica_count (< min_allowed_replica_count < alive_node_count)
    // - set with wrong max_replica_count (< alive_node_count < min_allowed_replica_count)
    // - set with wrong max_replica_count (< min_allowed_replica_count = alive_node_count)
    // - set with wrong max_replica_count (< min_allowed_replica_count, and > alive_node_count)
    // - set with wrong max_replica_count (< min_allowed_replica_count, and = alive_node_count)
    // - set with wrong max_replica_count (= min_allowed_replica_count, and > alive_node_count)
    // - cluster is freezed (alive_node_count = 0)
    // - cluster is freezed (alive_node_count = 1 < min_live_node_count_for_unfreeze)
    // - cluster is freezed (alive_node_count = 2 < min_live_node_count_for_unfreeze)
    // - request is rejected once there has been already an unfinished update
    // - increase with valid max_replica_count (= max_allowed_replica_count, and = alive_node_count)
    // - decrease with valid max_replica_count (= max_allowed_replica_count, and = alive_node_count)
    // - unchanged valid max_replica_count (= max_allowed_replica_count, and = alive_node_count)
    // - increase with valid max_replica_count (= max_allowed_replica_count, and < alive_node_count)
    // - decrease with valid max_replica_count (= max_allowed_replica_count, and < alive_node_count)
    // - unchanged valid max_replica_count (= max_allowed_replica_count, and < alive_node_count)
    // - increase with valid max_replica_count (< max_allowed_replica_count, and = alive_node_count)
    // - decrease with valid max_replica_count (< max_allowed_replica_count, and = alive_node_count)
    // - unchanged valid max_replica_count (< max_allowed_replica_count, and = alive_node_count)
    // - decrease with valid max_replica_count (< max_allowed_replica_count < alive_node_count)
    // - unchanged valid max_replica_count (< max_allowed_replica_count < alive_node_count)
    // - decrease with valid max_replica_count (< alive_node_count < max_allowed_replica_count)
    // - unchanged valid max_replica_count (< alive_node_count < max_allowed_replica_count)
    // - increase with valid max_replica_count (< alive_node_count = max_allowed_replica_count)
    // - decrease with valid max_replica_count (< alive_node_count = max_allowed_replica_count)
    // - unchanged valid max_replica_count (< alive_node_count = max_allowed_replica_count)
    // - increase with valid max_replica_count (= min_allowed_replica_count, and < alive_node_count)
    // - decrease with valid max_replica_count (= min_allowed_replica_count, and < alive_node_count)
    // - unchanged valid max_replica_count (= min_allowed_replica_count, and < alive_node_count)
    // - increase max_replica_count from 2 to 3
    // - increase max_replica_count from 1 to 3
    // - decrease max_replica_count from 3 to 1
    struct test_case
    {
        std::string app_name;
        int32_t expected_old_max_replica_count;
        int32_t initial_max_replica_count;
        int32_t new_max_replica_count;
        uint64_t min_live_node_count_for_unfreeze;
        int alive_node_count;
        int32_t min_allowed_replica_count;
        int32_t max_allowed_replica_count;
        std::string env;
        error_code expected_err;
    } tests[] = {{"abc_xyz", 0, 3, 3, 2, 3, 1, 3, "", ERR_APP_NOT_EXIST},
                 {APP_NAME, 0, 3, 3, 2, 3, 1, 3, "", ERR_INCONSISTENT_STATE},
                 {APP_NAME, 3, 3, -1, 2, 3, 1, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 3, 3, 0, 2, 3, 1, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 3, 1, 1, 1, 2, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 3, 1, 2, 1, 1, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 1, 1, 1, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 1, 1, 2, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 1, 1, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 2, 1, 1, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 3, 1, 1, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 2, 2, 1, 1, 3, 2, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 3, 3, 1, 1, 2, 3, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 3, 3, 2, 1, 3, 3, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 3, 3, 2, 1, 1, 3, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 3, 3, 2, 1, 2, 3, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 2, 2, 2, 1, 1, 2, 3, "", ERR_INVALID_PARAMETERS},
                 {APP_NAME, 1, 1, 2, 1, 0, 1, 3, "", ERR_STATE_FREEZED},
                 {APP_NAME, 1, 1, 2, 2, 1, 1, 3, "", ERR_STATE_FREEZED},
                 {APP_NAME, 1, 1, 2, 3, 2, 1, 3, "", ERR_STATE_FREEZED},
                 {APP_NAME, 2, 2, 3, 2, 3, 2, 3, "updating;3", ERR_OPERATION_DISABLED},
                 {APP_NAME, 1, 1, 2, 1, 2, 1, 2, "", ERR_OK},
                 {APP_NAME, 2, 2, 1, 1, 1, 1, 1, "", ERR_OK},
                 {APP_NAME, 2, 2, 2, 1, 2, 1, 2, "", ERR_OK},
                 {APP_NAME, 1, 1, 2, 1, 3, 1, 2, "", ERR_OK},
                 {APP_NAME, 2, 2, 1, 1, 2, 1, 1, "", ERR_OK},
                 {APP_NAME, 2, 2, 2, 1, 3, 1, 2, "", ERR_OK},
                 {APP_NAME, 1, 1, 2, 1, 2, 1, 3, "", ERR_OK},
                 {APP_NAME, 3, 3, 2, 1, 2, 1, 3, "", ERR_OK},
                 {APP_NAME, 2, 2, 2, 1, 2, 1, 3, "", ERR_OK},
                 {APP_NAME, 2, 2, 1, 1, 3, 1, 2, "", ERR_OK},
                 {APP_NAME, 1, 1, 1, 1, 3, 1, 2, "", ERR_OK},
                 {APP_NAME, 2, 2, 1, 1, 2, 1, 3, "", ERR_OK},
                 {APP_NAME, 1, 1, 1, 1, 2, 1, 3, "", ERR_OK},
                 {APP_NAME, 1, 1, 2, 1, 3, 1, 3, "", ERR_OK},
                 {APP_NAME, 3, 3, 2, 1, 3, 1, 3, "", ERR_OK},
                 {APP_NAME, 2, 2, 2, 1, 3, 1, 3, "", ERR_OK},
                 {APP_NAME, 1, 1, 2, 1, 3, 2, 3, "", ERR_OK},
                 {APP_NAME, 3, 3, 2, 1, 3, 2, 3, "", ERR_OK},
                 {APP_NAME, 2, 2, 2, 1, 3, 2, 3, "", ERR_OK},
                 {APP_NAME, 2, 2, 3, 2, 3, 2, 3, "", ERR_OK},
                 {APP_NAME, 1, 1, 3, 2, 3, 1, 3, "", ERR_OK},
                 {APP_NAME, 3, 3, 1, 2, 3, 1, 3, "", ERR_OK}};

    const int32_t total_node_count = 3;
    auto nodes = ensure_enough_alive_nodes(total_node_count);

    for (const auto &test : tests) {
        std::cout << "test set_max_replica_count: "
                  << "app_name=" << test.app_name
                  << ", expected_old_max_replica_count=" << test.expected_old_max_replica_count
                  << ", initial_max_replica_count=" << test.initial_max_replica_count
                  << ", new_max_replica_count=" << test.new_max_replica_count
                  << ", min_live_node_count_for_unfreeze=" << test.min_live_node_count_for_unfreeze
                  << ", alive_node_count=" << test.alive_node_count
                  << ", min_allowed_replica_count=" << test.min_allowed_replica_count
                  << ", max_allowed_replica_count=" << test.max_allowed_replica_count
                  << ", expected_err=" << test.expected_err << std::endl;

        // disable _node_live_percentage_threshold_for_update
        // for the reason that the meta function level will become freezed once
        // alive_nodes * 100 < total_nodes * _node_live_percentage_threshold_for_update
        // even if alive_nodes >= min_live_node_count_for_unfreeze
        set_node_live_percentage_threshold_for_update(0);

        if (test.expected_err != ERR_APP_NOT_EXIST) {
            set_max_replica_count_env(test.app_name, test.env);
            // set the initial max_replica_count for the app and all of its partitions
            set_app_and_all_partitions_max_replica_count(test.app_name,
                                                         test.initial_max_replica_count);

            const auto resp = get_max_replica_count(test.app_name);
            ASSERT_EQ(ERR_OK, resp.err);
            ASSERT_EQ(test.initial_max_replica_count, resp.max_replica_count);
        }

        // recover automatically the original FLAGS_min_live_node_count_for_unfreeze,
        // FLAGS_min_allowed_replica_count and FLAGS_max_allowed_replica_count
        auto recover = defer(
            [reserved_min_live_node_count_for_unfreeze = FLAGS_min_live_node_count_for_unfreeze,
             reserved_min_allowed_replica_count = FLAGS_min_allowed_replica_count,
             reserved_max_allowed_replica_count = FLAGS_max_allowed_replica_count]() {
                FLAGS_max_allowed_replica_count = reserved_max_allowed_replica_count;
                FLAGS_min_allowed_replica_count = reserved_min_allowed_replica_count;
                FLAGS_min_live_node_count_for_unfreeze = reserved_min_live_node_count_for_unfreeze;
            });
        FLAGS_min_live_node_count_for_unfreeze = test.min_live_node_count_for_unfreeze;
        FLAGS_min_allowed_replica_count = test.min_allowed_replica_count;
        FLAGS_max_allowed_replica_count = test.max_allowed_replica_count;

        // set some nodes unalive to match the expected number of alive ndoes
        CHECK_GE(total_node_count, test.alive_node_count);
        for (int i = 0; i < total_node_count - test.alive_node_count; i++) {
            _ms->set_node_state({nodes[i]}, false);
        }

        // choose and set a partition randomly with an inconsistent max_replica_count
        if (test.expected_err == ERR_INCONSISTENT_STATE) {
            auto partition_index = static_cast<int32_t>(random32(0, partition_count - 1));
            set_partition_max_replica_count(
                test.app_name, partition_index, test.initial_max_replica_count + 1);
        }

        const auto set_resp = set_max_replica_count(test.app_name, test.new_max_replica_count);
        ASSERT_EQ(set_resp.err, test.expected_err);
        ASSERT_EQ(set_resp.old_max_replica_count, test.expected_old_max_replica_count);
        if (test.expected_err == ERR_OK) {
            verify_all_partitions_max_replica_count(test.app_name, test.new_max_replica_count);
            verify_app_max_replica_count(test.app_name, test.new_max_replica_count);
        }

        const auto get_resp = get_max_replica_count(test.app_name);
        if (test.expected_err == ERR_APP_NOT_EXIST || test.expected_err == ERR_INCONSISTENT_STATE) {
            ASSERT_EQ(test.expected_err, get_resp.err);
        } else if (test.expected_err != ERR_OK) {
            ASSERT_EQ(ERR_OK, get_resp.err);
        }

        if (test.expected_err != ERR_OK) {
            ASSERT_EQ(test.expected_old_max_replica_count, get_resp.max_replica_count);
        }

        _ms->set_node_state(nodes, true);
    }
}

TEST_F(meta_app_operation_test, recover_from_max_replica_count_env)
{
    const uint32_t partition_count = 4;
    create_app(APP_NAME, partition_count);

    const int32_t new_max_replica_count = 5;
    const auto env = fmt::format("updating;{}", new_max_replica_count);
    set_max_replica_count_env(APP_NAME, env);

    _ss->recover_from_max_replica_count_env();

    verify_all_partitions_max_replica_count(APP_NAME, new_max_replica_count);
    verify_app_max_replica_count(APP_NAME, new_max_replica_count);
}

TEST_F(meta_app_operation_test, change_atomic_idempotent)
{
    create_app(APP_NAME, 4);

    // Initial `atomic_idempotent` should be false by default.
    test_get_atomic_idempotent(APP_NAME, false);

    // Enable `atomic_idempotent` and its previous value should be false.
    test_set_atomic_idempotent(APP_NAME, true, false);

    // Disable `atomic_idempotent` and its previous value should be true.
    test_set_atomic_idempotent(APP_NAME, false, true);
}

TEST_F(meta_app_operation_test, rename_app)
{
    const std::string app_name_1 = APP_NAME + "_rename_1";
    create_app(app_name_1);
    auto app = find_app(app_name_1);
    ASSERT_TRUE(app) << fmt::format("app({}) does not exist", app_name_1);
    auto app_id_1 = app->app_id;

    const std::string app_name_2 = APP_NAME + "_rename_2";
    create_app(app_name_2);
    app = find_app(app_name_2);
    ASSERT_TRUE(app) << fmt::format("app({}) does not exist", app_name_2);
    auto app_id_2 = app->app_id;

    // case 1: new_app_name table exist
    auto resp = rename_app(app_name_1, app_name_2);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, resp.err);

    const std::string app_name_3 = APP_NAME + "_rename_3";
    // case 2: old_app_name table not exist
    resp = rename_app(APP_NAME + "_rename_invaild", app_name_3);
    ASSERT_EQ(ERR_APP_NOT_EXIST, resp.err);

    // case 3: rename successful
    resp = rename_app(app_name_1, app_name_3);
    ASSERT_EQ(ERR_OK, resp.err);
    app = find_app(app_name_3);
    ASSERT_TRUE(app) << fmt::format("app({}) does not exist", app_name_3);
    ASSERT_EQ(app_id_1, app->app_id);
}
} // namespace replication
} // namespace dsn
