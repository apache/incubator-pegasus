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

#include <gtest/gtest.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/service_api_c.h>

#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "meta/meta_split_service.h"

namespace dsn {
namespace replication {

DSN_DECLARE_uint64(min_live_node_count_for_unfreeze);
DSN_DECLARE_int32(min_allowed_replica_count);
DSN_DECLARE_int32(max_allowed_replica_count);

class meta_app_operation_test : public meta_test_base
{
public:
    meta_app_operation_test() {}

    error_code create_app_test(int32_t partition_count,
                               int32_t replica_count,
                               bool success_if_exist,
                               const std::string &app_name)
    {
        configuration_create_app_request create_request;
        configuration_create_app_response create_response;
        create_request.app_name = app_name;
        create_request.options.app_type = "simple_kv";
        create_request.options.partition_count = partition_count;
        create_request.options.replica_count = replica_count;
        create_request.options.success_if_exist = success_if_exist;
        create_request.options.is_stateful = true;

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
        // hold_seconds_for_dropped_app = 604800 in unit test config
        // make app expired immediatly
        app->expire_second -= 604800;
    }

    void clear_nodes() { _ss->_nodes.clear(); }

    const std::string APP_NAME = "app_operation_test";
    const std::string OLD_APP_NAME = "old_app_operation";
};

TEST_F(meta_app_operation_test, create_app)
{
    // Test cases: (assert min_allowed_replica_count <= max_allowed_replica_count)
    // - wrong partition_count (< 0)
    // - wrong partition_count (= 0)
    // - wrong replica_count (< 0)
    // - wrong replica_count (= 0)
    // - wrong replica_count (> max_allowed_replica_count > alive_node_count)
    // - wrong replica_count (> alive_node_count > max_allowed_replica_count)
    // - wrong replica_count (> alive_node_count = max_allowed_replica_count)
    // - wrong replica_count (= max_allowed_replica_count, and > alive_node_count)
    // - wrong replica_count (< max_allowed_replica_count, and > alive_node_count)
    // - wrong replica_count (= alive_node_count, and > max_allowed_replica_count)
    // - wrong replica_count (< alive_node_count, and > max_allowed_replica_count)
    // - valid replica_count (= max_allowed_replica_count, and = alive_node_count)
    // - valid replica_count (= max_allowed_replica_count, and < alive_node_count)
    // - valid replica_count (< max_allowed_replica_count, and = alive_node_count)
    // - valid replica_count (< max_allowed_replica_count < alive_node_count)
    // - valid replica_count (< alive_node_count < max_allowed_replica_count)
    // - valid replica_count (< alive_node_count = max_allowed_replica_count)
    // - wrong replica_count (< min_allowed_replica_count < alive_node_count)
    // - wrong replica_count (< alive_node_count < min_allowed_replica_count)
    // - wrong replica_count (< min_allowed_replica_count = alive_node_count)
    // - wrong replica_count (< min_allowed_replica_count, and > alive_node_count)
    // - wrong replica_count (< min_allowed_replica_count, and = alive_node_count)
    // - wrong replica_count (= min_allowed_replica_count, and > alive_node_count)
    // - valid replica_count (= min_allowed_replica_count, and < alive_node_count)
    // - cluster freezed (alive_node_count = 0)
    // - cluster freezed (alive_node_count = 1 < min_live_node_count_for_unfreeze)
    // - cluster freezed (alive_node_count = 2 < min_live_node_count_for_unfreeze)
    // - cluster not freezed (alive_node_count = min_live_node_count_for_unfreeze)
    // - create succeed with single-replica
    // - create succeed with double-replica
    // - create app succeed
    // - create failed with table existed
    // - wrong app_status creating
    // - wrong app_status recalling
    // - wrong app_status dropping
    // - create succeed with app_status dropped
    // - create succeed with success_if_exist=true
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
    } tests[] = {{APP_NAME, -1, 3, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 0, 3, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, -1, 1, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 0, 1, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 6, 2, 4, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 7, 2, 6, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 6, 2, 5, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 5, 2, 4, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 4, 2, 3, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 6, 2, 6, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 6, 2, 7, 1, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME + "_1", 4, 5, 2, 5, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_2", 4, 5, 2, 6, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_3", 4, 4, 2, 4, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_4", 4, 4, 2, 6, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_5", 4, 3, 2, 4, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_6", 4, 4, 2, 5, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME, 4, 3, 2, 5, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 3, 2, 4, 5, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 3, 2, 4, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 3, 2, 2, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 3, 2, 3, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME, 4, 4, 2, 3, 4, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
                 {APP_NAME + "_7", 4, 3, 2, 4, 3, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME, 4, 1, 1, 0, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
                 {APP_NAME, 4, 2, 2, 1, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
                 {APP_NAME, 4, 3, 3, 2, 1, false, app_status::AS_INVALID, ERR_STATE_FREEZED},
                 {APP_NAME + "_8", 4, 3, 3, 3, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_9", 4, 1, 1, 1, 1, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME + "_10", 4, 2, 1, 2, 2, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_INVALID, ERR_OK},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_INVALID, ERR_APP_EXIST},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_CREATING, ERR_BUSY_CREATING},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_RECALLING, ERR_BUSY_CREATING},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_DROPPING, ERR_BUSY_DROPPING},
                 {APP_NAME, 4, 3, 2, 3, 3, false, app_status::AS_DROPPED, ERR_OK},
                 {APP_NAME, 4, 3, 2, 3, 3, true, app_status::AS_INVALID, ERR_OK}};

    clear_nodes();

    // keep the number of all nodes greater than that of alive nodes
    const int total_node_count = 10;
    std::vector<rpc_address> nodes = ensure_enough_alive_nodes(total_node_count);

    // the meta function level will become freezed once
    // alive_nodes * 100 < total_nodes * node_live_percentage_threshold_for_update
    // even if alive_nodes >= min_live_node_count_for_unfreeze
    set_node_live_percentage_threshold_for_update(0);

    // save original FLAGS_min_live_node_count_for_unfreeze
    auto reserved_min_live_node_count_for_unfreeze = FLAGS_min_live_node_count_for_unfreeze;

    // save original FLAGS_max_allowed_replica_count
    auto reserved_max_allowed_replica_count = FLAGS_max_allowed_replica_count;

    // keep FLAGS_max_allowed_replica_count fixed in the tests
    auto res = update_flag("max_allowed_replica_count", "5");
    ASSERT_TRUE(res.is_ok());

    // save original FLAGS_min_allowed_replica_count
    auto reserved_min_allowed_replica_count = FLAGS_min_allowed_replica_count;

    for (auto test : tests) {
        res = update_flag("min_allowed_replica_count",
                          std::to_string(test.min_allowed_replica_count));
        ASSERT_TRUE(res.is_ok());

        set_min_live_node_count_for_unfreeze(test.min_live_node_count_for_unfreeze);

        dassert_f(total_node_count >= test.alive_node_count,
                  "total_node_count({}) should be >= alive_node_count({})",
                  total_node_count,
                  test.alive_node_count);
        for (int i = 0; i < total_node_count - test.alive_node_count; i++) {
            _ms->set_node_state({nodes[i]}, false);
        }

        if (test.before_status == app_status::AS_DROPPED) {
            update_app_status(app_status::AS_AVAILABLE);
            drop_app(APP_NAME);
        } else if (test.before_status != app_status::AS_INVALID) {
            update_app_status(test.before_status);
        }
        auto err = create_app_test(
            test.partition_count, test.replica_count, test.success_if_exist, test.app_name);
        ASSERT_EQ(err, test.expected_err);

        _ms->set_node_state(nodes, true);
    }

    // set FLAGS_min_allowed_replica_count successfully
    res = update_flag("min_allowed_replica_count", "2");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_min_allowed_replica_count, 2);

    // set FLAGS_max_allowed_replica_count successfully
    res = update_flag("max_allowed_replica_count", "6");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_max_allowed_replica_count, 6);

    // failed to set FLAGS_min_allowed_replica_count due to individual validation
    res = update_flag("min_allowed_replica_count", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_min_allowed_replica_count, 2);
    std::cout << res.description() << std::endl;

    // failed to set FLAGS_max_allowed_replica_count due to individual validation
    res = update_flag("max_allowed_replica_count", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_max_allowed_replica_count, 6);
    std::cout << res.description() << std::endl;

    // failed to set FLAGS_min_allowed_replica_count due to grouped validation
    res = update_flag("min_allowed_replica_count", "7");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_min_allowed_replica_count, 2);
    std::cout << res.description() << std::endl;

    // failed to set FLAGS_max_allowed_replica_count due to grouped validation
    res = update_flag("max_allowed_replica_count", "1");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_max_allowed_replica_count, 6);
    std::cout << res.description() << std::endl;

    // recover original FLAGS_min_allowed_replica_count
    res = update_flag("min_allowed_replica_count",
                      std::to_string(reserved_min_allowed_replica_count));
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_min_allowed_replica_count, reserved_min_allowed_replica_count);

    // recover original FLAGS_max_allowed_replica_count
    res = update_flag("max_allowed_replica_count",
                      std::to_string(reserved_max_allowed_replica_count));
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_max_allowed_replica_count, reserved_max_allowed_replica_count);

    // recover original FLAGS_min_live_node_count_for_unfreeze
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

} // namespace replication
} // namespace dsn
