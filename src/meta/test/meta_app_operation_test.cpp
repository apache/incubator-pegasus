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
#include <dsn/service_api_c.h>

#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "meta/meta_split_service.h"

namespace dsn {
namespace replication {
class meta_app_operation_test : public meta_test_base
{
public:
    meta_app_operation_test() {}

    error_code
    create_app_test(int32_t partition_count, int32_t replica_count, bool success_if_exist)
    {
        configuration_create_app_request create_request;
        configuration_create_app_response create_response;
        create_request.app_name = APP_NAME;
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

    const std::string APP_NAME = "app_operation_test";
    const std::string OLD_APP_NAME = "old_app_operation";
    const int32_t PARTITION_COUNT = 4;
    const int32_t REPLICA_COUNT = 3;
};

TEST_F(meta_app_operation_test, create_app)
{
    // Test cases:
    // - create app succeed
    // - wrong partition_count
    // - wrong replica_count
    // - create failed with table existed
    // - wrong app_status creating
    // - wrong app_status recalling
    // - wrong app_status dropping
    // - create succeed with app_status dropped
    // - create succeed with success_if_exist=true
    struct create_test
    {
        int32_t partition_count;
        int32_t replica_count;
        bool success_if_exist;
        app_status::type before_status;
        error_code expected_err;
    } tests[] = {
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_INVALID, ERR_OK},
        {0, REPLICA_COUNT, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        {PARTITION_COUNT, 0, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_INVALID, ERR_INVALID_PARAMETERS},
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_CREATING, ERR_BUSY_CREATING},
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_RECALLING, ERR_BUSY_CREATING},
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_DROPPING, ERR_BUSY_DROPPING},
        {PARTITION_COUNT, REPLICA_COUNT, false, app_status::AS_DROPPED, ERR_OK},
        {PARTITION_COUNT, REPLICA_COUNT, true, app_status::AS_INVALID, ERR_OK}};

    for (auto test : tests) {
        if (test.before_status == app_status::AS_DROPPED) {
            update_app_status(app_status::AS_AVAILABLE);
            drop_app(APP_NAME);
        } else if (test.before_status != app_status::AS_INVALID) {
            update_app_status(test.before_status);
        }
        auto err = create_app_test(test.partition_count, test.replica_count, test.success_if_exist);
        ASSERT_EQ(err, test.expected_err);
    }
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
