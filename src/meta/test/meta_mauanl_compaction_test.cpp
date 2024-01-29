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

#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/manual_compact.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_data.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "meta_test_base.h"
#include "metadata_types.h"
#include "utils/error_code.h"

namespace dsn {
namespace replication {
class meta_app_compaction_test : public meta_test_base
{
public:
    meta_app_compaction_test() {}

    void SetUp() override
    {
        meta_test_base::SetUp();
        prepare();
    }

    void prepare()
    {
        create_app(APP_NAME, PARTITION_COUNT);
        auto app = find_app(APP_NAME);
        app->partitions.resize(PARTITION_COUNT);
        app->helpers->contexts.resize(PARTITION_COUNT);
        for (auto i = 0; i < PARTITION_COUNT; ++i) {
            serving_replica rep;
            rep.compact_status = manual_compaction_status::IDLE;
            std::vector<serving_replica> reps;
            reps.emplace_back(rep);
            reps.emplace_back(rep);
            reps.emplace_back(rep);
            app->helpers->contexts[i].serving = reps;
        }
    }

    error_code start_manual_compaction(std::string app_name,
                                       std::string disable_manual,
                                       bool bottommost = false,
                                       int32_t target_level = -1,
                                       int32_t running_count = 0)
    {
        if (app_name == APP_NAME) {
            auto app = find_app(app_name);
            app->envs[replica_envs::MANUAL_COMPACT_DISABLED] = disable_manual;
        }
        auto request = std::make_unique<start_app_manual_compact_request>();
        request->app_name = app_name;
        if (target_level != -1) {
            request->__set_target_level(target_level);
        }
        if (running_count != 0) {
            request->__set_max_running_count(running_count);
        }
        request->__set_bottommost(bottommost);

        start_manual_compact_rpc rpc(std::move(request), RPC_CM_START_MANUAL_COMPACT);
        _ss->on_start_manual_compact(rpc);
        _ss->wait_all_task();
        return rpc.response().err;
    }

    void check_after_start_compaction(std::string bottommost,
                                      int32_t target_level = -1,
                                      int32_t running_count = 0)
    {
        auto app = find_app(APP_NAME);
        if (app->envs.find(replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION) !=
            app->envs.end()) {
            ASSERT_EQ(app->envs[replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION],
                      bottommost);
        }
        if (app->envs.find(replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL) != app->envs.end()) {
            ASSERT_EQ(app->envs[replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL],
                      std::to_string(target_level));
        }
        if (running_count > 0 &&
            app->envs.find(replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT) !=
                app->envs.end()) {
            ASSERT_EQ(app->envs[replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT],
                      std::to_string(running_count));
        }
        for (auto &cc : app->helpers->contexts) {
            for (auto &r : cc.serving) {
                ASSERT_EQ(r.compact_status, manual_compaction_status::IDLE);
            }
        }
    }

    query_app_manual_compact_response query_manual_compaction(int32_t mock_progress)
    {
        manual_compaction_status::type status = manual_compaction_status::IDLE;
        if (mock_progress == 0) {
            status = manual_compaction_status::QUEUING;
        } else if (mock_progress == 100) {
            status = manual_compaction_status::FINISHED;
        }
        auto app = find_app(APP_NAME);
        app->helpers->reset_manual_compact_status();
        for (auto &cc : app->helpers->contexts) {
            for (auto &r : cc.serving) {
                r.compact_status = status;
            }
        }
        if (mock_progress == 50) {
            for (auto i = 0; i < PARTITION_COUNT / 2; i++) {
                auto &cc = app->helpers->contexts[i];
                for (auto &r : cc.serving) {
                    r.compact_status = manual_compaction_status::FINISHED;
                }
            }
        }
        auto request = std::make_unique<query_app_manual_compact_request>();
        request->app_name = APP_NAME;

        query_manual_compact_rpc rpc(std::move(request), RPC_CM_QUERY_MANUAL_COMPACT_STATUS);
        _ss->on_query_manual_compact_status(rpc);
        wait_all();
        return rpc.response();
    }

public:
    std::string APP_NAME = "manual_compaction_test";
    int32_t PARTITION_COUNT = 4;
};

TEST_F(meta_app_compaction_test, test_start_compaction)
{
    struct test_case
    {
        std::string app_name;
        std::string disable_compaction;
        bool bottommost;
        int32_t target_level;
        int32_t running_count;
        error_code expected_err;
        std::string expected_bottommost;
    } tests[] = {{"app_not_exist", "false", false, -1, 0, ERR_APP_NOT_EXIST, "skip"},
                 {APP_NAME, "true", false, -1, 0, ERR_OPERATION_DISABLED, "skip"},
                 {APP_NAME, "false", false, -5, 0, ERR_INVALID_PARAMETERS, "skip"},
                 {APP_NAME, "false", false, -1, -1, ERR_INVALID_PARAMETERS, "skip"},
                 {APP_NAME, "false", false, -1, 0, ERR_OK, "skip"},
                 {APP_NAME, "false", true, -1, 0, ERR_OK, "force"},
                 {APP_NAME, "false", false, 1, 0, ERR_OK, "skip"},
                 {APP_NAME, "false", true, -1, 1, ERR_OK, "force"}};

    for (const auto &test : tests) {
        auto err = start_manual_compaction(test.app_name,
                                           test.disable_compaction,
                                           test.bottommost,
                                           test.target_level,
                                           test.running_count);
        ASSERT_EQ(err, test.expected_err);
        if (err == ERR_OK) {
            check_after_start_compaction(
                test.expected_bottommost, test.target_level, test.running_count);
        }
    }
}

TEST_F(meta_app_compaction_test, test_query_compaction)
{
    struct test_case
    {
        int32_t mock_progress;
        error_code expected_err;
    } tests[] = {{-1, ERR_INVALID_STATE}, {0, ERR_OK}, {50, ERR_OK}, {100, ERR_OK}};

    for (const auto &test : tests) {
        auto resp = query_manual_compaction(test.mock_progress);
        ASSERT_EQ(resp.err, test.expected_err);
        if (resp.err == ERR_OK) {
            ASSERT_EQ(resp.progress, test.mock_progress);
        }
    }
}

} // namespace replication
} // namespace dsn
