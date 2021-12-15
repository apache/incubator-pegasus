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

#include <dsn/dist/replication/replica_envs.h>

#include "meta_service_test_app.h"
#include "meta_test_base.h"

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
        auto request = dsn::make_unique<query_app_manual_compact_request>();
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

TEST_F(meta_app_compaction_test, test_query_compaction)
{
    struct test_case
    {
        int32_t mock_progress;
        error_code expected_err;
    } tests[] = {{-1, ERR_INVALID_STATE}, {0, ERR_OK}, {50, ERR_OK}, {100, ERR_OK}};

    for (auto test : tests) {
        auto resp = query_manual_compaction(test.mock_progress);
        ASSERT_EQ(resp.err, test.expected_err);
        if (resp.err == ERR_OK) {
            ASSERT_EQ(resp.progress, test.mock_progress);
        }
    }
}

} // namespace replication
} // namespace dsn
