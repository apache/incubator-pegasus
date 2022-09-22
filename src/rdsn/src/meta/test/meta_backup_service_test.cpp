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
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

#include "meta_test_base.h"
#include "meta_service_test_app.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_server_failure_detector.h"

namespace dsn {
namespace replication {

class meta_backup_service_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        fail::setup();
    }

    void TearDown() override
    {
        _onetime_backups.clear();
        fail::teardown();
        meta_test_base::TearDown();
    }

    void create_backup_app()
    {
        create_app(APP_NAME, PARTITION_COUNT);
        std::shared_ptr<app_state> app = find_app(APP_NAME);
        _app_id = app->app_id;
    }

    void mock_app_backup_context(bool onetime, bool periodic)
    {
        backup_svc()._onetime_backup_states.clear();

        if (!onetime && !periodic) {
            return;
        }
        if (onetime) {
            auto engine = std::make_shared<meta_backup_engine>(_ms.get(), false);
            engine->init_backup(_app_id, PARTITION_COUNT, APP_NAME, PROVIDER, PATH);
            backup_svc()._onetime_backup_states.emplace_back(engine);
        }
        if (periodic) {
            auto engine = std::make_shared<meta_backup_engine>(_ms.get(), true);
            engine->init_backup(_app_id, PARTITION_COUNT, APP_NAME, PROVIDER, PATH);
            // TODO(heyuchen): TBD
        }
    }

    error_code start_backup(const int32_t app_id, const std::string &provider)
    {
        auto request = dsn::make_unique<start_backup_app_request>();
        request->backup_provider_type = provider;
        request->app_id = app_id;
        request->__set_backup_path(PATH);

        start_backup_app_rpc rpc(std::move(request), RPC_CM_START_BACKUP_APP);
        backup_svc().start_backup_app(rpc);
        wait_all();
        return rpc.response().err;
    }

protected:
    const std::string APP_NAME = "backup_table";
    const std::string PROVIDER = "local_service";
    const std::string PATH = "unit_test";
    const int32_t PARTITION_COUNT = 8;
    int32_t _app_id;

    std::vector<backup_item> _onetime_backups;
};

TEST_F(meta_backup_service_test, start_backup_test)
{
    fail::cfg("meta_create_onetime_backup", "return()");
    create_backup_app();

    // Test cases:
    // - wrong app id
    // - onetime backup is executing
    // - TODO(heyuchen): periodic backup is executing
    // - wrong provider name
    // - succeed
    struct start_backup_test
    {
        int32_t app_id;
        bool mock_onetime;
        bool mock_periodic;
        std::string provider_name;
        error_code expected_err;
    } tests[] = {{233, false, false, PROVIDER, ERR_APP_NOT_EXIST},
                 {_app_id, true, false, PROVIDER, ERR_INVALID_STATE},
                 /*{_app_id, false, true, PROVIDER, ERR_INVALID_STATE},*/
                 {_app_id, false, false, "wrong_provider", ERR_INVALID_PARAMETERS},
                 {_app_id, false, false, PROVIDER, ERR_OK}};
    for (const auto &test : tests) {
        mock_app_backup_context(test.mock_onetime, test.mock_periodic);
        ASSERT_EQ(start_backup(test.app_id, test.provider_name), test.expected_err) << fmt::format(
            "Test case params: app_id({}), provider({}), mock_onetime={}, mock_periodic={}",
            test.app_id,
            test.provider_name,
            test.mock_onetime,
            test.mock_periodic);
    }
}

} // namespace replication
} // namespace dsn
