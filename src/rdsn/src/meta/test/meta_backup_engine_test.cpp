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

#include "meta_test_base.h"
#include "meta/meta_backup_engine.h"

namespace dsn {
namespace replication {

class meta_backup_engine_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        create_app(APP_NAME, PARTITION_COUNT);
        std::shared_ptr<app_state> app = find_app(APP_NAME);
        _app_id = app->app_id;
        _backup_engine = std::make_shared<meta_backup_engine>(_ms.get(), false);
        fail::setup();
    }

    void TearDown() override
    {
        fail::teardown();
        meta_test_base::TearDown();
    }

    void init_backup(int32_t app_id, backup_status::type status = backup_status::UNINITIALIZED)
    {
        _backup_engine->init_backup(app_id, PARTITION_COUNT, APP_NAME, PROVIDER, PATH);
        auto item = _backup_engine->get_backup_item();
        item.end_time_ms = 0;
        item.status = status;
        _backup_engine->_cur_backup = item;
    }

    backup_status::type start(int32_t app_id, bool mock_write_file_succeed)
    {
        fail::setup();
        fail::cfg("meta_backup_engine_start", "return()");
        fail::cfg("meta_update_backup_item", "return()");
        if (mock_write_file_succeed) {
            fail::cfg("meta_write_backup_file", "return(ok)");
        } else {
            fail::cfg("meta_write_backup_file", "return(error)");
        }

        init_backup(app_id);
        _backup_engine->start();

        fail::teardown();
        return _backup_engine->get_backup_status();
    }

protected:
    const std::string APP_NAME = "backup_test";
    const int32_t PARTITION_COUNT = 8;
    const int32_t INDEX = 0;
    const std::string PROVIDER = "local_service";
    const std::string PATH = "unit_test";
    int32_t _app_id;
    std::shared_ptr<meta_backup_engine> _backup_engine;
};

TEST_F(meta_backup_engine_test, start_test)
{
    // Test cases:
    // - failed: app not existed
    // - failed: write app_info failed
    // - succeed
    struct start_test
    {
        int32_t app_id;
        bool mock_write_file_succeed;
        backup_status::type expected_status;
        bool in_progress;
    } tests[] = {{2333, true, backup_status::FAILED, false},
                 {_app_id, false, backup_status::FAILED, false},
                 {_app_id, true, backup_status::CHECKPOINTING, true}};
    for (const auto &test : tests) {
        auto status = start(test.app_id, test.mock_write_file_succeed);
        ASSERT_EQ(status, test.expected_status);
        ASSERT_EQ(_backup_engine->is_in_progress(), test.in_progress);
    }
}

} // namespace replication
} // namespace dsn
