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

    backup_status::type
    on_backup_reply(const backup_response &resp, backup_status::type meta_status, bool mock_all)
    {
        mock_backup_context(resp.status, meta_status, mock_all);
        _backup_engine->on_backup_reply(ERR_OK, resp, gpid(_app_id, INDEX), PRIMARY);
        return _backup_engine->get_backup_status();
    }

    void mock_backup_context(backup_status::type resp_status,
                             backup_status::type old_status,
                             bool mock_all)
    {
        auto other_status = backup_status::UNINITIALIZED;
        if (mock_all) {
            if (resp_status == backup_status::CHECKPOINTED) {
                other_status = backup_status::UPLOADING;
            }
            if (resp_status == backup_status::SUCCEED) {
                other_status = backup_status::SUCCEED;
            }
        }
        for (auto i = 0; i < PARTITION_COUNT; i++) {
            if (i == INDEX) {
                _backup_engine->_backup_status[i] = old_status;
            } else {
                _backup_engine->_backup_status[i] = other_status;
            }
        }
    }

    backup_response mock_backup_response(backup_status::type status,
                                         int64_t backup_id,
                                         error_code resp_err,
                                         error_code checkpoint_err,
                                         error_code upload_err,
                                         int32_t upload_progress)
    {
        backup_response resp;
        resp.err = resp_err;
        resp.pid = gpid(_app_id, INDEX);
        resp.backup_id = backup_id;
        resp.status = status;
        if (checkpoint_err != ERR_OK) {
            resp.__set_checkpoint_upload_err(checkpoint_err);
        }
        if (upload_err != ERR_OK) {
            resp.__set_checkpoint_upload_err(upload_err);
        }
        if (upload_progress > 0) {
            resp.__set_upload_progress(upload_progress);
        }
        return resp;
    }

protected:
    const std::string APP_NAME = "backup_test";
    const int32_t PARTITION_COUNT = 8;
    const int32_t INDEX = 0;
    const std::string PROVIDER = "local_service";
    const std::string PATH = "unit_test";
    const rpc_address PRIMARY = rpc_address("127.0.0.1", 10086);
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

TEST_F(meta_backup_engine_test, on_backup_reply_succeed_test)
{
    fail::cfg("meta_retry_backup", "return()");
    fail::cfg("meta_update_backup_item", "return()");

    struct on_backup_reply_test
    {
        backup_status::type resp_status;
        backup_status::type old_status;
        bool mock_all;
        backup_status::type expected_status;
        bool in_progress;
    } tests[] = {
        {backup_status::CHECKPOINTING,
         backup_status::CHECKPOINTING,
         false,
         backup_status::CHECKPOINTING,
         true},
        {backup_status::CHECKPOINTED,
         backup_status::CHECKPOINTING,
         false,
         backup_status::CHECKPOINTING,
         true},
        {backup_status::CHECKPOINTED,
         backup_status::CHECKPOINTING,
         true,
         backup_status::UPLOADING,
         true},
        // TODO(heyuchen): add other status cases
    };

    for (const auto &test : tests) {
        init_backup(_app_id, test.old_status);
        auto resp = mock_backup_response(
            test.resp_status, _backup_engine->get_backup_id(), ERR_OK, ERR_OK, ERR_OK, 0);
        auto status = on_backup_reply(resp, test.old_status, test.mock_all);
        ASSERT_EQ(status, test.expected_status);
        ASSERT_EQ(_backup_engine->is_in_progress(), test.in_progress);
    }
}

TEST_F(meta_backup_engine_test, on_backup_reply_failed_test)
{
    fail::cfg("meta_retry_backup", "return()");
    fail::cfg("meta_update_backup_item", "return()");

    // case1. response error
    {
        init_backup(_app_id, backup_status::CHECKPOINTING);
        auto resp = mock_backup_response(backup_status::CHECKPOINTED,
                                         _backup_engine->get_backup_id(),
                                         ERR_INVALID_STATE,
                                         ERR_OK,
                                         ERR_OK,
                                         0);
        auto status = on_backup_reply(resp, backup_status::CHECKPOINTING, false);
        ASSERT_EQ(status, backup_status::FAILED);
        ASSERT_FALSE(_backup_engine->is_in_progress());
    }

    // case2. old backup_id
    {
        init_backup(_app_id, backup_status::UPLOADING);
        auto resp = mock_backup_response(backup_status::UPLOADING, 0, ERR_OK, ERR_OK, ERR_OK, 0);
        auto status = on_backup_reply(resp, backup_status::UPLOADING, false);
        ASSERT_EQ(status, backup_status::UPLOADING);
        ASSERT_TRUE(_backup_engine->is_in_progress());
    }

    // case3. checkpoint error
    {
        init_backup(_app_id, backup_status::CHECKPOINTING);
        auto resp = mock_backup_response(backup_status::CHECKPOINTING,
                                         _backup_engine->get_backup_id(),
                                         ERR_OK,
                                         ERR_FILE_OPERATION_FAILED,
                                         ERR_OK,
                                         0);
        auto status = on_backup_reply(resp, backup_status::CHECKPOINTING, true);
        ASSERT_EQ(status, backup_status::FAILED);
        ASSERT_FALSE(_backup_engine->is_in_progress());
    }

    // case4. upload error
    {
        init_backup(_app_id, backup_status::UPLOADING);
        auto resp = mock_backup_response(backup_status::UPLOADING,
                                         _backup_engine->get_backup_id(),
                                         ERR_OK,
                                         ERR_OK,
                                         ERR_FS_INTERNAL,
                                         0);
        auto status = on_backup_reply(resp, backup_status::UPLOADING, false);
        ASSERT_EQ(status, backup_status::FAILED);
        ASSERT_FALSE(_backup_engine->is_in_progress());
    }
}

} // namespace replication
} // namespace dsn
