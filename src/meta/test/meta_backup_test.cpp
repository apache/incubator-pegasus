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

#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include <gtest/gtest.h>

#include "common/backup_common.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta_test_base.h"

namespace dsn {
namespace replication {

class backup_service_test : public meta_test_base
{
public:
    backup_service_test()
        : _policy_root("test_policy_root"),
          _backup_root("test_backup_root"),
          _app_name("test_app"),
          _backup_service(nullptr)
    {
    }

    void SetUp() override
    {
        meta_test_base::SetUp();
        _ms->_backup_handler =
            std::make_shared<backup_service>(_ms.get(), _policy_root, _backup_root, nullptr);
        _backup_service = _ms->_backup_handler;

        // create an app with 8 partitions.
        create_app(_app_name);
    }

    start_backup_app_response
    start_backup(int32_t app_id, const std::string &provider, const std::string &backup_path = "")
    {
        auto request = dsn::make_unique<start_backup_app_request>();
        request->app_id = app_id;
        request->backup_provider_type = provider;
        if (!backup_path.empty()) {
            request->__set_backup_path(backup_path);
        }

        start_backup_app_rpc rpc(std::move(request), RPC_CM_START_BACKUP_APP);
        _backup_service->start_backup_app(rpc);
        wait_all();
        return rpc.response();
    }

    query_backup_status_response query_backup(int32_t app_id, int64_t backup_id)
    {
        auto request = dsn::make_unique<query_backup_status_request>();
        request->app_id = app_id;
        request->__isset.backup_id = true;
        request->backup_id = backup_id;

        query_backup_status_rpc rpc(std::move(request), RPC_CM_QUERY_BACKUP_STATUS);
        _backup_service->query_backup_status(rpc);
        wait_all();
        return rpc.response();
    }

    bool write_metadata_succeed(int32_t app_id,
                                int64_t backup_id,
                                const std::string &user_specified_path)
    {
        std::string backup_root = dsn::utils::filesystem::path_combine(
            user_specified_path, _backup_service->backup_root());
        auto app = _ms->_state->get_app(app_id);
        std::string metadata_file =
            cold_backup::get_app_metadata_file(backup_root, app->app_name, app_id, backup_id);

        int64_t metadata_file_size = 0;
        if (!dsn::utils::filesystem::file_size(metadata_file, metadata_file_size)) {
            return false;
        }
        return metadata_file_size > 0;
    }

    void test_specific_backup_path(int32_t test_app_id, const std::string &user_specified_path = "")
    {
        auto resp = start_backup(test_app_id, "local_service_empty_root", user_specified_path);
        ASSERT_EQ(ERR_OK, resp.err);
        ASSERT_TRUE(resp.__isset.backup_id);
        ASSERT_EQ(1, _backup_service->_backup_states.size());

        auto backup_engine = _backup_service->_backup_states[0];
        if (user_specified_path.empty()) {
            ASSERT_TRUE(backup_engine->_backup_path.empty());
        } else {
            ASSERT_EQ(user_specified_path, backup_engine->_backup_path);
        }

        int64_t backup_id = resp.backup_id;
        ASSERT_TRUE(write_metadata_succeed(test_app_id, backup_id, user_specified_path));
    }

protected:
    const std::string _policy_root;
    const std::string _backup_root;
    const std::string _app_name;
    std::shared_ptr<backup_service> _backup_service;
};

TEST_F(backup_service_test, test_invalid_backup_request)
{
    // invalid app id.
    int32_t test_app_id = _ss->next_app_id();
    auto resp = start_backup(test_app_id, "local_service");
    ASSERT_EQ(ERR_INVALID_STATE, resp.err);

    // invalid provider.
    resp = start_backup(1, "invalid_provider");
    ASSERT_EQ(ERR_INVALID_PARAMETERS, resp.err);
}

TEST_F(backup_service_test, test_init_backup)
{
    int64_t now = dsn_now_ms();
    auto resp = start_backup(1, "local_service");
    ASSERT_EQ(ERR_OK, resp.err);
    ASSERT_LE(now, resp.backup_id);
    ASSERT_EQ(1, _backup_service->_backup_states.size());

    // backup for app 1 is running, couldn't backup it again.
    resp = start_backup(1, "local_service");
    ASSERT_EQ(ERR_INVALID_STATE, resp.err);

    resp = start_backup(2, "local_service");
    ASSERT_EQ(ERR_OK, resp.err);
}

TEST_F(backup_service_test, test_write_backup_metadata_failed)
{
    fail::setup();
    fail::cfg("mock_local_service_write_failed", "100%1*return(ERR_FS_INTERNAL)");

    // we couldn't start backup an app if write backup metadata failed.
    auto resp = start_backup(1, "local_service");
    ASSERT_EQ(ERR_FS_INTERNAL, resp.err);

    fail::teardown();
}

TEST_F(backup_service_test, test_backup_app_with_no_specific_path) { test_specific_backup_path(1); }

TEST_F(backup_service_test, test_backup_app_with_user_specified_path)
{
    test_specific_backup_path(1, "test/backup");
}

TEST_F(backup_service_test, test_query_backup_status)
{
    // query a backup that does not exist
    auto resp = query_backup(1, 1);
    ASSERT_EQ(ERR_INVALID_PARAMETERS, resp.err);

    auto start_backup_resp = start_backup(1, "local_service");
    ASSERT_EQ(ERR_OK, start_backup_resp.err);
    ASSERT_EQ(1, _backup_service->_backup_states.size());

    // query backup succeed
    int64_t backup_id = start_backup_resp.backup_id;
    resp = query_backup(1, backup_id);
    ASSERT_EQ(ERR_OK, resp.err);
    ASSERT_TRUE(resp.__isset.backup_items);
    ASSERT_EQ(1, resp.backup_items.size());
}

class backup_engine_test : public meta_test_base
{
public:
    backup_engine_test()
        : _policy_root("test_policy_root"),
          _backup_root("test_backup_root"),
          _app_name("test_app"),
          _app_id(1),
          _partition_count(8),
          _backup_engine(nullptr)
    {
    }

    void SetUp() override
    {
        meta_test_base::SetUp();
        _ms->_backup_handler =
            std::make_shared<backup_service>(_ms.get(), _policy_root, _backup_root, nullptr);
        _backup_engine = std::make_shared<backup_engine>(_ms->_backup_handler.get());
        _backup_engine->set_block_service("local_service");

        zauto_lock lock(_backup_engine->_lock);
        _backup_engine->_backup_status.clear();
        for (int i = 0; i < _partition_count; ++i) {
            _backup_engine->_backup_status.emplace(i, backup_status::UNALIVE);
        }
        _backup_engine->_cur_backup.app_id = _app_id;
        _backup_engine->_cur_backup.app_name = _app_name;
        _backup_engine->_cur_backup.backup_id = static_cast<int64_t>(dsn_now_ms());
        _backup_engine->_cur_backup.start_time_ms = _backup_engine->_cur_backup.backup_id;
    }

    void mock_backup_app_partitions()
    {
        zauto_lock l(_backup_engine->_lock);
        for (int i = 0; i < _partition_count; ++i) {
            _backup_engine->_backup_status[i] = backup_status::ALIVE;
        }
    }

    void mock_on_backup_reply(int32_t partition_index,
                              error_code rpc_err,
                              error_code resp_err,
                              int32_t progress)
    {
        gpid pid = gpid(_app_id, partition_index);
        rpc_address mock_primary_address = rpc_address("127.0.0.1", 10000 + partition_index);

        backup_response resp;
        resp.backup_id = _backup_engine->_cur_backup.backup_id;
        resp.pid = pid;
        resp.err = resp_err;
        resp.progress = progress;

        _backup_engine->on_backup_reply(rpc_err, resp, pid, mock_primary_address);
    }

    void mock_on_backup_reply_when_timeout(int32_t partition_index, error_code rpc_err)
    {
        gpid pid = gpid(_app_id, partition_index);
        rpc_address mock_primary_address = rpc_address("127.0.0.1", 10000 + partition_index);
        backup_response resp;
        _backup_engine->on_backup_reply(rpc_err, resp, pid, mock_primary_address);
    }

    bool is_backup_failed() const
    {
        zauto_lock l(_backup_engine->_lock);
        return _backup_engine->_is_backup_failed;
    }

    void reset_backup_engine()
    {
        zauto_lock l(_backup_engine->_lock);
        _backup_engine->_is_backup_failed = false;
    }

protected:
    const std::string _policy_root;
    const std::string _backup_root;
    const std::string _app_name;
    const int32_t _app_id;
    const int32_t _partition_count;
    std::shared_ptr<backup_engine> _backup_engine;
};

TEST_F(backup_engine_test, test_on_backup_reply)
{
    mock_backup_app_partitions();

    // recieve a rpc error
    mock_on_backup_reply(/*partition_index=*/0, ERR_NETWORK_FAILURE, ERR_BUSY, /*progress=*/0);
    ASSERT_TRUE(_backup_engine->is_in_progress());

    // recieve a backup finished response
    reset_backup_engine();
    mock_on_backup_reply(/*partition_index=*/1,
                         ERR_OK,
                         ERR_OK,
                         /*progress=*/cold_backup_constant::PROGRESS_FINISHED);
    ASSERT_TRUE(_backup_engine->is_in_progress());

    // receive a backup in-progress response
    reset_backup_engine();
    mock_on_backup_reply(/*partition_index=*/2, ERR_OK, ERR_BUSY, /*progress=*/0);
    ASSERT_TRUE(_backup_engine->is_in_progress());
    ASSERT_EQ(_backup_engine->_backup_status[2], backup_status::ALIVE);

    // if one partition fail, all backup plan will fail
    {
        // receive a backup failed response
        reset_backup_engine();
        mock_on_backup_reply(/*partition_index=*/3, ERR_OK, ERR_LOCAL_APP_FAILURE, /*progress=*/0);
        ASSERT_TRUE(is_backup_failed());

        // this backup is still a failure even received non-failure response
        mock_on_backup_reply(/*partition_index=*/4, ERR_OK, ERR_BUSY, /*progress=*/0);
        ASSERT_TRUE(is_backup_failed());

        mock_on_backup_reply(/*partition_index=*/5,
                             ERR_OK,
                             ERR_OK,
                             /*progress=*/cold_backup_constant::PROGRESS_FINISHED);
        ASSERT_TRUE(is_backup_failed());
    }

    // meta request is timeout
    reset_backup_engine();
    mock_on_backup_reply_when_timeout(/*partition_index=*/5, ERR_TIMEOUT);
    ASSERT_FALSE(is_backup_failed());
}

TEST_F(backup_engine_test, test_backup_completed)
{
    mock_backup_app_partitions();
    for (int i = 0; i < _partition_count; ++i) {
        mock_on_backup_reply(/*partition_index=*/i,
                             ERR_OK,
                             ERR_OK,
                             /*progress=*/cold_backup_constant::PROGRESS_FINISHED);
    }
    ASSERT_FALSE(is_backup_failed());
    ASSERT_LE(_backup_engine->_cur_backup.start_time_ms, _backup_engine->_cur_backup.end_time_ms);
}

TEST_F(backup_engine_test, test_write_backup_info_failed)
{
    fail::setup();
    fail::cfg("mock_local_service_write_failed", "100%1*return(ERR_FS_INTERNAL)");

    // finish all partitions backup but write backup info failed.
    mock_backup_app_partitions();
    for (int i = 0; i < _partition_count; ++i) {
        mock_on_backup_reply(/*partition_index=*/i,
                             ERR_OK,
                             ERR_OK,
                             /*progress=*/cold_backup_constant::PROGRESS_FINISHED);
    }
    ASSERT_TRUE(is_backup_failed());
    ASSERT_EQ(0, _backup_engine->_cur_backup.end_time_ms);

    fail::teardown();
}

} // namespace replication
} // namespace dsn
