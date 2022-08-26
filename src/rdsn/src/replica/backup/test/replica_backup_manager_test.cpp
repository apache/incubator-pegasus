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

#include <dsn/utility/fail_point.h>
#include <gtest/gtest.h>

#include "replica/backup/replica_backup_manager.h"
#include "replica/test/replica_test_base.h"

namespace dsn {
namespace replication {

class replica_backup_manager_test : public replica_test_base
{
public:
    replica_backup_manager_test()
    {
        _replica = create_mock_replica(stub.get());
        _backup_mgr = make_unique<replica_backup_manager>(_replica.get());
        utils::filesystem::create_directory(LOCAL_BACKUP_DIR);
        fail::setup();
    }

    ~replica_backup_manager_test()
    {
        utils::filesystem::remove_path(LOCAL_BACKUP_DIR);
        utils::filesystem::remove_path(PATH);
        fail::teardown();
    }

    void generate_checkpoint() { _backup_mgr->generate_checkpoint(); }

    bool set_backup_metadata()
    {
        auto dir_name = create_local_backup_checkpoint_dir();
        create_local_backup_file(dir_name, FILE_NAME1);
        return _backup_mgr->set_backup_metadata_unlock(dir_name, DECREE, _backup_mgr->_backup_id);
    }

    void report_checkpointing(backup_response &response)
    {
        _backup_mgr->report_checkpointing(response);
    }

    void upload_checkpoint(const std::string &dir_name)
    {
        _backup_mgr->set_backup_metadata_unlock(dir_name, DECREE, _backup_mgr->_backup_id);
        _backup_mgr->upload_checkpoint(PROVIDER, PATH, APP_NAME);
        _backup_mgr->tracker()->wait_outstanding_tasks();
    }

    void report_uploading(backup_response &response) { _backup_mgr->report_uploading(response); }

    void mock_local_backup_states(backup_status::type status,
                                  error_code checkpoint_err = ERR_OK,
                                  error_code upload_err = ERR_OK,
                                  int32_t upload_file_size = 0)
    {
        _backup_mgr->_status = status;
        _backup_mgr->_backup_id = dsn_now_ms();
        _backup_mgr->_checkpoint_err = checkpoint_err;
        _backup_mgr->_upload_err = upload_err;
        _backup_mgr->_upload_file_size = upload_file_size;
        _backup_mgr->_backup_metadata.checkpoint_total_size = 100;
    }

    std::string create_local_backup_checkpoint_dir()
    {
        _backup_mgr->_backup_id = dsn_now_ms();
        auto dir = utils::filesystem::path_combine(LOCAL_BACKUP_DIR,
                                                   std::to_string(_backup_mgr->_backup_id));
        utils::filesystem::create_directory(dir);
        return dir;
    }

    void create_local_backup_file(const std::string &dir, const std::string &fname)
    {
        auto fpath = utils::filesystem::path_combine(dir, fname);
        utils::filesystem::create_file(fpath);
        std::string value = "test_value";
        utils::filesystem::write_file(fpath, value);
    }

    backup_status::type get_status() { return _backup_mgr->_status; }

    error_code get_checkpoint_err() { return _backup_mgr->_checkpoint_err; }

    int64_t get_backup_id() { return _backup_mgr->_backup_id; }

    bool is_upload_finished()
    {
        return _backup_mgr->_backup_metadata.checkpoint_total_size ==
               _backup_mgr->_upload_file_size;
    }

    error_code get_upload_err() { return _backup_mgr->_upload_err; }

protected:
    const std::string LOCAL_BACKUP_DIR = "backup";
    const std::string APP_NAME = "backup_test";
    const std::string PROVIDER = "local_service";
    const std::string PATH = "unit_test";
    const int64_t DECREE = 5;
    const std::string FILE_NAME1 = "test_file1";
    const std::string FILE_NAME2 = "test_file2";
    std::unique_ptr<replica_backup_manager> _backup_mgr;
};

// TODO(heyuchen): add unit test for on_backup after implement all status

TEST_F(replica_backup_manager_test, generate_checkpoint_test)
{
    fail::cfg("replica_set_backup_metadata", "return()");
    mock_local_backup_states(backup_status::CHECKPOINTING);
    generate_checkpoint();
    ASSERT_EQ(get_checkpoint_err(), ERR_OK);
    ASSERT_EQ(get_status(), backup_status::CHECKPOINTED);
}

TEST_F(replica_backup_manager_test, set_backup_metadata_test)
{
    ASSERT_TRUE(set_backup_metadata());
}

TEST_F(replica_backup_manager_test, report_checkpointing_test)
{
    struct test_struct
    {
        backup_status::type status;
        error_code checkpoint_err;
    } tests[]{
        {backup_status::CHECKPOINTING, ERR_FILE_OPERATION_FAILED},
        {backup_status::CHECKPOINTING, ERR_WRONG_TIMING},
        {backup_status::CHECKPOINTING, ERR_LOCAL_APP_FAILURE},
        {backup_status::CHECKPOINTED, ERR_OK},
    };
    for (const auto &test : tests) {
        mock_local_backup_states(test.status, test.checkpoint_err);
        backup_response resp;
        report_checkpointing(resp);
        ASSERT_EQ(resp.status, test.status);
        ASSERT_EQ(resp.checkpoint_upload_err, test.checkpoint_err);
    }
}

TEST_F(replica_backup_manager_test, upload_checkpoint_test)
{
    auto dir_name = create_local_backup_checkpoint_dir();
    create_local_backup_file(dir_name, FILE_NAME1);
    create_local_backup_file(dir_name, FILE_NAME2);

    upload_checkpoint(dir_name);

    std::string remote_partition_dir =
        get_backup_partition_path(get_backup_root(FLAGS_cold_backup_root, PATH),
                                  APP_NAME,
                                  _replica->get_gpid().get_app_id(),
                                  get_backup_id(),
                                  _replica->get_gpid().get_partition_index());
    std::string remote_checkpoint_dir = get_backup_checkpoint_path(remote_partition_dir);
    ASSERT_TRUE(is_upload_finished());
    ASSERT_TRUE(utils::filesystem::file_exists(
        utils::filesystem::path_combine(remote_checkpoint_dir, FILE_NAME1)));
    ASSERT_TRUE(utils::filesystem::file_exists(
        utils::filesystem::path_combine(remote_checkpoint_dir, FILE_NAME2)));
    ASSERT_TRUE(utils::filesystem::file_exists(
        utils::filesystem::path_combine(remote_checkpoint_dir, backup_constant::BACKUP_METADATA)));
    ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(
        remote_partition_dir, backup_constant::CURRENT_CHECKPOINT)));
    ASSERT_TRUE(utils::filesystem::file_exists(
        utils::filesystem::path_combine(remote_partition_dir, backup_constant::DATA_VERSION)));
    ASSERT_EQ(get_status(), backup_status::SUCCEED);
}

TEST_F(replica_backup_manager_test, report_uploading_test)
{
    struct test_struct
    {
        backup_status::type status;
        error_code upload_err;
        bool is_response_upload_err_set;
        int32_t upload_file_size;
        int32_t expected_upload_progress;
    } tests[]{{backup_status::UPLOADING, ERR_FILE_OPERATION_FAILED, true, 0, 0},
              {backup_status::UPLOADING, ERR_IO_PENDING, false, 0, 0},
              {backup_status::UPLOADING, ERR_IO_PENDING, false, 40, 40},
              {backup_status::UPLOADING, ERR_IO_PENDING, false, 100, 100},
              {backup_status::UPLOADING, ERR_OK, false, 100, 100}};
    for (const auto &test : tests) {
        mock_local_backup_states(test.status, ERR_OK, test.upload_err, test.upload_file_size);
        backup_response resp;
        report_uploading(resp);
        ASSERT_EQ(resp.status, test.status);
        ASSERT_EQ(resp.__isset.checkpoint_upload_err, test.is_response_upload_err_set);
        if (test.is_response_upload_err_set) {
            ASSERT_EQ(resp.checkpoint_upload_err, test.upload_err);
        }
        ASSERT_EQ(resp.upload_progress, test.expected_upload_progress);
    }
}

} // namespace replication
} // namespace dsn
