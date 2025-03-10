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

#include <atomic>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "backup_block_service_mock.h"
#include "backup_types.h"
#include "block_service/block_service.h"
#include "block_service/test/block_service_mock.h"
#include "common/backup_common.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "gtest/gtest.h"
#include "metadata_types.h"
#include "replica/backup/cold_backup_context.h"
#include "replica/test/replication_service_test_app.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/filesystem.h"

ref_ptr<block_file_mock> current_chkpt_file = new block_file_mock("", 0, "");
ref_ptr<block_file_mock> backup_metadata_file = new block_file_mock("", 0, "");
ref_ptr<block_file_mock> regular_file = new block_file_mock("", 0, "");

static std::string backup_root = "root";
static backup_request request;
static int32_t concurrent_uploading_file_cnt = 1;
std::shared_ptr<backup_block_service_mock> block_service =
    std::make_shared<backup_block_service_mock>();

void replication_service_test_app::check_backup_on_remote_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    // case1 : current_chkpt_file don't exist
    {
        std::cout << "testing current_chkpt_file don't exist..." << std::endl;
        backup_context->check_backup_on_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupChecked);
    }
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    // case2 : create current_chkpt_file fail
    {
        std::cout << "testing create current_chkpt_file fail..." << std::endl;
        block_service->enable_create_file_fail = true;
        std::cout << "ref_counter = " << backup_context->get_count() << std::endl;
        backup_context->check_backup_on_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        block_service->enable_create_file_fail = false;
        std::cout << "ref_counter = " << backup_context->get_count() << std::endl;
    }
    // case3 : current_chkpt_file exist
    // this case will call read_current_chkpt_file, so we make read_current_chkpt_file fail to stop
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    {
        std::cout << "testing read current_chkpt_file fail..." << std::endl;
        current_chkpt_file->enable_read_fail = true;
        // so current_chkpt_file must exit
        current_chkpt_file->file_exist("123", 123);
        backup_context->check_backup_on_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        current_chkpt_file->enable_read_fail = false;
        current_chkpt_file->clear_file_exist();
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::read_current_chkpt_file_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    // read current_chkpt_file fail has been already tested in check_backup_on_remote_test()
    // case1: current_chkpt_file is not exist
    {
        std::cout << "testing read_current_chkpt_file(file not exist)..." << std::endl;
        current_chkpt_file->clear_file_exist();
        block_file_ptr file_handle = current_chkpt_file.get();
        backup_context->read_current_chkpt_file(file_handle);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupChecked);
    }
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    // case2: current_chkpt_file exist
    //  this case will call remote_chkpt_dir_exist(), so we make list_dir fail to stop
    {
        std::cout
            << "testing read_current_chkpt_file(file exist and check whether chkpt_dir is exist)..."
            << std::endl;
        current_chkpt_file->file_exist("123", 10);
        current_chkpt_file->set_context("test_dir");
        block_service->enable_list_dir_fail = true;
        block_file_ptr file_handle = current_chkpt_file.get();
        backup_context->read_current_chkpt_file(file_handle);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        current_chkpt_file->clear_file_exist();
        current_chkpt_file->clear_context();
        block_service->enable_list_dir_fail = false;
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::remote_chkpt_dir_exist_test()
{
    gpid mock_gpid(1, 2);
    std::string mock_app_name("mock_app");
    int64_t mock_backup_id(1000);
    std::string mock_backup_provider_name("mock_backup_provider_name");
    request.__set_pid(mock_gpid);
    request.__set_app_name(mock_app_name);
    request.__set_backup_id(mock_backup_id);
    policy_info mock_policy_info;
    mock_policy_info.__set_backup_provider_type("mock_service");
    mock_policy_info.__set_policy_name("mock_policy");
    request.__set_policy(mock_policy_info);
    // the case that list_dir fail has been already tested
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);

    // case1: directory is exist
    {
        std::cout << "testing remote checkpoint directory is exist..." << std::endl;
        std::string dir_name = std::string("test_dir");
        current_chkpt_file->file_exist("123", 10);
        current_chkpt_file->set_context(dir_name);

        std::string parent_dir = cold_backup::get_replica_backup_path(
            backup_root, mock_app_name, mock_gpid, mock_backup_id);

        std::vector<ls_entry> entries;
        entries.emplace_back(ls_entry{std::string(dir_name), true});
        // remote_chkpt_dir_exist() function judge whether the dir-A is exist through listing
        //      the dir-A's parent path
        block_service->dir_files.insert(
            std::make_pair(::dsn::utils::filesystem::get_file_name(parent_dir), entries));
        backup_context->remote_chkpt_dir_exist(dir_name);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupCompleted);
        current_chkpt_file->clear_file_exist();
        current_chkpt_file->clear_context();
        block_service->dir_files.clear();
    }
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    // case2: directory is not exist
    {
        std::cout << "testing remote checkpoint directory is not exist..." << std::endl;
        std::string dir_name = std::string("test_dir");
        current_chkpt_file->file_exist("123", 10);
        current_chkpt_file->set_context(dir_name);
        backup_context->remote_chkpt_dir_exist(dir_name);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupChecked);
        current_chkpt_file->clear_file_exist();
        current_chkpt_file->clear_context();
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::upload_checkpoint_to_remote_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    backup_context->_status.store(cold_backup_status::ColdBackupChecking);
    backup_context->_have_check_upload_status.store(false);
    backup_context->_upload_status.store(cold_backup_context::upload_status::UploadInvalid);
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case1: create metadata_file fail
    {
        std::cout << "testing upload_checkpoint_to_remote, stop with create metadata fail..."
                  << std::endl;
        block_service->enable_create_file_fail = true;
        backup_context->upload_checkpoint_to_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        block_service->enable_create_file_fail = false;
    }
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case2: create metadata succeed, and metadata is exist
    //  this case will stop when call read_backup_metadata with reason read file fail
    {
        std::cout << "testing upload_checkpoint_to_remote, stop with read metadata file fail..."
                  << std::endl;
        backup_context->_have_check_upload_status.store(false);
        backup_context->_upload_status.store(cold_backup_context::upload_status::UploadInvalid);
        std::string md5 = "test_md5";
        int64_t size = 10;
        backup_metadata_file->enable_read_fail = true;
        backup_metadata_file->file_exist(md5, size);
        backup_context->upload_checkpoint_to_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        backup_metadata_file->clear_file_exist();
        backup_metadata_file->enable_read_fail = false;
    }
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case3: create metadata succeed, but metadata is not exist
    //  this case will stop after call write_metadata_file with write fail
    {
        std::cout << "testing upload_chekpoint_to_remote, stop with create metadata file fail..."
                  << std::endl;
        backup_context->_have_check_upload_status.store(false);
        backup_context->_upload_status.store(cold_backup_context::upload_status::UploadInvalid);
        backup_metadata_file->enable_write_fail = true;
        backup_context->upload_checkpoint_to_remote();
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        backup_metadata_file->enable_write_fail = false;
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::read_backup_metadata_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;

    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case1: metadata is valid
    //  stop with create current_chkpt_file fail
    {
        std::cout << "testing read_backup_metadata_file, with context of metadata is valid..."
                  << std::endl;
        blob buf = ::json::json_forwarder<cold_backup_metadata>::encode(backup_context->_metadata);
        std::string context(buf.data(), buf.length());
        backup_metadata_file->set_context(context);
        backup_metadata_file->file_exist("test_md5", 10);
        block_service->enable_create_file_fail = true;
        ref_ptr<block_file> file_handle = backup_metadata_file.get();
        backup_context->read_backup_metadata(file_handle);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        block_service->enable_create_file_fail = false;
        backup_metadata_file->clear_context();
        backup_metadata_file->clear_file_exist();
    }
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case2: metadata is invalid
    //  stop with create current_chkpt_file fail
    {
        std::cout << "testing read_backup_metada_file, with context of metadata is invalid..."
                  << std::endl;
        backup_metadata_file->file_exist("test_md5", 10);
        backup_metadata_file->set_context("{\"key\":value\"");
        block_service->enable_create_file_fail = true;
        ref_ptr<block_file> file_handle = backup_metadata_file.get();
        backup_context->read_backup_metadata(file_handle);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        block_service->enable_create_file_fail = false;
        backup_metadata_file->clear_file_exist();
        backup_metadata_file->clear_context();
    }
    // case3: read metadata fail
    //  this case has been already tested before, here just ignore
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::on_upload_chkpt_dir_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);

    backup_context->_upload_status.store(cold_backup_context::upload_status::UploadUncomplete);
    backup_context->_max_concurrent_uploading_file_cnt = 2;
    // case1: empty checkpoint file has been already tested in read_backup_metadata
    //  so, here just ignore

    // case2: checkpoint file is not empty
    {
        std::cout << "testing on_upload_chkpt_dir with non-empty checkpoint files..." << std::endl;
        // smiulate some files, because file is not truly exist, so we must ignore testing
        // prepare_upload
        // TODO: find a better way to test prepare_upload
        std::string test_file1 = "test_file1";
        int32_t file1_size = 10;
        std::string file1_md5 = "test_file1_md5";

        std::string test_file2 = "test_file2";
        int32_t file2_size = 10;
        std::string file2_md5 = "test_file2_md5";

        backup_context->checkpoint_files.emplace_back(test_file1);
        backup_context->checkpoint_files.emplace_back(test_file2);

        {
            // should smiulate prepare_upload here
            backup_context->_file_remain_cnt = 2;

            file_meta f_meta;
            f_meta.name = test_file1;
            f_meta.md5 = file1_md5;
            f_meta.size = file1_size;
            backup_context->_file_status.insert(
                std::make_pair(test_file1, cold_backup_context::file_status::FileUploadUncomplete));
            backup_context->_file_infos.insert(
                std::make_pair(test_file1, std::make_pair(file1_size, file1_md5)));
            backup_context->checkpoint_file_total_size = file1_size;
            backup_context->_metadata.files.emplace_back(f_meta);

            f_meta.name = test_file2;
            f_meta.md5 = file2_md5;
            f_meta.size = file2_size;
            backup_context->_file_status.insert(
                std::make_pair(test_file2, cold_backup_context::file_status::FileUploadUncomplete));
            backup_context->_file_infos.insert(
                std::make_pair(test_file2, std::make_pair(file2_size, file2_md5)));
            backup_context->checkpoint_file_total_size += file2_size;
            backup_context->_metadata.files.emplace_back(f_meta);
        }

        backup_metadata_file->enable_write_fail = true;
        regular_file->size = 10;
        backup_context->on_upload_chkpt_dir();
        std::cout << cold_backup_status_to_string(backup_context->status()) << std::endl;
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        regular_file->clear_file_exist();
        backup_metadata_file->enable_write_fail = false;
        backup_context->_metadata.files.clear();
        backup_context->checkpoint_files.clear();
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::write_backup_metadata_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case1: create backup_metadata file fail
    //  this case has been already tested

    // case2: create backup_metadata file succeed, but write file fail
    //  this case has been already tested

    // case3: create backup_metadata file succeed, and write file succeed
    {
        std::cout << "create backup_metadata_file succeed, and write file succeed..." << std::endl;
        std::string test_file1 = "test_file1";
        std::string test_file2 = "test_file2";
        backup_context->_metadata.checkpoint_decree = 100;

        file_meta f_meta;
        f_meta.name = test_file1;
        f_meta.md5 = "test_file1_md5";
        f_meta.size = 10;
        backup_context->_metadata.files.emplace_back(f_meta);
        f_meta.name = test_file2;
        f_meta.md5 = "test_file2_md5";
        f_meta.size = 11;
        backup_context->_metadata.files.emplace_back(f_meta);

        blob result =
            ::json::json_forwarder<cold_backup_metadata>::encode(backup_context->_metadata);
        std::string value(result.data(), result.length());
        current_chkpt_file->enable_write_fail = true;
        backup_context->write_backup_metadata();
        std::string value_write(backup_metadata_file->context.data(),
                                backup_metadata_file->context.length());
        ASSERT_TRUE(result.data() != backup_metadata_file->context.data());
        ASSERT_TRUE(value == value_write);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupFailed);
        current_chkpt_file->enable_write_fail = false;
        backup_context->_metadata.files.clear();
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}

void replication_service_test_app::write_current_chkpt_file_test()
{
    cold_backup_context_ptr backup_context =
        new cold_backup_context(nullptr, request, concurrent_uploading_file_cnt);

    backup_context->start_check();
    backup_context->block_service = block_service.get();
    backup_context->backup_root = backup_root;
    backup_context->_status.store(cold_backup_status::ColdBackupUploading);
    // case1: create current_chkpt_file succeed, and write succeed
    {
        std::string value = "test write_current_chkpt_file";
        backup_context->write_current_chkpt_file(value);

        std::string result(current_chkpt_file->context.data(),
                           current_chkpt_file->context.length());
        ASSERT_TRUE(value == result);
        ASSERT_TRUE(backup_context->status() == cold_backup_status::ColdBackupCompleted);
        ASSERT_TRUE(backup_context->_progress.load() >= 1000);
    }
    ASSERT_TRUE(backup_context->get_count() == 1);
    ASSERT_TRUE(current_chkpt_file->get_count() == 1);
    ASSERT_TRUE(backup_metadata_file->get_count() == 1);
    ASSERT_TRUE(regular_file->get_count() == 1);
}
