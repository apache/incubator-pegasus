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

#include <fmt/core.h>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "block_service/block_service.h"
#include "block_service/hdfs/hdfs_service.h"
#include "gtest/gtest.h"
#include "runtime/api_layer1.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/enum_helper.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/test_macros.h"
#include "utils/threadpool_code.h"

using namespace dsn;
using namespace dsn::dist::block_service;

DSN_DEFINE_string(hdfs_test, test_name_node, "", "hdfs name node");
DSN_DEFINE_string(hdfs_test, test_backup_path, "", "path for uploading and downloading test files");

DSN_DEFINE_uint32(hdfs_test, num_test_file_lines, 4096, "number of lines in test file");
DSN_DEFINE_uint32(hdfs_test,
                  num_total_files_for_hdfs_concurrent_test,
                  64,
                  "number of total files for hdfs concurrent test");

DEFINE_TASK_CODE(LPC_TEST_HDFS, TASK_PRIORITY_HIGH, dsn::THREAD_POOL_DEFAULT)

class HDFSClientTest : public pegasus::encrypt_data_test_base
{
protected:
    HDFSClientTest() : pegasus::encrypt_data_test_base()
    {
        for (int i = 0; i < FLAGS_num_test_file_lines; ++i) {
            _local_test_data += "test";
        }
    }

    void generate_test_file(const std::string &filename);
    void write_test_files_async(const std::string &local_test_path,
                                int total_files,
                                int *success_count,
                                task_tracker *tracker);

private:
    std::string _local_test_data;
};

void HDFSClientTest::generate_test_file(const std::string &filename)
{
    int lines = FLAGS_num_test_file_lines;
    std::unique_ptr<rocksdb::WritableFile> wfile;
    auto s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
                 ->NewWritableFile(filename, &wfile, rocksdb::EnvOptions());
    ASSERT_TRUE(s.ok()) << s.ToString();
    for (int i = 0; i < lines; ++i) {
        rocksdb::Slice data(fmt::format("{:04}d_this_is_a_simple_test_file\n", i));
        s = wfile->Append(data);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    s = wfile->Fsync();
    ASSERT_TRUE(s.ok()) << s.ToString();
}

void HDFSClientTest::write_test_files_async(const std::string &local_test_path,
                                            int total_files,
                                            int *success_count,
                                            task_tracker *tracker)
{
    dsn::utils::filesystem::create_directory(local_test_path);
    for (int i = 0; i < total_files; ++i) {
        tasking::enqueue(LPC_TEST_HDFS, tracker, [this, &local_test_path, i, success_count]() {
            // mock the writing process in hdfs_file_object::download().
            std::string test_file_name = local_test_path + "/test_file_" + std::to_string(i);
            auto s = rocksdb::WriteStringToFile(rocksdb::Env::Default(),
                                                rocksdb::Slice(_local_test_data),
                                                test_file_name,
                                                /* should_sync */ true);
            if (s.ok()) {
                ++(*success_count);
            } else {
                CHECK(s.IsIOError(), "{}", s.ToString());
                auto pos1 = s.ToString().find(
                    "IO error: No such file or directory: While open a file for appending: ");
                auto pos2 = s.ToString().find("IO error: While appending to file: ");
                CHECK(pos1 == 0 || pos2 == 0, "{}", s.ToString());
            }
        });
    }
}

INSTANTIATE_TEST_CASE_P(, HDFSClientTest, ::testing::Values(false, true));

TEST_P(HDFSClientTest, test_hdfs_read_write)
{
    if (strlen(FLAGS_test_name_node) == 0 || strlen(FLAGS_test_backup_path) == 0) {
        GTEST_SKIP() << "Set hdfs_test.* configs in config-test.ini to enable hdfs_service_test.";
    }

    auto s = std::make_shared<hdfs_service>();
    ASSERT_EQ(dsn::ERR_OK, s->initialize({FLAGS_test_name_node, FLAGS_test_backup_path}));

    const std::string kRemoteTestPath = "hdfs_client_test";
    const std::string kRemoteTestRWFile = kRemoteTestPath + "/test_write_file";
    const std::string kTestBuffer = "write_hello_world_for_test";
    const int kTestBufferLength = kTestBuffer.size();

    // 1. clean up all old file in remote test directory.
    printf("clean up all old files.\n");
    remove_path_response rem_resp;
    s->remove_path(remove_path_request{kRemoteTestPath, true},
                   LPC_TEST_HDFS,
                   [&rem_resp](const remove_path_response &resp) { rem_resp = resp; },
                   nullptr)
        ->wait();
    ASSERT_TRUE(dsn::ERR_OK == rem_resp.err || dsn::ERR_OBJECT_NOT_FOUND == rem_resp.err);

    // 2. create file.
    printf("test write operation.\n");
    create_file_response cf_resp;
    s->create_file(create_file_request{kRemoteTestRWFile, false},
                   LPC_TEST_HDFS,
                   [&cf_resp](const create_file_response &r) { cf_resp = r; },
                   nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, cf_resp.err);

    // 3. write file.
    dsn::blob bb(kTestBuffer.c_str(), 0, kTestBufferLength);
    write_response w_resp;
    cf_resp.file_handle
        ->write(write_request{bb},
                LPC_TEST_HDFS,
                [&w_resp](const write_response &w) { w_resp = w; },
                nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, w_resp.err);
    ASSERT_EQ(kTestBufferLength, w_resp.written_size);
    ASSERT_EQ(kTestBufferLength, cf_resp.file_handle->get_size());

    // 4. read file.
    printf("test read just written contents.\n");
    read_response r_resp;
    cf_resp.file_handle
        ->read(read_request{0, -1},
               LPC_TEST_HDFS,
               [&r_resp](const read_response &r) { r_resp = r; },
               nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, r_resp.err);
    ASSERT_EQ(kTestBufferLength, r_resp.buffer.length());
    ASSERT_EQ(kTestBuffer, r_resp.buffer.to_string());

    // 5. partial read.
    const uint64_t kOffset = 5;
    const int64_t kSize = 10;
    cf_resp.file_handle
        ->read(read_request{kOffset, kSize},
               LPC_TEST_HDFS,
               [&r_resp](const read_response &r) { r_resp = r; },
               nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, r_resp.err);
    ASSERT_EQ(kSize, r_resp.buffer.length());
    ASSERT_EQ(kTestBuffer.substr(kOffset, kSize), r_resp.buffer.to_string());
}

TEST_P(HDFSClientTest, test_upload_and_download)
{
    if (strlen(FLAGS_test_name_node) == 0 || strlen(FLAGS_test_backup_path) == 0) {
        GTEST_SKIP() << "Set hdfs_test.* configs in config-test.ini to enable hdfs_service_test.";
    }

    auto s = std::make_shared<hdfs_service>();
    ASSERT_EQ(dsn::ERR_OK, s->initialize({FLAGS_test_name_node, FLAGS_test_backup_path}));

    const std::string kLocalFile = "test_file";
    const std::string kRemoteTestPath = "hdfs_client_test";
    const std::string kRemoteTestFile = kRemoteTestPath + "/test_file";

    // 0. generate test file.
    NO_FATALS(generate_test_file(kLocalFile));
    int64_t local_file_size = 0;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kLocalFile, dsn::utils::FileDataType::kSensitive, local_file_size));
    std::string local_file_md5sum;
    dsn::utils::filesystem::md5sum(kLocalFile, local_file_md5sum);

    // 1. clean up all old file in remote test directory.
    printf("clean up all old files.\n");
    remove_path_response rem_resp;
    s->remove_path(remove_path_request{kRemoteTestPath, true},
                   LPC_TEST_HDFS,
                   [&rem_resp](const remove_path_response &resp) { rem_resp = resp; },
                   nullptr)
        ->wait();
    ASSERT_TRUE(dsn::ERR_OK == rem_resp.err || dsn::ERR_OBJECT_NOT_FOUND == rem_resp.err);

    // 2. create file.
    printf("create and upload: %s.\n", kRemoteTestFile.c_str());
    create_file_response cf_resp;
    s->create_file(create_file_request{kRemoteTestFile, true},
                   LPC_TEST_HDFS,
                   [&cf_resp](const create_file_response &r) { cf_resp = r; },
                   nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, cf_resp.err);

    // 3. upload file.
    upload_response u_resp;
    cf_resp.file_handle
        ->upload(upload_request{kLocalFile},
                 LPC_TEST_HDFS,
                 [&u_resp](const upload_response &r) { u_resp = r; },
                 nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, u_resp.err);
    ASSERT_EQ(local_file_size, cf_resp.file_handle->get_size());

    // 4. list directory.
    ls_response l_resp;
    s->list_dir(ls_request{kRemoteTestPath},
                LPC_TEST_HDFS,
                [&l_resp](const ls_response &resp) { l_resp = resp; },
                nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, l_resp.err);
    ASSERT_EQ(1, l_resp.entries->size());
    ASSERT_EQ(kLocalFile, l_resp.entries->at(0).entry_name);
    ASSERT_EQ(false, l_resp.entries->at(0).is_directory);

    // 5. download file.
    download_response d_resp;
    printf("test download %s.\n", kRemoteTestFile.c_str());
    s->create_file(create_file_request{kRemoteTestFile, false},
                   LPC_TEST_HDFS,
                   [&cf_resp](const create_file_response &resp) { cf_resp = resp; },
                   nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, cf_resp.err);
    ASSERT_EQ(local_file_size, cf_resp.file_handle->get_size());
    std::string kLocalDownloadFile = "test_file_d";
    cf_resp.file_handle
        ->download(download_request{kLocalDownloadFile, 0, -1},
                   LPC_TEST_HDFS,
                   [&d_resp](const download_response &resp) { d_resp = resp; },
                   nullptr)
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, d_resp.err);
    ASSERT_EQ(local_file_size, d_resp.downloaded_size);

    // 6. compare kLocalFile and kLocalDownloadFile.
    // 6.1 check file size.
    int64_t local_download_file_size = 0;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kLocalDownloadFile, dsn::utils::FileDataType::kSensitive, local_download_file_size));
    ASSERT_EQ(local_file_size, local_download_file_size);
    // 6.2 check file md5sum.
    std::string local_download_file_md5sum;
    dsn::utils::filesystem::md5sum(kLocalDownloadFile, local_download_file_md5sum);
    ASSERT_EQ(local_file_md5sum, local_download_file_md5sum);

    // 7. clean up local test files.
    utils::filesystem::remove_path(kLocalFile);
    utils::filesystem::remove_path(kLocalDownloadFile);
}

TEST_P(HDFSClientTest, test_concurrent_upload_download)
{
    if (strlen(FLAGS_test_name_node) == 0 || strlen(FLAGS_test_backup_path) == 0) {
        GTEST_SKIP() << "Set hdfs_test.* configs in config-test.ini to enable hdfs_service_test.";
    }

    auto s = std::make_shared<hdfs_service>();
    ASSERT_EQ(dsn::ERR_OK, s->initialize({FLAGS_test_name_node, FLAGS_test_backup_path}));

    int total_files = FLAGS_num_total_files_for_hdfs_concurrent_test;
    std::vector<std::string> local_file_names;
    std::vector<std::string> remote_file_names;
    std::vector<std::string> downloaded_file_names;

    std::vector<int64_t> files_size;
    std::vector<std::string> files_md5sum;
    local_file_names.reserve(total_files);
    remote_file_names.reserve(total_files);
    downloaded_file_names.reserve(total_files);
    files_size.reserve(total_files);
    files_md5sum.reserve(total_files);

    // generate test files.
    for (int i = 0; i < total_files; ++i) {
        std::string file_name = "randomfile" + std::to_string(i);
        NO_FATALS(generate_test_file(file_name));
        int64_t file_size = 0;
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            file_name, dsn::utils::FileDataType::kSensitive, file_size));
        std::string md5sum;
        ASSERT_EQ(ERR_OK, dsn::utils::filesystem::md5sum(file_name, md5sum));

        local_file_names.emplace_back(file_name);
        remote_file_names.emplace_back("hdfs_concurrent_test/" + file_name);
        downloaded_file_names.emplace_back(file_name + "_d");
        files_size.emplace_back(file_size);
        files_md5sum.emplace_back(md5sum);
    }

    printf("clean up all old files.\n");
    remove_path_response rem_resp;
    s->remove_path(remove_path_request{"hdfs_concurrent_test", true},
                   LPC_TEST_HDFS,
                   [&rem_resp](const remove_path_response &resp) { rem_resp = resp; },
                   nullptr)
        ->wait();
    ASSERT_TRUE(dsn::ERR_OK == rem_resp.err || dsn::ERR_OBJECT_NOT_FOUND == rem_resp.err);

    printf("test concurrent upload files.\n");
    {
        std::vector<block_file_ptr> block_files;
        for (int i = 0; i < total_files; ++i) {
            create_file_response cf_resp;
            s->create_file(create_file_request{remote_file_names[i], true},
                           LPC_TEST_HDFS,
                           [&cf_resp](const create_file_response &resp) { cf_resp = resp; },
                           nullptr)
                ->wait();
            ASSERT_EQ(dsn::ERR_OK, cf_resp.err);
            ASSERT_NE(nullptr, cf_resp.file_handle.get());
            block_files.push_back(cf_resp.file_handle);
        }

        std::vector<dsn::task_ptr> callbacks;
        for (int i = 0; i < total_files; ++i) {
            block_file_ptr p = block_files[i];
            dsn::task_ptr t =
                p->upload(upload_request{local_file_names[i]},
                          LPC_TEST_HDFS,
                          [p, &local_file_names, &files_size, i](const upload_response &resp) {
                              printf("file %s upload finished.\n", local_file_names[i].c_str());
                              ASSERT_EQ(dsn::ERR_OK, resp.err);
                              ASSERT_EQ(files_size[i], resp.uploaded_size);
                              ASSERT_EQ(files_size[i], p->get_size());
                          });
            callbacks.push_back(t);
        }

        for (int i = 0; i < total_files; ++i) {
            callbacks[i]->wait();
        }
    }

    printf("test concurrent download files.\n");
    {
        std::vector<block_file_ptr> block_files;
        for (int i = 0; i < total_files; ++i) {
            create_file_response cf_resp;
            s->create_file(create_file_request{remote_file_names[i], true},
                           LPC_TEST_HDFS,
                           [&cf_resp](const create_file_response &r) { cf_resp = r; },
                           nullptr)
                ->wait();
            ASSERT_EQ(dsn::ERR_OK, cf_resp.err);
            ASSERT_NE(nullptr, cf_resp.file_handle.get());
            block_files.push_back(cf_resp.file_handle);
        }

        std::vector<dsn::task_ptr> callbacks;
        for (int i = 0; i < total_files; ++i) {
            block_file_ptr p = block_files[i];
            dsn::task_ptr t = p->download(
                download_request{downloaded_file_names[i], 0, -1},
                LPC_TEST_HDFS,
                [&files_md5sum, &downloaded_file_names, &files_size, i, p](
                    const download_response &dr) {
                    printf("file %s download finished\n", downloaded_file_names[i].c_str());
                    ASSERT_EQ(dsn::ERR_OK, dr.err);
                    ASSERT_EQ(files_size[i], dr.downloaded_size);
                    ASSERT_EQ(files_size[i], p->get_size());
                    std::string md5;
                    dsn::utils::filesystem::md5sum(downloaded_file_names[i], md5);
                    ASSERT_EQ(files_md5sum[i], md5);
                });
            callbacks.push_back(t);
        }

        for (int i = 0; i < total_files; ++i) {
            callbacks[i]->wait();
        }
    }

    // clean up local test files.
    for (int i = 0; i < total_files; ++i) {
        utils::filesystem::remove_path(local_file_names[i]);
        utils::filesystem::remove_path(downloaded_file_names[i]);
    }
}

TEST_P(HDFSClientTest, test_rename_path_while_writing)
{
    std::string kLocalTestPath = "test_dir";
    const int kTotalFiles = 100;

    // The test is flaky, retry if it failed in 300 seconds.
    ASSERT_IN_TIME_WITH_FIXED_INTERVAL(
        [&] {
            task_tracker tracker;
            int success_count = 0;
            write_test_files_async(kLocalTestPath, kTotalFiles, &success_count, &tracker);
            usleep(100);

            std::string kLocalRenamedTestPath = "rename_dir." + std::to_string(dsn_now_ms());
            // Rename succeed but some files write failed.
            ASSERT_TRUE(dsn::utils::filesystem::rename_path(kLocalTestPath, kLocalRenamedTestPath));
            tracker.cancel_outstanding_tasks();
            // Generally, we can assume partial files are written failed.
            ASSERT_GT(success_count, 0) << success_count;
            ASSERT_LT(success_count, kTotalFiles) << success_count;
        },
        300);
}
