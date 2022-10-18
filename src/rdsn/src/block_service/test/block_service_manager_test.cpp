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

#include "block_service_mock.h"
#include "block_service/block_service_manager.h"
#include "block_service/local/local_service.h"

#include <fstream>

#include "utils/filesystem.h"
#include <gtest/gtest.h>

namespace dsn {
namespace dist {
namespace block_service {

class block_service_manager_test : public ::testing::Test
{
public:
    block_service_manager_test()
    {
        _fs = make_unique<block_service_mock>();
        utils::filesystem::create_directory(LOCAL_DIR);
    }

    ~block_service_manager_test() { utils::filesystem::remove_path(LOCAL_DIR); }

public:
    error_code test_download_file(uint64_t &download_size)
    {
        return _block_service_manager.download_file(
            PROVIDER, LOCAL_DIR, FILE_NAME, _fs.get(), download_size);
    }

    void create_local_file(const std::string &file_name)
    {
        std::string whole_name = utils::filesystem::path_combine(LOCAL_DIR, file_name);
        utils::filesystem::create_file(whole_name);
        std::ofstream test_file;
        test_file.open(whole_name);
        test_file << "write some data.\n";
        test_file.close();

        _file_meta.name = whole_name;
        utils::filesystem::md5sum(whole_name, _file_meta.md5);
        utils::filesystem::file_size(whole_name, _file_meta.size);
    }

    void create_remote_file(const std::string &file_name, int64_t size, const std::string &md5)
    {
        std::string whole_file_name = utils::filesystem::path_combine(PROVIDER, file_name);
        _fs->files[whole_file_name] = std::make_pair(size, md5);
    }

public:
    block_service_manager _block_service_manager;
    std::unique_ptr<block_service_mock> _fs;

    replication::file_meta _file_meta;
    std::string PROVIDER = "local_service";
    std::string LOCAL_DIR = "test_dir";
    std::string FILE_NAME = "test_file";
};

// download_file unit tests
TEST_F(block_service_manager_test, do_download_remote_file_not_exist)
{
    utils::filesystem::remove_path(LOCAL_DIR);
    auto fs = make_unique<local_service>();
    fs->initialize({LOCAL_DIR});
    uint64_t download_size = 0;
    error_code err = _block_service_manager.download_file(
        PROVIDER, LOCAL_DIR, FILE_NAME, fs.get(), download_size);
    ASSERT_EQ(err, ERR_CORRUPTION); // file does not exist
}

TEST_F(block_service_manager_test, do_download_same_name_file)
{
    // local file exists, but md5 not matched with remote file
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, 2333, "md5_not_match");
    uint64_t download_size = 0;
    ASSERT_EQ(test_download_file(download_size), ERR_PATH_ALREADY_EXIST);
    ASSERT_EQ(download_size, 0);
}

TEST_F(block_service_manager_test, do_download_file_exist)
{
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, _file_meta.size, _file_meta.md5);
    uint64_t download_size = 0;
    ASSERT_EQ(test_download_file(download_size), ERR_PATH_ALREADY_EXIST);
    ASSERT_EQ(download_size, 0);
}

TEST_F(block_service_manager_test, do_download_succeed)
{
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, _file_meta.size, _file_meta.md5);
    // remove local file to mock condition that file not existed
    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::remove_path(file_name);
    uint64_t download_size = 0;
    ASSERT_EQ(test_download_file(download_size), ERR_OK);
    ASSERT_EQ(download_size, _file_meta.size);
}

} // namespace block_service
} // namespace dist
} // namespace dsn
