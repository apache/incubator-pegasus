// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica_test_base.h"

#include <fstream>

#include <gtest/gtest.h>

namespace dsn {
namespace replication {

class replica_file_provider_test : public replica_test_base
{
public:
    replica_file_provider_test()
    {
        _replica = create_mock_replica(stub.get());
        _fs = stub->get_block_filesystem();
        utils::filesystem::create_directory(LOCAL_DIR);
    }

    ~replica_file_provider_test() { utils::filesystem::remove_path(LOCAL_DIR); }

public:
    error_code test_do_download()
    {
        uint64_t download_size = 0;
        return _replica->do_download(PROVIDER, LOCAL_DIR, FILE_NAME, _fs.get(), download_size);
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
    std::unique_ptr<mock_replica> _replica;
    std::unique_ptr<block_service_mock> _fs;

    file_meta _file_meta;
    std::string PROVIDER = "local_service";
    std::string LOCAL_DIR = "test_dir";
    std::string FILE_NAME = "test_file";
};

// do_download unit tests
TEST_F(replica_file_provider_test, do_download_remote_file_not_exist)
{
    ASSERT_EQ(test_do_download(), ERR_CORRUPTION);
}

TEST_F(replica_file_provider_test, do_download_redownload_file)
{
    // local file exists, but md5 not matched with remote file
    // expected to remove old local file and redownload it
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, 2333, "md5_not_match");
    ASSERT_EQ(test_do_download(), ERR_OK);
}

TEST_F(replica_file_provider_test, do_download_file_exist)
{
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, _file_meta.size, _file_meta.md5);
    ASSERT_EQ(test_do_download(), ERR_OK);
}

TEST_F(replica_file_provider_test, do_download_succeed)
{
    create_local_file(FILE_NAME);
    create_remote_file(FILE_NAME, _file_meta.size, _file_meta.md5);
    // remove local file to mock condition that file not existed
    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::remove_path(file_name);
    ASSERT_EQ(test_do_download(), ERR_OK);
}

} // namespace replication
} // namespace dsn
