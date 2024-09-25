/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <rocksdb/status.h>
#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aio/aio_task.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "nfs/nfs_node.h"
#include "rpc/rpc_host_port.h"
#include "runtime/app_model.h"
#include "runtime/tool_api.h"
#include "task/task_code.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/threadpool_code.h"

DSN_DECLARE_bool(encrypt_data_at_rest);

using namespace dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_NFS, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
struct aio_result
{
    dsn::error_code err;
    size_t sz;
};

class nfs_test : public pegasus::encrypt_data_test_base
{
};

INSTANTIATE_TEST_SUITE_P(, nfs_test, ::testing::Values(false, true));

TEST_P(nfs_test, basic)
{
    auto nfs = dsn::nfs_node::create();
    nfs->start();
    nfs->register_async_rpc_handler_for_test();
    dsn::gpid fake_pid = gpid(1, 0);

    // Prepare the destination directory.
    const std::string kDstDir = "nfs_test_dir";
    ASSERT_TRUE(utils::filesystem::remove_path(kDstDir));
    ASSERT_FALSE(utils::filesystem::directory_exists(kDstDir));
    ASSERT_TRUE(utils::filesystem::create_directory(kDstDir));
    ASSERT_TRUE(utils::filesystem::directory_exists(kDstDir));

    // Prepare the source files information.
    std::vector<std::string> kSrcFilenames({"nfs_test_file1", "nfs_test_file2"});
    if (FLAGS_encrypt_data_at_rest) {
        for (auto &src_filename : kSrcFilenames) {
            auto s = dsn::utils::encrypt_file(src_filename, src_filename + ".encrypted");
            ASSERT_TRUE(s.ok()) << s.ToString();
            src_filename += ".encrypted";
        }
    }
    std::vector<int64_t> src_file_sizes;
    std::vector<std::string> src_file_md5s;
    for (const auto &src_filename : kSrcFilenames) {
        int64_t file_size;
        ASSERT_TRUE(utils::filesystem::file_size(
            src_filename, dsn::utils::FileDataType::kSensitive, file_size));
        src_file_sizes.push_back(file_size);
        std::string src_file_md5;
        ASSERT_EQ(ERR_OK, utils::filesystem::md5sum(src_filename, src_file_md5));
        src_file_md5s.emplace_back(std::move(src_file_md5));
    }

    // copy files to the destination directory.
    {
        // The destination directory is empty before copying.
        std::vector<std::string> dst_filenames;
        ASSERT_TRUE(utils::filesystem::get_subfiles(kDstDir, dst_filenames, true));
        ASSERT_TRUE(dst_filenames.empty());

        aio_result r;
        auto t = nfs->copy_remote_files(
            dsn::host_port("localhost", 20101),
            "default",
            ".",
            kSrcFilenames,
            "default",
            kDstDir,
            fake_pid,
            false,
            false,
            LPC_AIO_TEST_NFS,
            nullptr,
            [&r](dsn::error_code err, size_t sz) {
                r.err = err;
                r.sz = sz;
            },
            0);
        ASSERT_NE(nullptr, t);
        ASSERT_TRUE(t->wait(20000));
        ASSERT_EQ(r.err, t->error());
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, t->get_transferred_size());

        // The destination files equal to the source files after copying.
        ASSERT_TRUE(utils::filesystem::get_subfiles(kDstDir, dst_filenames, true));
        std::sort(dst_filenames.begin(), dst_filenames.end());
        ASSERT_EQ(kSrcFilenames.size(), dst_filenames.size());
        int i = 0;
        for (const auto &dst_filename : dst_filenames) {
            int64_t file_size;
            ASSERT_TRUE(utils::filesystem::file_size(
                dst_filename, dsn::utils::FileDataType::kSensitive, file_size));
            ASSERT_EQ(src_file_sizes[i], file_size);
            std::string file_md5;
            ASSERT_EQ(ERR_OK, utils::filesystem::md5sum(dst_filename, file_md5));
            ASSERT_EQ(src_file_md5s[i], file_md5);
            i++;
        }
    }

    // copy files to the destination directory, files will be overwritten.
    {
        aio_result r;
        auto t = nfs->copy_remote_files(
            dsn::host_port("localhost", 20101),
            "default",
            ".",
            kSrcFilenames,
            "default",
            kDstDir,
            fake_pid,
            true,
            false,
            LPC_AIO_TEST_NFS,
            nullptr,
            [&r](dsn::error_code err, size_t sz) {
                r.err = err;
                r.sz = sz;
            },
            0);
        ASSERT_NE(nullptr, t);
        ASSERT_TRUE(t->wait(20000));
        ASSERT_EQ(r.err, t->error());
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, t->get_transferred_size());

        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, t->get_count());
        }

        // The destination files equal to the source files after overwrite copying.
        std::vector<std::string> dst_filenames;
        ASSERT_TRUE(utils::filesystem::get_subfiles(kDstDir, dst_filenames, true));
        std::sort(dst_filenames.begin(), dst_filenames.end());
        ASSERT_EQ(kSrcFilenames.size(), dst_filenames.size());
        int i = 0;
        for (const auto &dst_filename : dst_filenames) {
            int64_t file_size;
            ASSERT_TRUE(utils::filesystem::file_size(
                dst_filename, dsn::utils::FileDataType::kSensitive, file_size));
            ASSERT_EQ(src_file_sizes[i], file_size);
            std::string file_md5;
            ASSERT_EQ(ERR_OK, utils::filesystem::md5sum(dst_filename, file_md5));
            ASSERT_EQ(src_file_md5s[i], file_md5);
            i++;
        }
    }

    // copy files from kDstDir to kNewDstDir.
    {
        const std::string kNewDstDir = "nfs_test_dir_copy";
        ASSERT_TRUE(utils::filesystem::remove_path(kNewDstDir));
        ASSERT_FALSE(utils::filesystem::directory_exists(kNewDstDir));

        aio_result r;
        auto t = nfs->copy_remote_directory(
            dsn::host_port("localhost", 20101),
            "default",
            kDstDir,
            "default",
            kNewDstDir,
            fake_pid,
            false,
            false,
            LPC_AIO_TEST_NFS,
            nullptr,
            [&r](dsn::error_code err, size_t sz) {
                r.err = err;
                r.sz = sz;
            },
            0);
        ASSERT_NE(nullptr, t);
        ASSERT_TRUE(t->wait(20000));
        ASSERT_EQ(r.err, t->error());
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, t->get_transferred_size());

        // The kNewDstDir will be created automatically.
        ASSERT_TRUE(utils::filesystem::directory_exists(kNewDstDir));

        std::vector<std::string> new_dst_filenames;
        ASSERT_TRUE(utils::filesystem::get_subfiles(kNewDstDir, new_dst_filenames, true));
        std::sort(new_dst_filenames.begin(), new_dst_filenames.end());
        ASSERT_EQ(kSrcFilenames.size(), new_dst_filenames.size());

        int i = 0;
        for (const auto &new_dst_filename : new_dst_filenames) {
            int64_t file_size;
            ASSERT_TRUE(utils::filesystem::file_size(
                new_dst_filename, dsn::utils::FileDataType::kSensitive, file_size));
            ASSERT_EQ(src_file_sizes[i], file_size);
            std::string file_md5;
            ASSERT_EQ(ERR_OK, utils::filesystem::md5sum(new_dst_filename, file_md5));
            ASSERT_EQ(src_file_md5s[i], file_md5);
            i++;
        }
    }

    nfs->stop();
}

int g_test_ret = 0;
GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    dsn_run_config("config.ini", false);
    g_test_ret = RUN_ALL_TESTS();
    dsn_exit(g_test_ret);
}
