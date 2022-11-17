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

#include <gtest/gtest.h>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/filesystem.h"
#include "runtime/task/task.h"
#include "runtime/task/async_calls.h"
#include "nfs/nfs_node.h"

using namespace dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_NFS, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
struct aio_result
{
    dsn::error_code err;
    size_t sz;
};

TEST(nfs, basic)
{
    std::unique_ptr<dsn::nfs_node> nfs(dsn::nfs_node::create());
    nfs->start();

    utils::filesystem::remove_path("nfs_test_dir");
    utils::filesystem::remove_path("nfs_test_dir_copy");

    ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir"));
    ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir_copy"));

    ASSERT_TRUE(utils::filesystem::create_directory("nfs_test_dir"));
    ASSERT_TRUE(utils::filesystem::directory_exists("nfs_test_dir"));

    {
        // copy nfs_test_file1 nfs_test_file2 nfs_test_dir
        ASSERT_FALSE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file1"));
        ASSERT_FALSE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file2"));

        std::vector<std::string> files{"nfs_test_file1", "nfs_test_file2"};

        aio_result r;
        dsn::aio_task_ptr t = nfs->copy_remote_files(dsn::rpc_address("localhost", 20101),
                                                     "default",
                                                     ".",
                                                     files,
                                                     "default",
                                                     "nfs_test_dir",
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

        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file1"));
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file2"));

        int64_t sz1, sz2;
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_file1", sz1));
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir/nfs_test_file1", sz2));
        ASSERT_EQ(sz1, sz2);
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_file2", sz1));
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir/nfs_test_file2", sz2));
        ASSERT_EQ(sz1, sz2);
    }

    {
        // copy files again, overwrite
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file1"));
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file2"));

        std::vector<std::string> files{"nfs_test_file1", "nfs_test_file2"};

        aio_result r;
        dsn::aio_task_ptr t = nfs->copy_remote_files(dsn::rpc_address("localhost", 20101),
                                                     "default",
                                                     ".",
                                                     files,
                                                     "default",
                                                     "nfs_test_dir",
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
    }

    {
        // copy nfs_test_dir nfs_test_dir_copy
        ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir_copy"));

        aio_result r;
        dsn::aio_task_ptr t = nfs->copy_remote_directory(dsn::rpc_address("localhost", 20101),
                                                         "default",
                                                         "nfs_test_dir",
                                                         "default",
                                                         "nfs_test_dir_copy",
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

        ASSERT_TRUE(utils::filesystem::directory_exists("nfs_test_dir_copy"));
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir_copy/nfs_test_file1"));
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir_copy/nfs_test_file2"));

        std::vector<std::string> sub1, sub2;
        ASSERT_TRUE(utils::filesystem::get_subfiles("nfs_test_dir", sub1, true));
        ASSERT_TRUE(utils::filesystem::get_subfiles("nfs_test_dir_copy", sub2, true));
        ASSERT_EQ(sub1.size(), sub2.size());

        int64_t sz1, sz2;
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir/nfs_test_file1", sz1));
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir_copy/nfs_test_file1", sz2));
        ASSERT_EQ(sz1, sz2);
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir/nfs_test_file2", sz1));
        ASSERT_TRUE(utils::filesystem::file_size("nfs_test_dir_copy/nfs_test_file2", sz2));
        ASSERT_EQ(sz1, sz2);
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
