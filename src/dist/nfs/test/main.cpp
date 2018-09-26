#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/task.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/dist/nfs_node.h>

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
                                                     ".",
                                                     files,
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
                                                     ".",
                                                     files,
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
                                                         "nfs_test_dir",
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
}

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    dsn_run_config("config.ini", false);
    return RUN_ALL_TESTS();
}
