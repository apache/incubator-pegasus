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

/*
 * Description:
 *     Unit-test for c service api.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/service_api_c.h>
#include <dsn/tool_api.h>
#include <dsn/tool-api/task.h>
#include <dsn/cpp/auto_codes.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/filesystem.h>
#include <gtest/gtest.h>
#include <thread>
#include "../core/service_engine.h"

using namespace dsn;

TEST(core, dsn_error)
{
    ASSERT_EQ(ERR_OK, dsn::error_code("ERR_OK"));
    ASSERT_STREQ("ERR_OK", ERR_OK.to_string());
}

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FOR_TEST)
TEST(core, dsn_threadpool_code)
{
    ASSERT_FALSE(dsn::threadpool_code::is_exist("THREAD_POOL_NOT_EXIST"));
    ASSERT_STREQ("THREAD_POOL_DEFAULT", THREAD_POOL_DEFAULT.to_string());
    ASSERT_EQ(THREAD_POOL_DEFAULT, dsn::threadpool_code("THREAD_POOL_DEFAULT"));
    ASSERT_LE(THREAD_POOL_DEFAULT, dsn::threadpool_code::max());

    ASSERT_STREQ("THREAD_POOL_FOR_TEST", THREAD_POOL_FOR_TEST.to_string());
    ASSERT_EQ(THREAD_POOL_FOR_TEST, dsn::threadpool_code("THREAD_POOL_FOR_TEST"));
    ASSERT_LE(THREAD_POOL_FOR_TEST, dsn::threadpool_code::max());

    ASSERT_LT(0, dsn::utils::get_current_tid());
}

DEFINE_TASK_CODE(TASK_CODE_COMPUTE_FOR_TEST, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_AIO(TASK_CODE_AIO_FOR_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(TASK_CODE_RPC_FOR_TEST, TASK_PRIORITY_LOW, THREAD_POOL_DEFAULT)
TEST(core, dsn_task_code)
{
    dsn_task_type_t type;
    dsn_task_priority_t pri;
    dsn::threadpool_code pool;

    ASSERT_EQ(TASK_CODE_INVALID, dsn::task_code::try_get("TASK_CODE_NOT_EXIST", TASK_CODE_INVALID));

    ASSERT_STREQ("TASK_TYPE_COMPUTE", enum_to_string(TASK_TYPE_COMPUTE));

    ASSERT_STREQ("TASK_PRIORITY_HIGH", enum_to_string(TASK_PRIORITY_HIGH));

    ASSERT_STREQ("TASK_CODE_COMPUTE_FOR_TEST",
                 dsn::task_code(TASK_CODE_COMPUTE_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_COMPUTE_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_COMPUTE_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_COMPUTE_FOR_TEST, dsn::task_code::max());
    dsn::task_spec *spec = dsn::task_spec::get(TASK_CODE_COMPUTE_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_COMPUTE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_HIGH, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_AIO_FOR_TEST", dsn::task_code(TASK_CODE_AIO_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_AIO_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_AIO_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_AIO_FOR_TEST, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_AIO_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_AIO, spec->type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST", dsn::task_code(TASK_CODE_RPC_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_RPC_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_RPC_FOR_TEST, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_RPC_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_RPC_REQUEST, spec->type);
    ASSERT_EQ(TASK_PRIORITY_LOW, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST_ACK",
                 dsn::task_code(TASK_CODE_RPC_FOR_TEST_ACK).to_string());
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST_ACK,
              dsn::task_code::try_get("TASK_CODE_RPC_FOR_TEST_ACK", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_RPC_FOR_TEST_ACK, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_RPC_FOR_TEST_ACK.code());
    ASSERT_EQ(TASK_TYPE_RPC_RESPONSE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_LOW, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    spec = dsn::task_spec::get(TASK_CODE_COMPUTE_FOR_TEST.code());
    spec->pool_code = THREAD_POOL_FOR_TEST;
    spec->priority = TASK_PRIORITY_COMMON;
    ASSERT_EQ(TASK_TYPE_COMPUTE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, spec->priority);
    ASSERT_EQ(THREAD_POOL_FOR_TEST, spec->pool_code);

    spec->pool_code = THREAD_POOL_DEFAULT;
    spec->priority = TASK_PRIORITY_HIGH;
}

TEST(core, dsn_config)
{
    ASSERT_TRUE(dsn_config_get_value_bool("apps.client", "run", false, "client run"));
    ASSERT_EQ(1u, dsn_config_get_value_uint64("apps.client", "count", 100, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));
    const char *buffers[100];
    int buffer_count = 100;
    ASSERT_EQ(2, dsn_config_get_all_keys("core.test", buffers, &buffer_count));
    ASSERT_EQ(2, buffer_count);
    ASSERT_STREQ("count", buffers[0]);
    ASSERT_STREQ("run", buffers[1]);
    buffer_count = 1;
    ASSERT_EQ(2, dsn_config_get_all_keys("core.test", buffers, &buffer_count));
    ASSERT_EQ(1, buffer_count);
    ASSERT_STREQ("count", buffers[0]);
}

TEST(core, dsn_coredump) {}

TEST(core, dsn_crc32) {}

TEST(core, dsn_task) {}

TEST(core, dsn_exlock)
{
    if (dsn::service_engine::fast_instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    {
        dsn_handle_t l = dsn_exlock_create(false);
        ASSERT_NE(nullptr, l);
        ASSERT_TRUE(dsn_exlock_try_lock(l));
        dsn_exlock_unlock(l);
        dsn_exlock_lock(l);
        dsn_exlock_unlock(l);
        dsn_exlock_destroy(l);
    }
    {
        dsn_handle_t l = dsn_exlock_create(true);
        ASSERT_NE(nullptr, l);
        ASSERT_TRUE(dsn_exlock_try_lock(l));
        ASSERT_TRUE(dsn_exlock_try_lock(l));
        dsn_exlock_unlock(l);
        dsn_exlock_unlock(l);
        dsn_exlock_lock(l);
        dsn_exlock_lock(l);
        dsn_exlock_unlock(l);
        dsn_exlock_unlock(l);
        dsn_exlock_destroy(l);
    }
}

TEST(core, dsn_rwlock)
{
    if (dsn::service_engine::fast_instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    dsn_handle_t l = dsn_rwlock_nr_create();
    ASSERT_NE(nullptr, l);
    dsn_rwlock_nr_lock_read(l);
    dsn_rwlock_nr_unlock_read(l);
    dsn_rwlock_nr_lock_write(l);
    dsn_rwlock_nr_unlock_write(l);
    dsn_rwlock_nr_destroy(l);
}

TEST(core, dsn_semaphore)
{
    if (dsn::service_engine::fast_instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    dsn_handle_t s = dsn_semaphore_create(2);
    dsn_semaphore_wait(s);
    ASSERT_TRUE(dsn_semaphore_wait_timeout(s, 10));
    ASSERT_FALSE(dsn_semaphore_wait_timeout(s, 10));
    dsn_semaphore_signal(s, 1);
    dsn_semaphore_wait(s);
    dsn_semaphore_destroy(s);
}

TEST(core, dsn_rpc) {}

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_READ, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_NFS, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
struct aio_result
{
    dsn::error_code err;
    size_t sz;
};
TEST(core, dsn_file)
{
    // if in dsn_mimic_app() and disk_io_mode == IOE_PER_QUEUE
    if (task::get_current_disk() == nullptr)
        return;

    int64_t fin_size, fout_size;
    ASSERT_TRUE(utils::filesystem::file_size("command.txt", fin_size));
    ASSERT_LT(0, fin_size);

    dsn_handle_t fin = dsn_file_open("command.txt", O_RDONLY, 0);
    ASSERT_NE(nullptr, fin);
    dsn_handle_t fout = dsn_file_open("command.copy.txt", O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_NE(nullptr, fout);
    char buffer[1024];
    uint64_t offset = 0;
    while (true) {
        aio_result rin;
        dsn_task_t tin = dsn_file_create_aio_task(LPC_AIO_TEST_READ,
                                                  [](dsn::error_code err, size_t sz, void *param) {
                                                      aio_result *r = (aio_result *)param;
                                                      r->err = err;
                                                      r->sz = sz;
                                                  },
                                                  &rin,
                                                  0);
        dsn_task_add_ref(tin);
        ASSERT_NE(nullptr, tin);
        ASSERT_EQ(1, dsn_task_get_ref(tin));
        dsn_file_read(fin, buffer, 1024, offset, tin);
        dsn_task_wait(tin);
        ASSERT_EQ(rin.err, dsn_task_error(tin));
        if (rin.err != ERR_OK) {
            ASSERT_EQ(ERR_HANDLE_EOF, rin.err);
            break;
        }
        ASSERT_LT(0u, rin.sz);
        ASSERT_EQ(rin.sz, dsn_file_get_io_size(tin));
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, dsn_task_get_ref(tin));
        }
        dsn_task_release_ref(tin);

        aio_result rout;
        dsn_task_t tout = dsn_file_create_aio_task(LPC_AIO_TEST_WRITE,
                                                   [](dsn::error_code err, size_t sz, void *param) {
                                                       aio_result *r = (aio_result *)param;
                                                       r->err = err;
                                                       r->sz = sz;
                                                   },
                                                   &rout,
                                                   0);
        dsn_task_add_ref(tout);
        ASSERT_NE(nullptr, tout);
        dsn_file_write(fout, buffer, rin.sz, offset, tout);
        dsn_task_wait(tout);
        ASSERT_EQ(ERR_OK, rout.err);
        ASSERT_EQ(ERR_OK, dsn_task_error(tout));
        ASSERT_EQ(rin.sz, rout.sz);
        ASSERT_EQ(rin.sz, dsn_file_get_io_size(tout));
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, dsn_task_get_ref(tout));
        }
        dsn_task_release_ref(tout);

        ASSERT_EQ(ERR_OK, dsn_file_flush(fout));

        offset += rin.sz;
    }

    ASSERT_EQ((uint64_t)fin_size, offset);
    ASSERT_EQ(ERR_OK, dsn_file_close(fout));
    ASSERT_EQ(ERR_OK, dsn_file_close(fin));

    ASSERT_TRUE(utils::filesystem::file_size("command.copy.txt", fout_size));
    ASSERT_EQ(fin_size, fout_size);
}

// TODO: On windows an opened file cannot be deleted, so this test cannot pass
#ifndef WIN32
TEST(core, dsn_nfs)
{
    // if in dsn_mimic_app() and nfs_io_mode == IOE_PER_QUEUE
    if (task::get_current_nfs() == nullptr)
        return;

    utils::filesystem::remove_path("nfs_test_dir");
    utils::filesystem::remove_path("nfs_test_dir_copy");

    ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir"));
    ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir_copy"));

    ASSERT_TRUE(utils::filesystem::create_directory("nfs_test_dir"));
    ASSERT_TRUE(utils::filesystem::directory_exists("nfs_test_dir"));

    { // copy nfs_test_file1 nfs_test_file2 nfs_test_dir
        ASSERT_FALSE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file1"));
        ASSERT_FALSE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file2"));

        const char *files[] = {"nfs_test_file1", "nfs_test_file2", nullptr};

        aio_result r;
        dsn_task_t t = dsn_file_create_aio_task(LPC_AIO_TEST_NFS,
                                                [](dsn::error_code err, size_t sz, void *param) {
                                                    aio_result *r = (aio_result *)param;
                                                    r->err = err;
                                                    r->sz = sz;
                                                },
                                                &r,
                                                0);
        dsn_task_add_ref(t);
        ASSERT_NE(nullptr, t);
        dsn_file_copy_remote_files(
            dsn_address_build("localhost", 20101), ".", files, "nfs_test_dir", false, false, t);
        ASSERT_TRUE(dsn_task_wait_timeout(t, 20000));
        ASSERT_EQ(r.err, dsn_task_error(t));
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, dsn_file_get_io_size(t));
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, dsn_task_get_ref(t));
        }
        dsn_task_release_ref(t);

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

    { // copy files again, overwrite
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file1"));
        ASSERT_TRUE(utils::filesystem::file_exists("nfs_test_dir/nfs_test_file2"));

        const char *files[] = {"nfs_test_file1", "nfs_test_file2", nullptr};

        aio_result r;
        dsn_task_t t = dsn_file_create_aio_task(LPC_AIO_TEST_NFS,
                                                [](dsn::error_code err, size_t sz, void *param) {
                                                    aio_result *r = (aio_result *)param;
                                                    r->err = err;
                                                    r->sz = sz;
                                                },
                                                &r,
                                                0);
        dsn_task_add_ref(t);
        ASSERT_NE(nullptr, t);
        dsn_file_copy_remote_files(
            dsn_address_build("localhost", 20101), ".", files, "nfs_test_dir", true, false, t);
        ASSERT_TRUE(dsn_task_wait_timeout(t, 20000));
        ASSERT_EQ(r.err, dsn_task_error(t));
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, dsn_file_get_io_size(t));
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, dsn_task_get_ref(t));
        }
        dsn_task_release_ref(t);
    }

    { // copy nfs_test_dir nfs_test_dir_copy
        ASSERT_FALSE(utils::filesystem::directory_exists("nfs_test_dir_copy"));

        aio_result r;
        dsn_task_t t = dsn_file_create_aio_task(LPC_AIO_TEST_NFS,
                                                [](dsn::error_code err, size_t sz, void *param) {
                                                    aio_result *r = (aio_result *)param;
                                                    r->err = err;
                                                    r->sz = sz;
                                                },
                                                &r,
                                                0);
        dsn_task_add_ref(t);
        ASSERT_NE(nullptr, t);
        dsn_file_copy_remote_directory(dsn_address_build("localhost", 20101),
                                       "nfs_test_dir",
                                       "nfs_test_dir_copy",
                                       false,
                                       false,
                                       t);
        ASSERT_TRUE(dsn_task_wait_timeout(t, 20000));
        ASSERT_EQ(r.err, dsn_task_error(t));
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.sz, dsn_file_get_io_size(t));
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, dsn_task_get_ref(t));
        }
        dsn_task_release_ref(t);

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
#endif

TEST(core, dsn_env)
{
    if (dsn::service_engine::fast_instance().spec().tool == "simulator")
        return;
    uint64_t now1 = dsn_now_ns();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    uint64_t now2 = dsn_now_ns();
    ASSERT_LE(now1 + 1000000, now2);
    uint64_t r = dsn_random64(100, 200);
    ASSERT_LE(100, r);
    ASSERT_GE(200, r);
}

TEST(core, dsn_system)
{
    ASSERT_TRUE(tools::is_engine_ready());
    tools::tool_app *tool = tools::get_current_tool();
    ASSERT_EQ(tool->name(), dsn_config_get_value_string("core", "tool", "", ""));

    int app_count = 5;
    int type_count = 1;
    if (tool->get_service_spec().enable_default_app_mimic) {
        app_count++;
        type_count++;
    }

    {
        std::vector<service_app *> apps;
        service_app::get_all_service_apps(&apps);
        ASSERT_EQ(app_count, apps.size());
        std::map<std::string, int> type_to_count;
        for (int i = 0; i < apps.size(); ++i) {
            type_to_count[apps[i]->info().type] += 1;
        }

        ASSERT_EQ(type_count, static_cast<int>(type_to_count.size()));
        ASSERT_EQ(5, type_to_count["test"]);
    }
}
