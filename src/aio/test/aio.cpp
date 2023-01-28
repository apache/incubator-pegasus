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

#include "runtime/task/async_calls.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/smart_pointers.h"
#include "utils/strings.h"

using namespace ::dsn;

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER);

TEST(core, aio)
{
    fail::setup();
    fail::cfg("aio_pwrite_incomplete", "void()");
    const char *buffer = "hello, world";
    int len = (int)strlen(buffer);

    // write
    auto fp = file::open("tmp", O_RDWR | O_CREAT | O_BINARY, 0666);

    std::list<aio_task_ptr> tasks;
    uint64_t offset = 0;

    // new write
    for (int i = 0; i < 100; i++) {
        auto t = ::dsn::file::write(fp, buffer, len, offset, LPC_AIO_TEST, nullptr, nullptr);
        tasks.push_back(t);
        offset += len;
    }

    for (auto &t : tasks) {
        t->wait();
    }

    // overwrite
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 100; i++) {
        auto t = ::dsn::file::write(fp, buffer, len, offset, LPC_AIO_TEST, nullptr, nullptr);
        tasks.push_back(t);
        offset += len;
    }

    for (auto &t : tasks) {
        t->wait();
        EXPECT_TRUE(t->get_transferred_size() == (size_t)len);
    }

    // vector write
    tasks.clear();
    std::unique_ptr<dsn_file_buffer_t[]> buffers(new dsn_file_buffer_t[100]);
    for (int i = 0; i < 10; i++) {
        buffers[i].buffer = static_cast<void *>(const_cast<char *>(buffer));
        buffers[i].size = len;
    }
    for (int i = 0; i < 10; i++) {
        tasks.push_back(::dsn::file::write_vector(
            fp, buffers.get(), 10, offset, LPC_AIO_TEST, nullptr, nullptr));
        offset += 10 * len;
    }
    for (auto &t : tasks) {
        t->wait();
        EXPECT_TRUE(t->get_transferred_size() == 10 * len);
    }
    auto err = file::close(fp);
    EXPECT_TRUE(err == ERR_OK);

    // read
    char *buffer2 = (char *)alloca((size_t)len);
    fp = file::open("tmp", O_RDONLY | O_BINARY, 0);

    // concurrent read
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 100; i++) {
        auto t = ::dsn::file::read(fp, buffer2, len, offset, LPC_AIO_TEST, nullptr, nullptr);
        tasks.push_back(t);
        offset += len;
    }

    for (auto &t : tasks) {
        t->wait();
        EXPECT_TRUE(t->get_transferred_size() == (size_t)len);
    }

    // sequential read
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 200; i++) {
        buffer2[0] = 'x';
        auto t = ::dsn::file::read(fp, buffer2, len, offset, LPC_AIO_TEST, nullptr, nullptr);
        offset += len;

        t->wait();
        EXPECT_TRUE(t->get_transferred_size() == (size_t)len);
        EXPECT_TRUE(dsn::utils::mequals(buffer, buffer2, len));
    }

    err = file::close(fp);
    fail::teardown();
    EXPECT_TRUE(err == ERR_OK);

    utils::filesystem::remove_path("tmp");
}

TEST(core, aio_share)
{
    auto fp = file::open("tmp", O_WRONLY | O_CREAT | O_BINARY, 0666);
    EXPECT_TRUE(fp != nullptr);

    auto fp2 = file::open("tmp", O_RDONLY | O_BINARY, 0);
    EXPECT_TRUE(fp2 != nullptr);

    file::close(fp);
    file::close(fp2);

    utils::filesystem::remove_path("tmp");
}

TEST(core, operation_failed)
{
    fail::setup();
    fail::cfg("aio_pwrite_incomplete", "void()");

    auto fp = file::open("tmp_test_file", O_WRONLY, 0600);
    EXPECT_TRUE(fp == nullptr);

    auto err = dsn::make_unique<dsn::error_code>();
    auto count = dsn::make_unique<size_t>();
    auto io_callback = [&err, &count](::dsn::error_code e, size_t n) {
        *err = e;
        *count = n;
    };

    fp = file::open("tmp_test_file", O_WRONLY | O_CREAT | O_BINARY, 0666);
    EXPECT_TRUE(fp != nullptr);
    char buffer[512];
    const char *str = "hello file";
    auto t = ::dsn::file::write(fp, str, strlen(str), 0, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    EXPECT_TRUE(*err == ERR_OK && *count == strlen(str));

    t = ::dsn::file::read(fp, buffer, 512, 0, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    EXPECT_TRUE(*err == ERR_FILE_OPERATION_FAILED);

    auto fp2 = file::open("tmp_test_file", O_RDONLY | O_BINARY, 0);
    EXPECT_TRUE(fp2 != nullptr);

    t = ::dsn::file::read(fp2, buffer, 512, 0, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    EXPECT_TRUE(*err == ERR_OK && *count == strlen(str));
    EXPECT_TRUE(dsn::utils::equals(buffer, str, 10));

    t = ::dsn::file::read(fp2, buffer, 5, 0, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    EXPECT_TRUE(*err == ERR_OK && *count == 5);
    EXPECT_TRUE(dsn::utils::equals(buffer, str, 5));

    t = ::dsn::file::read(fp2, buffer, 512, 100, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    LOG_INFO("error code: {}", *err);
    file::close(fp);
    file::close(fp2);
    fail::teardown();

    EXPECT_TRUE(utils::filesystem::remove_path("tmp_test_file"));
}

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_READ, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
struct aio_result
{
    dsn::error_code err;
    size_t sz;
};
TEST(core, dsn_file)
{
    int64_t fin_size, fout_size;
    ASSERT_TRUE(utils::filesystem::file_size("copy_source.txt", fin_size));
    ASSERT_LT(0, fin_size);

    dsn::disk_file *fin = file::open("copy_source.txt", O_RDONLY, 0);
    ASSERT_NE(nullptr, fin);
    dsn::disk_file *fout = file::open("copy_dest.txt", O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_NE(nullptr, fout);
    char buffer[1024];
    uint64_t offset = 0;
    while (true) {
        aio_result rin;
        aio_task_ptr tin = file::read(fin,
                                      buffer,
                                      1024,
                                      offset,
                                      LPC_AIO_TEST_READ,
                                      nullptr,
                                      [&rin](dsn::error_code err, size_t sz) {
                                          rin.err = err;
                                          rin.sz = sz;
                                      },
                                      0);
        ASSERT_NE(nullptr, tin);

        if (dsn::tools::get_current_tool()->name() != "simulator") {
            // at least 1 for tin, but if already read completed, then only 1
            ASSERT_LE(1, tin->get_count());
        }

        tin->wait();
        ASSERT_EQ(rin.err, tin->error());
        if (rin.err != ERR_OK) {
            ASSERT_EQ(ERR_HANDLE_EOF, rin.err);
            break;
        }
        ASSERT_LT(0u, rin.sz);
        ASSERT_EQ(rin.sz, tin->get_transferred_size());
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, tin->get_count());
        }

        aio_result rout;
        aio_task_ptr tout = file::write(fout,
                                        buffer,
                                        rin.sz,
                                        offset,
                                        LPC_AIO_TEST_WRITE,
                                        nullptr,
                                        [&rout](dsn::error_code err, size_t sz) {
                                            rout.err = err;
                                            rout.sz = sz;
                                        },
                                        0);
        ASSERT_NE(nullptr, tout);
        tout->wait();
        ASSERT_EQ(ERR_OK, rout.err);
        ASSERT_EQ(ERR_OK, tout->error());
        ASSERT_EQ(rin.sz, rout.sz);
        ASSERT_EQ(rin.sz, tout->get_transferred_size());
        // this is only true for simulator
        if (dsn::tools::get_current_tool()->name() == "simulator") {
            ASSERT_EQ(1, tout->get_count());
        }

        ASSERT_EQ(ERR_OK, file::flush(fout));

        offset += rin.sz;
    }

    ASSERT_EQ((uint64_t)fin_size, offset);
    ASSERT_EQ(ERR_OK, file::close(fout));
    ASSERT_EQ(ERR_OK, file::close(fin));

    ASSERT_TRUE(utils::filesystem::file_size("copy_dest.txt", fout_size));
    ASSERT_EQ(fin_size, fout_size);
}
