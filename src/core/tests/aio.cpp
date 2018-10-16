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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/aio_provider.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/utility/filesystem.h>
#include <dsn/service_api_cpp.h>

#include <gtest/gtest.h>
#include "test_utils.h"

using namespace ::dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER);

TEST(core, aio)
{
    // if in dsn_mimic_app() and disk_io_mode == IOE_PER_QUEUE
    if (task::get_current_disk() == nullptr)
        return;

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
        buffers[i].buffer = reinterpret_cast<void *>(const_cast<char *>(buffer));
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
        EXPECT_TRUE(memcmp(buffer, buffer2, len) == 0);
    }

    err = file::close(fp);
    EXPECT_TRUE(err == ERR_OK);

    utils::filesystem::remove_path("tmp");
}

TEST(core, aio_share)
{
    // if in dsn_mimic_app() and disk_io_mode == IOE_PER_QUEUE
    if (task::get_current_disk() == nullptr)
        return;

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
    // if in dsn_mimic_app() and disk_io_mode == IOE_PER_QUEUE
    if (task::get_current_disk() == nullptr)
        return;

    auto fp = file::open("tmp_test_file", O_WRONLY, 0600);
    EXPECT_TRUE(fp == nullptr);

    ::dsn::error_code *err = new ::dsn::error_code;
    size_t *count = new size_t;
    auto io_callback = [err, count](::dsn::error_code e, size_t n) {
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

    t = ::dsn::file::read(fp2, buffer, 5, 0, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    EXPECT_TRUE(*err == ERR_OK && *count == 5);

    t = ::dsn::file::read(fp2, buffer, 512, 100, LPC_AIO_TEST, nullptr, io_callback, 0);
    t->wait();
    ddebug("error code: %s", err->to_string());
    file::close(fp);
    file::close(fp2);

    EXPECT_TRUE(utils::filesystem::remove_path("tmp_test_file"));
}
