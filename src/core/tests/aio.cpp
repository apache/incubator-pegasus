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


# include <dsn/internal/aio_provider.h>
# include <gtest/gtest.h>
# include <dsn/service_api_cpp.h>
# include "test_utils.h"

using namespace ::dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER);

TEST(core, aio)
{
    const char* buffer = "hello, world";
    int len = (int)strlen(buffer);

    // write
    auto fp = dsn_file_open("tmp", O_RDWR | O_CREAT | O_BINARY, 0666);
    
    std::list<task_ptr> tasks;
    uint64_t offset = 0;

    // new write
    for (int i = 0; i < 100; i++)
    {
        auto t = ::dsn::file::write(fp, buffer, len, offset, LPC_AIO_TEST, nullptr, nullptr, 0);
        tasks.push_back(t);
        offset += len;
    }

    for (auto& t : tasks)
    {
        bool r = t->wait();
        EXPECT_TRUE(r);
    }

    // overwrite 
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 100; i++)
    {
        auto t = ::dsn::file::write(fp, buffer, len, offset, LPC_AIO_TEST, nullptr, nullptr, 0);
        tasks.push_back(t);
        offset += len;
    }

    for (auto& t : tasks)
    {
        bool r = t->wait();
        EXPECT_TRUE(r);
        EXPECT_TRUE(t->io_size() == (size_t)len);
    }

    auto err = dsn_file_close(fp);
    EXPECT_TRUE(err == ERR_OK);

    // read
    char* buffer2 = (char*)alloca((size_t)len);
    fp = dsn_file_open("tmp", O_RDONLY | O_BINARY, 0);

    // concurrent read
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 100; i++)
    {
        auto t = ::dsn::file::read(fp, buffer2, len, offset, LPC_AIO_TEST, nullptr, nullptr, 0);
        tasks.push_back(t);
        offset += len;
    }

    for (auto& t : tasks)
    {
        bool r = t->wait();
        EXPECT_TRUE(r);
        EXPECT_TRUE(t->io_size() == (size_t)len);
    }

    // sequential read
    offset = 0;
    tasks.clear();
    for (int i = 0; i < 100; i++)
    {
        buffer2[0] = 'x';
        auto t = ::dsn::file::read(fp, buffer2, len, offset, LPC_AIO_TEST, nullptr, nullptr, 0);
        offset += len;

        bool r = t->wait();
        EXPECT_TRUE(r);
        EXPECT_TRUE(t->io_size() == (size_t)len);
        EXPECT_TRUE(memcmp(buffer, buffer2, len) == 0);
    }

    err = dsn_file_close(fp);
    EXPECT_TRUE(err == ERR_OK);
}

TEST(core, aio_share)
{
    auto fp = dsn_file_open("tmp", O_WRONLY | O_CREAT | O_BINARY, 0666);
    EXPECT_TRUE(fp != nullptr);

    auto fp2 = dsn_file_open("tmp", O_RDONLY | O_BINARY, 0);
    EXPECT_TRUE(fp2 != nullptr);

    dsn_file_close(fp);
    dsn_file_close(fp2);
}
