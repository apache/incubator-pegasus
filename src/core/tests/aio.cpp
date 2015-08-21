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

# include <dsn/internal/aio_provider.h>
# include <gtest/gtest.h>
# include <dsn/service_api_cpp.h>

using namespace ::dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT);

TEST(core, aio)
{
    const char* buffer = "hello, world";
    int len = (int)strlen(buffer);

    // write
    auto fp = dsn_file_open("tmp", O_RDWR | O_CREAT | O_BINARY, 0666);
    
    auto t1 = ::dsn::file::write(fp, buffer, len, 0, LPC_AIO_TEST, nullptr, nullptr, 0);
    auto t2 = ::dsn::file::write(fp, buffer, len, len, LPC_AIO_TEST, nullptr, nullptr, 0);
    
    bool r = t1->wait();
    EXPECT_TRUE(r);

    r = t2->wait();
    EXPECT_TRUE(r);

    t1 = ::dsn::file::write(fp, buffer, len, 0, LPC_AIO_TEST, nullptr, nullptr, 0);
    t2 = ::dsn::file::write(fp, buffer, len, len, LPC_AIO_TEST, nullptr, nullptr, 0);

    r = t1->wait();
    EXPECT_TRUE(r);

    r = t2->wait();
    EXPECT_TRUE(r);

    auto err = dsn_file_close(fp);
    EXPECT_TRUE(err == ERR_OK);

    // read
    char* buffer2 = (char*)alloca((size_t)len);
    fp = dsn_file_open("tmp", O_RDONLY | O_BINARY, 0666);

    t1 = ::dsn::file::read(fp, buffer2, len, 0, LPC_AIO_TEST, nullptr, nullptr, 0);
    t2 = ::dsn::file::read(fp, buffer2, len, len, LPC_AIO_TEST, nullptr, nullptr, 0);

    r = t1->wait();
    EXPECT_TRUE(r);

    r = t2->wait();
    EXPECT_TRUE(r);

    EXPECT_TRUE(memcmp(buffer, buffer2, len) == 0);

    t1 = ::dsn::file::read(fp, buffer2, len, 0, LPC_AIO_TEST, nullptr, nullptr, 0);
    t2 = ::dsn::file::read(fp, buffer2, len, len, LPC_AIO_TEST, nullptr, nullptr, 0);

    r = t1->wait();
    EXPECT_TRUE(r);

    r = t2->wait();
    EXPECT_TRUE(r);

    EXPECT_TRUE(memcmp(buffer, buffer2, len) == 0);

    err = dsn_file_close(fp);
    EXPECT_TRUE(err == ERR_OK);
}
