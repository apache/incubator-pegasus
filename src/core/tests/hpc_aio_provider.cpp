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
 *     Unit-test for hpc aio provider.
 *
 * Revision history:
 *     Nov., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <iostream>
#include <fstream>
#include <cstdio>

#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/service_api_c.h>
#include <dsn/tool-api/task.h>
#include "../core/disk_engine.h"
#include "test_utils.h"
#include "../tools/hpc/hpc_aio_provider.h"

using namespace ::dsn;

DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER);

TEST(tools_hpc, aio)
{
    if (nullptr == task::get_current_disk())
        return;

    std::remove("test_hpc_aio.tmp"); // delete file

    // write to file
    char buffer[11];
    int count = 10;
    sprintf(buffer, "abcdefghij");
    dsn_handle_t file = dsn_file_open("test_hpc_aio.tmp", O_RDWR | O_CREAT, 0666);
    dsn::task_tracker *tracker = new dsn::task_tracker(13);

    dsn::aio_task *callback = new dsn::aio_task(LPC_AIO_TEST, nullptr, 0);
    callback->add_ref();

    callback->set_tracker(tracker);
    callback->aio()->buffer = (char *)buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = 0;
    callback->aio()->type = ::dsn::AIO_Write;
    ::dsn::task::get_current_disk()->write(callback);

    callback->wait();

    dsn::error_code err = dsn_file_close(file);
    EXPECT_TRUE(err == ERR_OK);

    callback->release_ref();
    delete tracker;

    // read from file
    char buffer_read[11];
    buffer_read[10] = '\0';
    file = dsn_file_open("test_hpc_aio.tmp", O_RDWR | O_CREAT, 0666);
    tracker = new dsn::task_tracker(13);
    dsn::aio_task *callback_read = new dsn::aio_task(LPC_AIO_TEST, nullptr, 0);
    callback_read->add_ref();

    callback_read->set_tracker((dsn::task_tracker *)tracker);
    callback_read->aio()->buffer = (char *)buffer_read;
    callback_read->aio()->buffer_size = count;
    callback_read->aio()->engine = nullptr;
    callback_read->aio()->file = file;
    callback_read->aio()->file_offset = 0;
    callback_read->aio()->type = ::dsn::AIO_Read;
    ::dsn::task::get_current_disk()->read(callback_read);

    callback_read->wait();

    EXPECT_STREQ(buffer, buffer_read);
    err = dsn_file_close(file);
    EXPECT_TRUE(err == ERR_OK);
    callback_read->release_ref();

    delete tracker;
    // invalid operation
}

TEST(tools_hpc, aio_create)
{
    ::dsn::tools::hpc_aio_provider *p = new tools::hpc_aio_provider(nullptr, nullptr);
    delete p;
}

TEST(tools_hpc, aio_invalid_type)
{
    if (nullptr == task::get_current_disk())
        return;

    std::remove("test_hpc_aio2.tmp"); // delete file

    // invalid io
    char buffer[10];
    int count = 10;
    sprintf(buffer, "abcdefghi");
    dsn_handle_t file = dsn_file_open("test_hpc_aio2.tmp", O_RDWR | O_CREAT, 0666);
    dsn::task_tracker *tracker = new dsn::task_tracker(13);

    dsn::aio_task *callback = new dsn::aio_task(LPC_AIO_TEST, nullptr, 0);
    callback->add_ref();

    callback->set_tracker(tracker);
    callback->aio()->buffer = (char *)buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = 0;
    callback->aio()->type = ::dsn::AIO_Invalid;
    ::dsn::task::get_current_disk()->write(callback);

    callback->wait();

    dsn::error_code err = dsn_file_close(file);
    EXPECT_TRUE(err == ERR_OK);

    callback->release_ref();
    delete tracker;
}
