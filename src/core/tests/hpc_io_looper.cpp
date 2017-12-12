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
 *     Unit-test for io_looper.
 *
 * Revision history:
 *     Nov., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/utility/priority_queue.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/tool-api/group_address.h>
#include "../core/service_engine.h"
#include "test_utils.h"
#include "../tools/hpc/io_looper.h"

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_SERVER_2);
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST_2, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER_2);
DEFINE_TASK_CODE_AIO(LPC_AIO_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER);

#ifdef __linux__

#include <sys/eventfd.h>
#include <libaio.h>

namespace dsn {
namespace test {
class io_looper_for_test : public dsn::tools::io_looper
{
public:
    io_looper_for_test() : io_looper() {}
    virtual ~io_looper_for_test() {}
public:
    virtual bool is_shared_timer_queue() override
    {
        if (io_looper::is_shared_timer_queue())
            return false;
        else
            return true;
    }
};
}
}

TEST(tools_hpc, io_looper)
{
    dsn::test::io_looper_for_test *io_looper = new dsn::test::io_looper_for_test();
    io_looper->start(::dsn::task::get_current_node(), 1);
    EXPECT_FALSE(io_looper->is_shared_timer_queue());

    int event_fd = eventfd(0, EFD_NONBLOCK);

    io_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    auto ret = io_setup(128, &ctx); // 128 concurrent events
    EXPECT_TRUE(ret == 0);

    utils::notify_event *evt = new utils::notify_event();
    dsn::tools::io_loop_callback callback =
        [ctx, event_fd, evt](int native_error, uint32_t io_size, uintptr_t lolp_or_events) {
            int64_t finished_aio = 0;

            if (read(event_fd, &finished_aio, sizeof(finished_aio)) != sizeof(finished_aio)) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return;
                dassert(false,
                        "read number of aio completion from eventfd failed, err = %s",
                        strerror(errno));
            }
            EXPECT_EQ(1, finished_aio);

            struct io_event events[1];
            int ret;

            struct timespec tms;
            tms.tv_sec = 0;
            tms.tv_nsec = 0;

            ret = io_getevents(ctx, 1, 1, events, &tms);
            EXPECT_EQ(1, ret);
            EXPECT_EQ(0, static_cast<int>(events[0].res2));
            EXPECT_EQ(10, static_cast<int>(events[0].res));
            evt->notify();
        };

    auto err =
        io_looper->bind_io_handle((dsn_handle_t)(intptr_t)event_fd, &callback, EPOLLIN | EPOLLET);
    EXPECT_EQ(ERR_OK, err);

    std::remove("test_io_looper.tmp"); // delete file
    std::ofstream myfile;
    myfile.open("test_io_looper.tmp");
    myfile << "abcdefghi$";
    myfile.close();
    int fd = ::open("test_io_looper.tmp", O_RDWR, 0666);
    char buffer[11];
    memset(buffer, 0, sizeof(buffer));

    struct iocb *cb = new iocb;
    memset(cb, 0, sizeof(iocb));
    io_prep_pread(cb, fd, buffer, 10, 0);
    io_set_eventfd(cb, event_fd);
    ret = io_submit(ctx, 1, &cb);
    EXPECT_EQ(1, ret);

    evt->wait();
    EXPECT_STREQ("abcdefghi$", buffer);

    err = io_looper->unbind_io_handle((dsn_handle_t)(intptr_t)event_fd, &callback);
    EXPECT_EQ(dsn::ERR_OK, err);

    delete io_looper;
    delete cb;
    delete evt;
}

void timer_callback(void *param)
{
    int *a = (int *)param;
    EXPECT_EQ(1, *a);
    *a = 2;
}

TEST(tools_hpc, io_looper_timer)
{
    if (dsn::service_engine::fast_instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;

    dsn::test::io_looper_for_test *io_looper = new dsn::test::io_looper_for_test();
    io_looper->start(::dsn::task::get_current_node(), 1);

    int *a = new int;
    *a = 1;
    dsn::task_c *t = new dsn::task_c(LPC_AIO_TEST, timer_callback, a, nullptr);
    t->add_ref();

    t->set_delay(1000);
    t->add_ref();
    io_looper->add_timer(t);
    t->wait();

    EXPECT_EQ(2, *a);

    t->release_ref();

    io_looper->stop();
    delete io_looper;
}

#endif // ifdef __linux__
