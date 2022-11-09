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
 *     Unit-test for sim lock.
 *
 * Revision history:
 *     Nov., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "runtime/tool_api.h"
#include "runtime/task/task.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "utils/utils.h"
#include "utils/synchronize.h"
#include <gtest/gtest.h>
#include <thread>
#include "runtime/service_engine.h"
#include "runtime/task/task_engine.sim.h"
#include "runtime/scheduler.h"

TEST(tools_simulator, dsn_semaphore)
{
    if (dsn::task::get_current_worker() == nullptr)
        return;
    if (dsn::service_engine::instance().spec().semaphore_factory_name !=
        "dsn::tools::sim_semaphore_provider")
        return;
    dsn::zsemaphore s(2);
    s.wait();
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.wait(0));
    s.signal(1);
    s.wait();
}

TEST(tools_simulator, dsn_lock_nr)
{
    if (dsn::task::get_current_worker() == nullptr)
        return;
    if (dsn::service_engine::instance().spec().lock_nr_factory_name !=
        "dsn::tools::sim_lock_nr_provider")
        return;

    dsn::tools::sim_lock_nr_provider *s = new dsn::tools::sim_lock_nr_provider(nullptr);
    s->lock();
    s->unlock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    delete s;
}

TEST(tools_simulator, dsn_lock)
{
    if (dsn::task::get_current_worker() == nullptr)
        return;
    if (dsn::service_engine::instance().spec().lock_factory_name != "dsn::tools::sim_lock_provider")
        return;

    dsn::tools::sim_lock_provider *s = new dsn::tools::sim_lock_provider(nullptr);
    s->lock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    s->unlock();
    delete s;
}

namespace dsn {
namespace test {
typedef std::function<void()> system_callback;
}
}
TEST(tools_simulator, scheduler)
{
    if (dsn::task::get_current_worker() == nullptr)
        return;
    if (dsn::service_engine::instance().spec().tool != "simulator")
        return;

    dsn::tools::sim_worker_state *s =
        dsn::tools::scheduler::task_worker_ext::get(dsn::task::get_current_worker());
    dsn::utils::notify_event *evt = new dsn::utils::notify_event();
    dsn::test::system_callback callback = [evt, s](void) {
        evt->notify();
        s->is_continuation_ready = true;
        return;
    };
    dsn::tools::scheduler::instance().add_system_event(100, callback);
    dsn::tools::scheduler::instance().wait_schedule(true, false);
    evt->wait();
}
