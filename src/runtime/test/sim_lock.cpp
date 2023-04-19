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

// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <functional>
#include <string>

#include "runtime/global_config.h"
#include "runtime/scheduler.h"
#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "runtime/task/task_engine.sim.h"
#include "utils/synchronize.h"
#include "utils/zlocks.h"

namespace pegasus {

TEST(tools_simulator, dsn_semaphore)
{
    if (task::get_current_worker() == nullptr)
        return;
    if (service_engine::instance().spec().semaphore_factory_name !=
        "pegasus::tools::sim_semaphore_provider")
        return;
    zsemaphore s(2);
    s.wait();
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.wait(0));
    s.signal(1);
    s.wait();
}

TEST(tools_simulator, dsn_lock_nr)
{
    if (task::get_current_worker() == nullptr)
        return;
    if (service_engine::instance().spec().lock_nr_factory_name !=
        "pegasus::tools::sim_lock_nr_provider")
        return;

    tools::sim_lock_nr_provider *s = new tools::sim_lock_nr_provider(nullptr);
    s->lock();
    s->unlock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    delete s;
}

TEST(tools_simulator, dsn_lock)
{
    if (task::get_current_worker() == nullptr)
        return;
    if (service_engine::instance().spec().lock_factory_name != "pegasus::tools::sim_lock_provider")
        return;

    tools::sim_lock_provider *s = new tools::sim_lock_provider(nullptr);
    s->lock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    s->unlock();
    delete s;
}

typedef std::function<void()> system_callback;

TEST(tools_simulator, scheduler)
{
    if (task::get_current_worker() == nullptr)
        return;
    if (service_engine::instance().spec().tool != "simulator")
        return;

    tools::sim_worker_state *s = tools::scheduler::task_worker_ext::get(task::get_current_worker());
    utils::notify_event *evt = new utils::notify_event();
    system_callback callback = [evt, s](void) {
        evt->notify();
        s->is_continuation_ready = true;
        return;
    };
    tools::scheduler::instance().add_system_event(100, callback);
    tools::scheduler::instance().wait_schedule(true, false);
    evt->wait();
}

} // namespace pegasus
