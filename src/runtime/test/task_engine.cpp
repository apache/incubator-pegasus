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

#include "runtime/task/task_engine.h"

#include <stdio.h>

#include "gtest/gtest.h"
#include "runtime/global_config.h"
#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "test_utils.h"
#include "utils/enum_helper.h"
#include "utils/threadpool_code.h"

namespace dsn {
class task_queue;
} // namespace dsn

using namespace ::dsn;

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FOR_TEST_1)
DEFINE_THREAD_POOL_CODE(THREAD_POOL_FOR_TEST_2)

TEST(core, task_engine)
{
    if (dsn::service_engine::instance().spec().tool == "simulator")
        return;
    service_node *node = task::get_current_node2();
    ASSERT_NE(nullptr, node);
    ASSERT_STREQ("client", node->full_name());

    task_engine *engine = node->computation();
    ASSERT_NE(nullptr, engine);

    ASSERT_TRUE(engine->is_started());
    std::vector<std::string> args;
    std::stringstream oss;
    engine->get_runtime_info("  ", args, oss);
    printf("%s\n", oss.str().c_str());

    std::vector<task_worker_pool *> &pools = engine->pools();
    for (size_t i = 0; i < pools.size(); ++i) {
        if (i == THREAD_POOL_DEFAULT || i == THREAD_POOL_TEST_SERVER ||
            i == THREAD_POOL_FOR_TEST_1 || i == THREAD_POOL_FOR_TEST_2) {
            ASSERT_NE(nullptr, pools[i]);
        }
    }

    task_worker_pool *pool1 = engine->get_pool(THREAD_POOL_FOR_TEST_1);
    ASSERT_NE(nullptr, pool1);
    ASSERT_EQ(pools[THREAD_POOL_FOR_TEST_1], pool1);
    const threadpool_spec &spec1 = pool1->spec();
    ASSERT_EQ("THREAD_POOL_FOR_TEST_1", spec1.name);
    ASSERT_EQ(engine, pool1->engine());
    ASSERT_EQ(task::get_current_node2(), pool1->node());
    std::vector<task_queue *> queues1 = pool1->queues();
    ASSERT_EQ(1u, queues1.size());
    std::vector<task_worker *> workers1 = pool1->workers();
    ASSERT_EQ(2u, workers1.size());

    task_worker_pool *pool2 = engine->get_pool(THREAD_POOL_FOR_TEST_2);
    ASSERT_NE(nullptr, pool2);
    ASSERT_EQ(pools[THREAD_POOL_FOR_TEST_2], pool2);
    const threadpool_spec &spec2 = pool2->spec();
    ASSERT_EQ("THREAD_POOL_FOR_TEST_2", spec2.name);
    ASSERT_EQ(engine, pool2->engine());
    ASSERT_EQ(task::get_current_node2(), pool2->node());
    std::vector<task_queue *> queues2 = pool2->queues();
    ASSERT_EQ(2u, queues2.size());
    std::vector<task_worker *> workers2 = pool2->workers();
    ASSERT_EQ(2u, workers2.size());
}
