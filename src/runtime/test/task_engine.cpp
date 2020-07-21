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
 *     Unit-test for task engine.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "runtime/task/task_engine.h"
#include "test_utils.h"
#include <dsn/tool_api.h>
#include <gtest/gtest.h>
#include <sstream>

using namespace ::dsn;

class admission_controller_for_test : public admission_controller
{
public:
    admission_controller_for_test(task_queue *q, std::vector<std::string> &sargs)
        : admission_controller(q, sargs), _args(sargs)
    {
    }

    virtual ~admission_controller_for_test() {}

    virtual bool is_task_accepted(task *task) { return true; }

    const std::vector<std::string> &arguments() const { return _args; }

private:
    std::vector<std::string> _args;
};

void task_engine_module_init()
{
    tools::register_component_provider<admission_controller_for_test>(
        "dsn::tools::admission_controller_for_test");
}

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
    ASSERT_EQ("dsn::tools::admission_controller_for_test", spec1.admission_controller_factory_name);
    ASSERT_EQ("this is test argument", spec1.admission_controller_arguments);
    ASSERT_EQ(engine, pool1->engine());
    ASSERT_EQ(task::get_current_node2(), pool1->node());
    std::vector<task_queue *> queues1 = pool1->queues();
    ASSERT_EQ(1u, queues1.size());
    std::vector<task_worker *> workers1 = pool1->workers();
    ASSERT_EQ(2u, workers1.size());
    std::vector<admission_controller *> controllers1 = pool1->controllers();
    ASSERT_EQ(1u, controllers1.size());
    admission_controller_for_test *c1 =
        dynamic_cast<admission_controller_for_test *>(controllers1[0]);
    ASSERT_NE(nullptr, c1);
    const std::vector<std::string> &a1 = c1->arguments();
    ASSERT_EQ(4u, a1.size());
    ASSERT_EQ("this", a1[0]);

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
    std::vector<admission_controller *> controllers2 = pool2->controllers();
    ASSERT_EQ(2u, controllers2.size());
    ASSERT_EQ(nullptr, controllers2[0]);
    ASSERT_EQ(nullptr, controllers2[1]);
}

/*
TEST(core, task_engine)
{
    task_engine engine(task::get_current_node2());

    std::list<dsn_threadpool_code_t> pool_codes;
    pool_codes.push_back(THREAD_POOL_FOR_TEST_1);
    pool_codes.push_back(THREAD_POOL_FOR_TEST_2);
    engine.create(pool_codes);
    ASSERT_FALSE(engine.is_started());
    ASSERT_EQ(task::get_current_node2(), engine.node());

    engine.start();
    ASSERT_TRUE(engine.is_started());
    std::vector<std::string> args;
    std::stringstream oss;
    engine.get_runtime_info("  ", args, oss);
    printf("%s\n", oss.str().c_str());

    std::vector<task_worker_pool*>& pools = engine.pools();
    for (size_t i = 0; i < pools.size(); ++i)
    {
        if (i == THREAD_POOL_FOR_TEST_1 ||
            i == THREAD_POOL_FOR_TEST_2)
        {
            ASSERT_NE(nullptr, pools[i]);
        }
        else
        {
            ASSERT_EQ(nullptr, pools[i]);
        }
    }

    task_worker_pool* pool1 = engine.get_pool(THREAD_POOL_FOR_TEST_1);
    ASSERT_NE(nullptr, pool1);
    ASSERT_EQ(pools[THREAD_POOL_FOR_TEST_1], pool1);
    const threadpool_spec& spec1 = pool1->spec();
    ASSERT_EQ("THREAD_POOL_FOR_TEST_1", spec1.name);
    ASSERT_EQ("dsn::tools::admission_controller_for_test", spec1.admission_controller_factory_name);
    ASSERT_EQ("this is test argument", spec1.admission_controller_arguments);
    ASSERT_EQ(&engine, pool1->engine());
    ASSERT_EQ(task::get_current_node2(), pool1->node());
    std::vector<task_queue*> queues1 = pool1->queues();
    ASSERT_EQ(1u, queues1.size());
    std::vector<task_worker*> workers1 = pool1->workers();
    ASSERT_EQ(2u, workers1.size());
    std::vector<admission_controller*> controllers1 = pool1->controllers();
    ASSERT_EQ(1u, controllers1.size());
    admission_controller_for_test* c1 =
dynamic_cast<admission_controller_for_test*>(controllers1[0]);
    ASSERT_NE(nullptr, c1);
    const std::vector<std::string>& a1 = c1->arguments();
    ASSERT_EQ(4u, a1.size());
    ASSERT_EQ("this", a1[0]);

    task_worker_pool* pool2 = engine.get_pool(THREAD_POOL_FOR_TEST_2);
    ASSERT_NE(nullptr, pool2);
    ASSERT_EQ(pools[THREAD_POOL_FOR_TEST_2], pool2);
    const threadpool_spec& spec2 = pool2->spec();
    ASSERT_EQ("THREAD_POOL_FOR_TEST_2", spec2.name);
    ASSERT_EQ(&engine, pool2->engine());
    ASSERT_EQ(task::get_current_node2(), pool2->node());
    std::vector<task_queue*> queues2 = pool2->queues();
    ASSERT_EQ(2u, queues2.size());
    std::vector<task_worker*> workers2 = pool2->workers();
    ASSERT_EQ(2u, workers2.size());
    std::vector<admission_controller*> controllers2 = pool2->controllers();
    ASSERT_EQ(2u, controllers2.size());
    ASSERT_EQ(nullptr, controllers2[0]);
    ASSERT_EQ(nullptr, controllers2[1]);
}
*/
