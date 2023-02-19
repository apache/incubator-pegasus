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
 *     Unit-test for c service api.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "runtime/tool_api.h"
#include "aio/file_io.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "utils/zlocks.h"
#include "utils/utils.h"
#include "utils/filesystem.h"
#include <gtest/gtest.h>
#include <thread>
#include "utils/rand.h"
#include "runtime/service_engine.h"
#include "utils/flags.h"

DSN_DECLARE_string(tool);

using namespace dsn;

TEST(core, dsn_error)
{
    ASSERT_EQ(ERR_OK, dsn::error_code("ERR_OK"));
    ASSERT_STREQ("ERR_OK", ERR_OK.to_string());
}

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FOR_TEST)
TEST(core, dsn_threadpool_code)
{
    ASSERT_FALSE(dsn::threadpool_code::is_exist("THREAD_POOL_NOT_EXIST"));
    ASSERT_STREQ("THREAD_POOL_DEFAULT", THREAD_POOL_DEFAULT.to_string());
    ASSERT_EQ(THREAD_POOL_DEFAULT, dsn::threadpool_code("THREAD_POOL_DEFAULT"));
    ASSERT_LE(THREAD_POOL_DEFAULT, dsn::threadpool_code::max());

    ASSERT_STREQ("THREAD_POOL_FOR_TEST", THREAD_POOL_FOR_TEST.to_string());
    ASSERT_EQ(THREAD_POOL_FOR_TEST, dsn::threadpool_code("THREAD_POOL_FOR_TEST"));
    ASSERT_LE(THREAD_POOL_FOR_TEST, dsn::threadpool_code::max());

    ASSERT_LT(0, dsn::utils::get_current_tid());
}

DEFINE_TASK_CODE(TASK_CODE_COMPUTE_FOR_TEST, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_AIO(TASK_CODE_AIO_FOR_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(TASK_CODE_RPC_FOR_TEST, TASK_PRIORITY_LOW, THREAD_POOL_DEFAULT)
TEST(core, dsn_task_code)
{
    dsn_task_type_t type;
    dsn_task_priority_t pri;
    dsn::threadpool_code pool;

    ASSERT_EQ(TASK_CODE_INVALID, dsn::task_code::try_get("TASK_CODE_NOT_EXIST", TASK_CODE_INVALID));

    ASSERT_STREQ("TASK_TYPE_COMPUTE", enum_to_string(TASK_TYPE_COMPUTE));

    ASSERT_STREQ("TASK_PRIORITY_HIGH", enum_to_string(TASK_PRIORITY_HIGH));

    ASSERT_STREQ("TASK_CODE_COMPUTE_FOR_TEST",
                 dsn::task_code(TASK_CODE_COMPUTE_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_COMPUTE_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_COMPUTE_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_COMPUTE_FOR_TEST, dsn::task_code::max());
    dsn::task_spec *spec = dsn::task_spec::get(TASK_CODE_COMPUTE_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_COMPUTE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_HIGH, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_AIO_FOR_TEST", dsn::task_code(TASK_CODE_AIO_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_AIO_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_AIO_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_AIO_FOR_TEST, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_AIO_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_AIO, spec->type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST", dsn::task_code(TASK_CODE_RPC_FOR_TEST).to_string());
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST,
              dsn::task_code::try_get("TASK_CODE_RPC_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_RPC_FOR_TEST, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_RPC_FOR_TEST.code());
    ASSERT_EQ(TASK_TYPE_RPC_REQUEST, spec->type);
    ASSERT_EQ(TASK_PRIORITY_LOW, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST_ACK",
                 dsn::task_code(TASK_CODE_RPC_FOR_TEST_ACK).to_string());
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST_ACK,
              dsn::task_code::try_get("TASK_CODE_RPC_FOR_TEST_ACK", TASK_CODE_INVALID));
    ASSERT_LE(TASK_CODE_RPC_FOR_TEST_ACK, dsn::task_code::max());
    spec = dsn::task_spec::get(TASK_CODE_RPC_FOR_TEST_ACK.code());
    ASSERT_EQ(TASK_TYPE_RPC_RESPONSE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_LOW, spec->priority);
    ASSERT_EQ(THREAD_POOL_DEFAULT, spec->pool_code);

    spec = dsn::task_spec::get(TASK_CODE_COMPUTE_FOR_TEST.code());
    spec->pool_code = THREAD_POOL_FOR_TEST;
    spec->priority = TASK_PRIORITY_COMMON;
    ASSERT_EQ(TASK_TYPE_COMPUTE, spec->type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, spec->priority);
    ASSERT_EQ(THREAD_POOL_FOR_TEST, spec->pool_code);

    spec->pool_code = THREAD_POOL_DEFAULT;
    spec->priority = TASK_PRIORITY_HIGH;
}

TEST(core, dsn_config)
{
    ASSERT_TRUE(dsn_config_get_value_bool("apps.client", "run", false, "client run"));
    ASSERT_EQ(1u, dsn_config_get_value_uint64("apps.client", "count", 100, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));

    std::vector<const char *> buffers;
    dsn_config_get_all_keys("core.test", buffers);
    ASSERT_EQ(2, buffers.size());
    ASSERT_STREQ("count", buffers[0]);
    ASSERT_STREQ("run", buffers[1]);
}

TEST(core, dsn_exlock)
{
    if (dsn::service_engine::instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    {
        dsn::zlock l(false);
        ASSERT_TRUE(l.try_lock());
        l.unlock();
        l.lock();
        l.unlock();
    }
    {
        dsn::zlock l(true);
        ASSERT_TRUE(l.try_lock());
        ASSERT_TRUE(l.try_lock());
        l.unlock();
        l.unlock();
        l.lock();
        l.lock();
        l.unlock();
        l.unlock();
    }
}

TEST(core, dsn_rwlock)
{
    if (dsn::service_engine::instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    dsn::zrwlock_nr l;
    l.lock_read();
    l.unlock_read();
    l.lock_write();
    l.unlock_write();
}

TEST(core, dsn_semaphore)
{
    if (dsn::service_engine::instance().spec().semaphore_factory_name ==
        "dsn::tools::sim_semaphore_provider")
        return;
    dsn::zsemaphore s(2);
    s.wait();
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.wait(10));
    s.signal(1);
    s.wait();
}

TEST(core, dsn_env)
{
    if (dsn::service_engine::instance().spec().tool == "simulator")
        return;
    uint64_t now1 = dsn_now_ns();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    uint64_t now2 = dsn_now_ns();
    ASSERT_LE(now1 + 1000000, now2);
    uint64_t r = rand::next_u64(100, 200);
    ASSERT_LE(100, r);
    ASSERT_GE(200, r);
}

TEST(core, dsn_system)
{
    ASSERT_TRUE(tools::is_engine_ready());
    tools::tool_app *tool = tools::get_current_tool();
    ASSERT_EQ(tool->name(), FLAGS_tool);

    int app_count = 5;
    int type_count = 1;
    if (tool->get_service_spec().enable_default_app_mimic) {
        app_count++;
        type_count++;
    }

    {
        std::vector<service_app *> apps;
        service_app::get_all_service_apps(&apps);
        ASSERT_EQ(app_count, apps.size());
        std::map<std::string, int> type_to_count;
        for (int i = 0; i < apps.size(); ++i) {
            type_to_count[apps[i]->info().type] += 1;
        }

        ASSERT_EQ(type_count, static_cast<int>(type_to_count.size()));
        ASSERT_EQ(5, type_to_count["test"]);
    }
}
