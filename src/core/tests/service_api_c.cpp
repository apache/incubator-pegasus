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

# include <dsn/service_api_c.h>
# include <dsn/cpp/auto_codes.h>
# include <gtest/gtest.h>

using namespace dsn;

TEST(core, dsn_error)
{
    ASSERT_EQ(ERR_OK, dsn_error_register("ERR_OK"));
    ASSERT_STREQ("ERR_OK", dsn_error_to_string(ERR_OK));
}

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FOR_TEST);
TEST(core, dsn_threadpool_code)
{
    ASSERT_EQ(THREAD_POOL_INVALID, dsn_threadpool_code_from_string("THREAD_POOL_NOT_EXIST", THREAD_POOL_INVALID));

    ASSERT_STREQ("THREAD_POOL_DEFAULT", dsn_threadpool_code_to_string(THREAD_POOL_DEFAULT));
    ASSERT_EQ(THREAD_POOL_DEFAULT, dsn_threadpool_code_from_string("THREAD_POOL_DEFAULT", THREAD_POOL_INVALID));
    ASSERT_LT(THREAD_POOL_DEFAULT, dsn_threadpool_code_max());

    ASSERT_STREQ("THREAD_POOL_FOR_TEST", dsn_threadpool_code_to_string(THREAD_POOL_FOR_TEST));
    ASSERT_EQ(THREAD_POOL_FOR_TEST, dsn_threadpool_code_from_string("THREAD_POOL_FOR_TEST", THREAD_POOL_INVALID));
    ASSERT_LT(THREAD_POOL_FOR_TEST, dsn_threadpool_code_max());
}

DEFINE_TASK_CODE(TASK_CODE_COMPUTE_FOR_TEST, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);
DEFINE_TASK_CODE_AIO(TASK_CODE_AIO_FOR_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT);
DEFINE_TASK_CODE_RPC(TASK_CODE_RPC_FOR_TEST, TASK_PRIORITY_LOW, THREAD_POOL_DEFAULT);
TEST(core, dsn_task_code)
{
    dsn_task_type_t type;
    dsn_task_priority_t pri;
    dsn_threadpool_code_t pool;

    ASSERT_EQ(TASK_CODE_INVALID, dsn_task_code_from_string("TASK_CODE_NOT_EXIST", TASK_CODE_INVALID));

    ASSERT_STREQ("TASK_TYPE_COMPUTE", dsn_task_type_to_string(TASK_TYPE_COMPUTE));

    ASSERT_STREQ("TASK_PRIORITY_HIGH", dsn_task_priority_to_string(TASK_PRIORITY_HIGH));

    ASSERT_STREQ("TASK_CODE_COMPUTE_FOR_TEST", dsn_task_code_to_string(TASK_CODE_COMPUTE_FOR_TEST));
    ASSERT_EQ(TASK_CODE_COMPUTE_FOR_TEST, dsn_task_code_from_string("TASK_CODE_COMPUTE_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LT(TASK_CODE_COMPUTE_FOR_TEST, dsn_task_code_max());
    dsn_task_code_query(TASK_CODE_COMPUTE_FOR_TEST, &type, &pri, &pool);
    ASSERT_EQ(TASK_TYPE_COMPUTE, type);
    ASSERT_EQ(TASK_PRIORITY_HIGH, pri);
    ASSERT_EQ(THREAD_POOL_DEFAULT, pool);

    ASSERT_STREQ("TASK_CODE_AIO_FOR_TEST", dsn_task_code_to_string(TASK_CODE_AIO_FOR_TEST));
    ASSERT_EQ(TASK_CODE_AIO_FOR_TEST, dsn_task_code_from_string("TASK_CODE_AIO_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LT(TASK_CODE_AIO_FOR_TEST, dsn_task_code_max());
    dsn_task_code_query(TASK_CODE_AIO_FOR_TEST, &type, &pri, &pool);
    ASSERT_EQ(TASK_TYPE_AIO, type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, pri);
    ASSERT_EQ(THREAD_POOL_DEFAULT, pool);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST", dsn_task_code_to_string(TASK_CODE_RPC_FOR_TEST));
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST, dsn_task_code_from_string("TASK_CODE_RPC_FOR_TEST", TASK_CODE_INVALID));
    ASSERT_LT(TASK_CODE_RPC_FOR_TEST, dsn_task_code_max());
    dsn_task_code_query(TASK_CODE_RPC_FOR_TEST, &type, &pri, &pool);
    ASSERT_EQ(TASK_TYPE_RPC_REQUEST, type);
    ASSERT_EQ(TASK_PRIORITY_LOW, pri);
    ASSERT_EQ(THREAD_POOL_DEFAULT, pool);

    ASSERT_STREQ("TASK_CODE_RPC_FOR_TEST_ACK", dsn_task_code_to_string(TASK_CODE_RPC_FOR_TEST_ACK));
    ASSERT_EQ(TASK_CODE_RPC_FOR_TEST_ACK, dsn_task_code_from_string("TASK_CODE_RPC_FOR_TEST_ACK", TASK_CODE_INVALID));
    ASSERT_LT(TASK_CODE_RPC_FOR_TEST_ACK, dsn_task_code_max());
    dsn_task_code_query(TASK_CODE_RPC_FOR_TEST_ACK, &type, &pri, &pool);
    ASSERT_EQ(TASK_TYPE_RPC_RESPONSE, type);
    ASSERT_EQ(TASK_PRIORITY_LOW, pri);
    ASSERT_EQ(THREAD_POOL_DEFAULT, pool);

    dsn_task_code_set_threadpool(TASK_CODE_COMPUTE_FOR_TEST, THREAD_POOL_FOR_TEST);
    dsn_task_code_set_priority(TASK_CODE_COMPUTE_FOR_TEST, TASK_PRIORITY_COMMON);
    dsn_task_code_query(TASK_CODE_COMPUTE_FOR_TEST, &type, &pri, &pool);
    ASSERT_EQ(TASK_TYPE_COMPUTE, type);
    ASSERT_EQ(TASK_PRIORITY_COMMON, pri);
    ASSERT_EQ(THREAD_POOL_FOR_TEST, pool);

    dsn_task_code_set_threadpool(TASK_CODE_COMPUTE_FOR_TEST, THREAD_POOL_DEFAULT);
    dsn_task_code_set_priority(TASK_CODE_COMPUTE_FOR_TEST, TASK_PRIORITY_HIGH);
}

TEST(core, dsn_config)
{
    ASSERT_STREQ("client", dsn_config_get_value_string("apps.client", "name", "unknown", "client name"));
    ASSERT_TRUE(dsn_config_get_value_bool("apps.client", "run", false, "client run"));
    ASSERT_EQ(1u, dsn_config_get_value_uint64("apps.client", "count", 100, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));
    ASSERT_EQ(1.0, dsn_config_get_value_double("apps.client", "count", 100.0, "client count"));
    const char* buffers[100];
    int buffer_count = 100;
    ASSERT_EQ(2, dsn_config_get_all_keys("core.test", buffers, &buffer_count));
    ASSERT_EQ(2, buffer_count);
    ASSERT_STREQ("count", buffers[0]);
    ASSERT_STREQ("run", buffers[1]);
    buffer_count = 1;
    ASSERT_EQ(2, dsn_config_get_all_keys("core.test", buffers, &buffer_count));
    ASSERT_EQ(1, buffer_count);
    ASSERT_STREQ("count", buffers[0]);
}

TEST(core, dsn_coredump)
{
}

TEST(core, dsn_crc32)
{
}

TEST(core, dsn_task)
{
}

TEST(core, dsn_exlock)
{
}

TEST(core, dsn_rwlock)
{
}

TEST(core, dsn_semaphore)
{
}

TEST(core, dsn_rpc)
{
}

TEST(core, dsn_file)
{
}

TEST(core, dsn_env)
{
}

TEST(core, dsn_system)
{
}

