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

#include <gtest/gtest.h>
#include "runtime/pipeline.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

namespace dsn {

TEST(pipeline_test, pause)
{
    struct mock_when : pipeline::when<>
    {
        void run() override { repeat(1_s); }
    };

    task_tracker tracker;

    {
        pipeline::base base;
        ASSERT_TRUE(base.paused());

        base.pause();
        ASSERT_TRUE(base.paused());

        mock_when s1;
        base.thread_pool(LPC_MUTATION_LOG_PENDING_TIMER).task_tracker(&tracker);
        base.from(s1);

        {
            base.run_pipeline();
            ASSERT_FALSE(base.paused());

            base.pause();
            ASSERT_TRUE(base.paused());

            base.wait_all();
        }
    }
}

TEST(pipeline_test, link_pipe)
{
    task_tracker tracker;

    struct mock_when : pipeline::when<>
    {
        void run() override { repeat(1_s); }
    };

    struct stage2 : pipeline::when<>, pipeline::result<>
    {
        void run() override { step_down_next_stage(); }
    };

    {
        pipeline::base base1;
        mock_when s1;
        base1.thread_pool(LPC_MUTATION_LOG_PENDING_TIMER).task_tracker(&tracker);
        base1.from(s1);

        // base2 executes s2, then executes s1 in another pipeline.
        pipeline::base base2;
        stage2 s2;
        base2.thread_pool(LPC_REPLICA_SERVER_DELAY_START).task_tracker(&tracker);
        base2.from(s2).link(s1);

        base2.run_pipeline();

        base1.pause();
        base2.pause();

        base2.wait_all();
    }
}

TEST(pipeline_test, verify_link_and_fork)
{
    struct mock_stage : pipeline::when<>, pipeline::result<>
    {
        void run() override { step_down_next_stage(); }
    };
    task_tracker tracker;
    {
        pipeline::base base;
        base.thread_pool(LPC_MUTATION_LOG_PENDING_TIMER).task_tracker(&tracker).thread_hash(1);
        mock_stage s1;
        mock_stage s2;
        base.from(s1).link(s2);
        ASSERT_EQ(s1.__conf.tracker, &tracker);
        ASSERT_EQ(s1.__conf.thread_hash, 1);
        ASSERT_EQ(s1.__conf.thread_pool_code, LPC_MUTATION_LOG_PENDING_TIMER);
        ASSERT_EQ(s1.__pipeline, &base);
        ASSERT_EQ(s2.__conf.tracker, &tracker);
        ASSERT_EQ(s2.__conf.thread_hash, 1);
        ASSERT_EQ(s2.__conf.thread_pool_code, LPC_MUTATION_LOG_PENDING_TIMER);
        ASSERT_EQ(s2.__pipeline, &base);
        mock_stage s3;
        base.fork(s3, LPC_REPLICA_SERVER_DELAY_START, 2).link(s2);
        ASSERT_EQ(s3.__conf.thread_pool_code, LPC_REPLICA_SERVER_DELAY_START);
        ASSERT_EQ(s3.__conf.tracker, &tracker);
        ASSERT_EQ(s3.__conf.thread_hash, 2);
        ASSERT_EQ(s3.__pipeline, &base);
        ASSERT_EQ(s2.__conf.tracker, &tracker);
        ASSERT_EQ(s2.__conf.thread_hash, 1);
        ASSERT_EQ(s2.__conf.thread_pool_code, LPC_MUTATION_LOG_PENDING_TIMER);
        ASSERT_EQ(s2.__pipeline, &base);
    }
}

} // namespace dsn
