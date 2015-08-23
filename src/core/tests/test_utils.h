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

# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/internal/task.h>
# include <dsn/internal/task_worker.h>

# ifndef _WIN32
# include <signal.h>
# endif

using namespace ::dsn;

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST_SERVER)
DEFINE_TASK_CODE_RPC(RPC_TEST_HASH, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)


class test_client :
    public ::dsn::serverlet<test_client>,
    public ::dsn::service_app    
{
public:
    test_client()
        : ::dsn::serverlet<test_client>("test-server", 7)
    {

    }

    void on_rpc_test_hash(const int& test_id, ::dsn::rpc_replier<std::string>& replier)
    {
        std::stringstream ss;
        ss << test_id << "."
           << ::dsn::task::get_current_worker()->name()
           ;

        std::string r = std::move(ss.str());
        replier(r);
    }

    ::dsn::error_code start(int argc, char** argv)
    {
        // server
        if (argc == 1)
        {
            register_async_rpc_handler(RPC_TEST_HASH, "rpc.test.hash", &test_client::on_rpc_test_hash);
        }

        // client
        else
        {
            testing::InitGoogleTest(&argc, argv);
            auto ret = RUN_ALL_TESTS();

            // exit without any destruction
# if defined(_WIN32)
            ::ExitProcess(0);
# else
            kill(getpid(), SIGKILL);
# endif
        }
        
        return ::dsn::ERR_OK;
    }

    void stop(bool cleanup = false)
    {

    }
};
