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
 *     Rpc performance test
 *
 * Revision history:
 *     2016-01-05, Tianyi Wang, first version
 */
#include <dsn/cpp/address.h>
#include <dsn/internal/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/internal/priority_queue.h>
#include "../core/group_address.h"
#include "test_utils.h"
#include <boost/lexical_cast.hpp>

TEST(core, rpc_perf_test)
{
    ::dsn::rpc_address localhost("localhost", 20101);

    ::dsn::rpc_read_stream response;
    std::mutex lock;
    for (auto concurrency : {10, 100, 1000, 10000})
    {
        std::atomic_int remain_concurrency;
        remain_concurrency = concurrency;
        size_t total_query_count = 1000000;
        std::chrono::steady_clock clock;
        auto tic = clock.now();
        for (auto remain_query_count = total_query_count; remain_query_count--;)
        {
            while(true)
            {
                if (remain_concurrency.fetch_sub(1, std::memory_order_relaxed) <= 0)
                {
                    remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    break;
                }
            }
            ::dsn::rpc::call(
                localhost,
                RPC_TEST_HASH,
                0,
                nullptr,
                [&remain_concurrency](error_code ec, const std::string&)
                {
                    ec.end_tracking();
                    remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                }
            );
        }
        while(remain_concurrency != concurrency)
        {
            ;
        }
        auto toc = clock.now();
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(toc - tic).count();
        std::cout << "rpc perf test: concurrency = " << concurrency
            << " throughput = " << total_query_count * 1000000llu / time_us << "call/sec" << std::endl;
    }

}