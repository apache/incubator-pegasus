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
 * Description:
 *     Rpc performance test
 *
 * Revision history:
 *     2016-01-05, Tianyi Wang, first version
 */
#include <dsn/cpp/address.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include "test_utils.h"
#include <boost/lexical_cast.hpp>

TEST(core, rpc_perf_test)
{
    rpc_address localhost("localhost", 20101);

    rpc_read_stream response;
    std::mutex lock;
    for (auto concurrency : {10, 100, 1000, 10000}) {
        std::atomic_int remain_concurrency;
        remain_concurrency = concurrency;
        size_t total_query_count = 1000000;
        std::chrono::steady_clock clock;
        auto tic = clock.now();
        for (auto remain_query_count = total_query_count; remain_query_count--;) {
            while (true) {
                if (remain_concurrency.fetch_sub(1, std::memory_order_relaxed) <= 0) {
                    remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                } else {
                    break;
                }
            }
            rpc::call(localhost,
                      RPC_TEST_HASH,
                      0,
                      nullptr,
                      [&remain_concurrency](error_code ec, const std::string &) {
                          ec.end_tracking();
                          remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                      });
        }
        while (remain_concurrency != concurrency) {
            ;
        }
        auto toc = clock.now();
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(toc - tic).count();
        std::cout << "rpc perf test: concurrency = " << concurrency
                  << " throughput = " << total_query_count * 1000000llu / time_us << "call/sec"
                  << std::endl;
    }
}

TEST(core, rpc_perf_test_sync)
{
    rpc_address localhost("localhost", 20101);

    std::chrono::steady_clock clock;
    auto tic = clock.now();

    auto round = 100000;
    auto concurrency = 10;
    auto total_query_count = round * concurrency;

    std::vector<task_ptr> tasks;
    for (auto i = 0; i < round; i++) {
        for (auto j = 0; j < concurrency; j++) {
            auto req = 0;
            auto task = rpc::call(
                localhost, RPC_TEST_HASH, req, nullptr, [](error_code err, std::string &&result) {
                    // nothing to do
                });

            tasks.push_back(task);
        }

        for (auto &t : tasks)
            t->wait();

        tasks.clear();
    }

    auto toc = clock.now();
    auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(toc - tic).count();
    std::cout << "rpc-sync perf test: throughput = " << total_query_count * 1000000llu / time_us
              << " #/s, avg latency = " << time_us / total_query_count << " us" << std::endl;
}

TEST(core, lpc_perf_test_sync)
{
    std::chrono::steady_clock clock;
    auto tic = clock.now();

    auto round = 1000000;
    auto concurrency = 10;
    auto total_query_count = round * concurrency;
    std::vector<task_ptr> tasks;
    std::vector<std::string> results;
    for (auto i = 0; i < round; i++) {
        results.resize(concurrency);
        for (auto j = 0; j < concurrency; j++) {
            auto task = tasking::enqueue(LPC_TEST_HASH, nullptr, [&results, j]() {
                std::string r = service_app::current_service_app_info().data_dir;
                results[j] = std::move(r);
            });
            tasks.push_back(task);
        }

        for (auto &t : tasks)
            t->wait();

        tasks.clear();
    }

    auto toc = clock.now();
    auto time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(toc - tic).count();
    std::cout << "lpc-sync perf test: throughput = " << total_query_count * 1000000000llu / time_ns
              << " #/s, avg latency = " << time_ns / total_query_count << " ns" << std::endl;
}
