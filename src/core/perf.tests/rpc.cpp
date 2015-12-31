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
    for (auto concurrency : {100, 1000, 10000})
    {
        std::atomic_int remain_concurrency;
        remain_concurrency = concurrency;
        auto total_query_count = 20000;
        std::chrono::steady_clock clock;
        auto tic = clock.now();
        for (;total_query_count --;)
        {
            while(true)
            {
                if (remain_concurrency.fetch_sub(1, std::memory_order_acquire) <= 0)
                {
                    remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    break;
                }
            }
            ::dsn::rpc::call_typed<int, std::string>(
                localhost,
                RPC_TEST_HASH,
                0,
                nullptr,
                [&remain_concurrency](error_code ec, const std::string&, void*)
                {
                    ec.end_tracking();
                    remain_concurrency.fetch_add(1, std::memory_order_relaxed);
                },
                nullptr
            );
        }
        while(remain_concurrency != concurrency)
        {
            ;
        }
        auto toc = clock.now();
        std::cout << "rpc perf test: concurrency = " << concurrency
            << "time = " << std::chrono::duration_cast<std::chrono::microseconds>(toc - tic).count() << std::endl;
    }
}