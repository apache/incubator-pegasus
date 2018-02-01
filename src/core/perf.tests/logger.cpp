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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/service_api_c.h>
#include <dsn/tool/providers.hpc.h>
#include <dsn/utility/singleton_store.h>
#include <dsn/utility/factory_store.h>
#include <dsn/tool_api.h>
#include <dsn/tool-api/logging_provider.h>

#include "service_engine.h"
#include "test_utils.h"
#include "../tools/hpc/hpc_logger.h"
#include "../tools/hpc/hpc_tail_logger.h"
#include "../tools/common/simple_logger.h"

using namespace ::dsn;
const char str[64] = "this is a logging test for log %010d @ thread %010d";

void logv(dsn::logging_provider *logger, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    logger->dsn_logv(__FILENAME__, __FUNCTION__, __LINE__, LOG_LEVEL_DEBUG, str, ap);
    va_end(ap);
}

template <typename TLOGGER>
void logger_test(int thread_count, int record_count)
{
    std::list<std::thread *> threads;
    TLOGGER logger("./");

    uint64_t nts = dsn_now_ns();
    uint64_t nts_start = nts;

    for (int i = 0; i < thread_count; ++i) {
        threads.push_back(new std::thread([&, i] {
            task::set_tls_dsn_context(task::get_current_node2(), nullptr, nullptr);
            for (int j = 0; j < record_count; j++) {
                logv(&logger, str, j, i);
            }

        }));
    }

    for (auto &thr : threads) {
        thr->join();
        delete thr;
    }
    threads.clear();

    nts = dsn_now_ns();

    // one sample log
    size_t size_per_log = strlen("13:11:02.678 (1446037862678885017 1d50) unknown.io-thrd.07504: "
                                 "this is a logging test for log 0000000000 @ thread 000000000") +
                          1;

    std::cout << thread_count << "\t\t\t " << record_count << "\t\t\t "
              << static_cast<double>(thread_count * record_count) * size_per_log / (1024 * 1024) /
                     (nts - nts_start) * 1000000000
              << "MB/s" << std::endl;

    // logger.flush();
}

TEST(core, simple_logger_test)
{
    std::cout << "thread_count\t\t record_count\t\t speed" << std::endl;

    auto threads_count = {1, 2, 5, 10};
    for (int i : threads_count) {
        logger_test<dsn::tools::simple_logger>(i, 100000);
    }
}

TEST(core, hpc_logger_test)
{
    std::cout << "thread_count\t\t record_count\t\t speed" << std::endl;

    auto threads_count = {1, 2, 5, 10};
    for (int i : threads_count)
        logger_test<dsn::tools::hpc_logger>(i, 100000);
}

TEST(core, hpc_tail_logger_test)
{
    std::cout << "thread_count\t\t record_count\t\t speed" << std::endl;

    auto threads_count = {1, 2, 5, 10};
    for (int i : threads_count) {
        logger_test<dsn::tools::hpc_tail_logger>(i, 10000);
    }
}
