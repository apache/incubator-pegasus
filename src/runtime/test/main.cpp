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

#include <chrono>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "test_utils.h"
#include "utils/flags.h"
#include "utils/strings.h"

DSN_DEFINE_string(core, tool, "simulator", "");

int g_test_count = 0;
int g_test_ret = 0;

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register all possible services
    dsn::service_app::register_factory<test_client>("test");

    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, false);

    // run in-rDSN tests
    while (g_test_count == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (g_test_ret != 0) {
#ifndef ENABLE_GCOV
        dsn_exit(g_test_ret);
#endif
        return g_test_ret;
    }

    if (!dsn::utils::equals("simulator", FLAGS_tool)) {
        // run out-rDSN tests in other threads
        std::cout << "=========================================================== " << std::endl;
        std::cout << "================== run in non-rDSN threads ================ " << std::endl;
        std::cout << "=========================================================== " << std::endl;
        std::thread t([]() {
            dsn_mimic_app("client", 1);
            exec_tests();
        });
        t.join();
        if (g_test_ret != 0) {
#ifndef ENABLE_GCOV
            dsn_exit(g_test_ret);
#endif
            return g_test_ret;
        }
    }

// exit without any destruction
#ifndef ENABLE_GCOV
    dsn_exit(0);
#endif
    return 0;
}
