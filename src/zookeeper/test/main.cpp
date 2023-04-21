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
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "utils/error_code.h"

int g_test_count = 0;
int g_test_ret = 0;

extern void lock_test_init();

class test_client : public ::dsn::service_app
{
public:
    test_client(const dsn::service_app_info *info) : ::dsn::service_app(info) {}

    ::dsn::error_code start(const std::vector<std::string> &args)
    {
        int argc = args.size();
        char *argv[20];
        for (int i = 0; i < argc; ++i) {
            argv[i] = (char *)(args[i].c_str());
        }
        testing::InitGoogleTest(&argc, argv);
        g_test_ret = RUN_ALL_TESTS();
        g_test_count = 1;
        return ::dsn::ERR_OK;
    }

    ::dsn::error_code stop(bool cleanup = false) { return ::dsn::ERR_OK; }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register all possible services
    dsn::service_app::register_factory<test_client>("test");
    lock_test_init();

    // specify what services and tools will run in config file, then run
    if (argc < 2)
        dsn_run_config("config-test.ini", false);
    else
        dsn_run_config(argv[1], false);

    while (g_test_count == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(g_test_ret);
#endif
    return g_test_ret;
}
