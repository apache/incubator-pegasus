// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "runtime/test_utils.h"
#include "utils/flags.h"
#include "utils/strings.h"

DSN_DEFINE_string(core, tool, "simulator", "");

int g_test_count = 0;
int g_test_ret = 0;

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register all possible services
    dsn::service_app::register_factory<dsn::test_client>("test");

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
