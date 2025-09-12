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

#include <gtest/gtest.h>
#include <atomic>

#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "utils/error_code.h"
#include "utils/synchronize.h"

dsn::utils::notify_event g_on_completed;
std::atomic_int g_test_result{0};

class test_client : public dsn::service_app
{
public:
    test_client(const dsn::service_app_info *info) : dsn::service_app(info) {}

    dsn::error_code start(const std::vector<std::string> &args) override
    {
        int argc = args.size();
        char *argv[20];
        for (int i = 0; i < argc; ++i) {
            argv[i] = const_cast<char *>(args[i].c_str());
        }

        std::cout << "start test_client" << std::endl;
        testing::InitGoogleTest(&argc, argv);
        g_test_result = RUN_ALL_TESTS();
        g_on_completed.notify();

        return ::dsn::ERR_OK;
    }

    dsn::error_code stop(bool cleanup = false) override { return dsn::ERR_OK; }

private:
    dsn::utils::notify_event *_on_completed;
    std::atomic_int *_test_result;
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // Register all services.
    dsn::service_app::register_factory<test_client>("test");

    dsn_run_config("config-test.ini", false);

    g_on_completed.wait();

#ifndef ENABLE_GCOV
    dsn_exit(g_test_result);
#endif

    return g_test_result;
}
