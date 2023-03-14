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
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "utils/error_code.h"

int g_test_count = 0;
int g_test_ret = 0;

class gtest_app : public dsn::service_app
{
public:
    gtest_app(const dsn::service_app_info *info) : ::dsn::service_app(info) {}

    dsn::error_code start(const std::vector<std::string> &args) override
    {
        g_test_ret = RUN_ALL_TESTS();
        g_test_count = 1;
        return dsn::ERR_OK;
    }

    dsn::error_code stop(bool) override { return dsn::ERR_OK; }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    dsn::service_app::register_factory<gtest_app>("replica");

    dsn_run_config("config-test.ini", false);
    while (g_test_count == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    dsn_exit(g_test_ret);
}
