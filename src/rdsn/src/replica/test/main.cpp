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

#include <cmath>
#include <fstream>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include <dsn/dist/replication/replication_service_app.h>

#include "replication_service_test_app.h"

int gtest_flags = 0;
int gtest_ret = 0;
replication_service_test_app *app;

error_code replication_service_test_app::start(const std::vector<std::string> &args)
{
    app = this;
    gtest_ret = RUN_ALL_TESTS();
    gtest_flags = 1;
    return dsn::ERR_OK;
}

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    dsn::service_app::register_factory<replication_service_test_app>("replica");

    dsn_run_config("config-test.ini", false);
    while (gtest_flags == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(gtest_ret);
#endif
    return gtest_ret;
}
