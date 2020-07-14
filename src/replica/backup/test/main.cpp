// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/service_api_cpp.h>

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
