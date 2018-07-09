// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication_service_app.h>
#include "server/pegasus_server_impl.h"

std::atomic_bool gtest_done{false};

class gtest_app : public dsn::service_app
{
public:
    explicit gtest_app(const dsn::service_app_info *info) : dsn::service_app(info) {}

    dsn::error_code start(const std::vector<std::string> &args) override
    {
        dsn::service_app::start(args);
        RUN_ALL_TESTS();
        gtest_done = true;
        return dsn::ERR_OK;
    }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    dsn::service_app::register_factory<gtest_app>("replica");

    dsn::replication::replication_app_base::register_storage_engine(
        "pegasus",
        dsn::replication::replication_app_base::create<pegasus::server::pegasus_server_impl>);

    dsn_run_config("config.ini", false);
    while (!gtest_done) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    dsn_exit(0);
}
