// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/pegasus_perf_counter.h"
#include "server/pegasus_service_app.h"
#include "test/utils/client_init.h"

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication_service_app.h>

std::atomic_bool gtest_done{false};

class gtest_app : public ::dsn::replication::replication_service_app
{
public:
    gtest_app(const dsn::service_app_info *info)
        : dsn::replication::replication_service_app::replication_service_app(info)
    {
    }

    virtual dsn::error_code start(const std::vector<std::string> &args) override
    {
        dsn::replication::replication_service_app::start(args);
        RUN_ALL_TESTS();
        gtest_done = true;
        return dsn::ERR_OK;
    }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register perf counter.
    ::dsn::tools::internal_use_only::register_component_provider(
        "pegasus::server::pegasus_perf_counter",
        pegasus::server::pegasus_perf_counter_factory,
        ::dsn::PROVIDER_TYPE_MAIN);

    dsn::service_app::register_factory<gtest_app>("replica");

    dsn_run_config("config.ini", false);
    while (!gtest_done) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    dsn_exit(0);
}
