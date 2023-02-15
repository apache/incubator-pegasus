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
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>

#include "meta/meta_data.h"
#include "meta_service_test_app.h"

int gtest_flags = 0;
int gtest_ret = 0;

namespace dsn {
namespace replication {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_META_TEST)
DEFINE_TASK_CODE(TASK_META_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_META_TEST)

meta_service_test_app *g_app;

DSN_DEFINE_uint32(tools.simulator, random_seed, 0, "random seed");

// as it is not easy to clean test environment in some cases, we simply run these tests in several
// commands,
// please check the script "run.sh" to modify the GTEST_FILTER
// currently, three filters are used to run these tests:
//   1. a test only run "meta.data_definition", coz it use different config-file
//   2. a test only run "meta.apply_balancer", coz it modify the global state of remote-storage,
//   this conflicts meta.state_sync
//   3. all others tests
//
// If adding a test which doesn't modify the global state, you should simple add your test to the
// case3.
TEST(meta, state_sync) { g_app->state_sync_test(); }

TEST(meta, update_configuration) { g_app->update_configuration_test(); }

TEST(meta, balancer_validator) { g_app->balancer_validator(); }

TEST(meta, apply_balancer) { g_app->apply_balancer_test(); }

TEST(meta, cannot_run_balancer_test) { g_app->cannot_run_balancer_test(); }

TEST(meta, construct_apps_test) { g_app->construct_apps_test(); }

TEST(meta, balance_config_file) { g_app->balance_config_file(); }

TEST(meta, json_compacity) { g_app->json_compacity(); }

TEST(meta, adjust_dropped_size) { g_app->adjust_dropped_size(); }

TEST(meta, app_envs_basic_test) { g_app->app_envs_basic_test(); }

dsn::error_code meta_service_test_app::start(const std::vector<std::string> &args)
{
    if (FLAGS_random_seed == 0) {
        FLAGS_random_seed = static_cast<uint32_t>(time(nullptr));
        LOG_INFO("initial seed: {}", FLAGS_random_seed);
    }
    srand(FLAGS_random_seed);

    int argc = args.size();
    char *argv[20];
    for (int i = 0; i < argc; ++i) {
        argv[i] = (char *)(args[i].c_str());
    }
    testing::InitGoogleTest(&argc, argv);
    g_app = this;
    gtest_ret = RUN_ALL_TESTS();
    gtest_flags = 1;
    return dsn::ERR_OK;
}

} // namespace replication
} // namespace dsn

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    dsn::service_app::register_factory<dsn::replication::meta_service_test_app>("test_meta");
    dsn::service::meta_service_app::register_all();
    dsn_run_config("config-test.ini", false);
    while (gtest_flags == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(gtest_ret);
#endif
    return gtest_ret;
}
