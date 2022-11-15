/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>
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
#include "replica/replication_service_app.h"
#include "server/compaction_operation.h"
#include "server/pegasus_server_impl.h"

std::atomic_bool gtest_done{false};
std::atomic_int gtest_ret{false};

class gtest_app : public dsn::service_app
{
public:
    explicit gtest_app(const dsn::service_app_info *info) : dsn::service_app(info) {}

    dsn::error_code start(const std::vector<std::string> &args) override
    {
        dsn::service_app::start(args);
        gtest_ret = RUN_ALL_TESTS();
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
    pegasus::server::register_compaction_operations();

    dsn_run_config("config.ini", false);
    while (!gtest_done) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    dsn_exit(gtest_ret);
}
