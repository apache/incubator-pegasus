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

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/replication.codes.h"
#include "meta_admin_types.h"
#include "utils/flags.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"

DSN_DECLARE_uint32(ddl_client_max_attempt_count);
DSN_DECLARE_uint32(ddl_client_retry_interval_ms);

namespace dsn {
namespace replication {

TEST(DDLClientTest, RetryEndMetaRequest)
{
    struct test_case
    {
        std::vector<dsn::error_code> mock_errors;
    } tests[] = {
        {{dsn::ERR_TIMEOUT,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_BUSY_CREATING}},
    };

    auto reserved_ddl_client_max_attempt_count = FLAGS_ddl_client_max_attempt_count;
    FLAGS_ddl_client_max_attempt_count = 3;

    auto reserved_ddl_client_retry_interval_ms = FLAGS_ddl_client_retry_interval_ms;
    FLAGS_ddl_client_retry_interval_ms = 100;

    std::vector<rpc_address> meta_list = {{"127.0.0.1", 34601}};
    auto req = std::make_shared<configuration_create_app_request>();
    for (const auto &test : tests) {
        fail::setup();
        fail::cfg("ddl_client_request_meta", "void()");

        auto ddl_client = std::make_unique<replication_ddl_client>(meta_list);
        ddl_client->set_mock_errors(test.mock_errors);
        auto resp_task = ddl_client->request_meta<configuration_create_app_request>(RPC_TEST, req);
        resp_task->wait();

        EXPECT_TRUE(ddl_client->_mock_errors.empty());

        fail::teardown();
    }

    FLAGS_ddl_client_retry_interval_ms = reserved_ddl_client_retry_interval_ms;
    FLAGS_ddl_client_max_attempt_count = reserved_ddl_client_max_attempt_count;
}

} // namespace replication
} // namespace dsn
