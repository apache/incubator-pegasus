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
#include <stdint.h>
#include <deque>
#include <memory>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "meta_admin_types.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/flags.h"

DSN_DECLARE_uint32(ddl_client_max_attempt_count);
DSN_DECLARE_uint32(ddl_client_retry_interval_ms);

namespace dsn {
namespace replication {

TEST(DDLClientTest, RetryMetaRequest)
{
    const auto reserved_ddl_client_max_attempt_count = FLAGS_ddl_client_max_attempt_count;
    FLAGS_ddl_client_max_attempt_count = 3;

    const auto reserved_ddl_client_retry_interval_ms = FLAGS_ddl_client_retry_interval_ms;
    FLAGS_ddl_client_retry_interval_ms = 100;

    // `mock_errors` are the sequence in which each error happens:
    // * first is the error that happens while sending request to meta server, for example
    // ERR_NETWORK_FAILURE or ERR_TIMEOUT, which could be called send error;
    // * then, once request is received successfully by meta server, the error might happen
    // while the request is being processed, which could be called response error.
    //
    // All the errors in `mock_errors` would be traversed in sequence. Once some logic is
    // wrong, CHECK_FALSE(_mock_errors.empty()) in `pop_mock_error()` will fail.
    //
    // The last error in the sequence would always be ERR_UNKNOWN. All of the errors before
    // the last ERR_UNKNOWN should be accepted; the last ERR_UNKNOWN should be the only left
    // error after process.
    //
    // Test cases:
    // - successful for the first attempt
    // - failed to send request to meta server since network cannot be connected
    // - meta server received request successfully, however there are some invalid parameters
    // - initially timeout while sending request to meta server, then busy creating for 2
    // times until success
    // - initially timeout while sending request to meta server, then busy creating for 3
    // times until retry is not allowed
    struct test_case
    {
        std::vector<dsn::error_code> mock_errors;
        uint64_t expected_sleep_ms;
        dsn::error_code final_send_error;
        dsn::error_code final_resp_error;
    } tests[] = {
        {{dsn::ERR_OK, dsn::ERR_OK}, 0, dsn::ERR_OK, dsn::ERR_OK},
        {{dsn::ERR_NETWORK_FAILURE, dsn::ERR_NETWORK_FAILURE, dsn::ERR_NETWORK_FAILURE},
         0,
         dsn::ERR_NETWORK_FAILURE,
         dsn::ERR_UNKNOWN},
        {{dsn::ERR_OK, dsn::ERR_INVALID_PARAMETERS}, 0, dsn::ERR_OK, dsn::ERR_INVALID_PARAMETERS},
        {{dsn::ERR_TIMEOUT,
          dsn::ERR_OK,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_OK,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_OK,
          dsn::ERR_OK},
         FLAGS_ddl_client_retry_interval_ms * 2,
         dsn::ERR_OK,
         dsn::ERR_OK},
        {{dsn::ERR_TIMEOUT,
          dsn::ERR_OK,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_OK,
          dsn::ERR_BUSY_CREATING,
          dsn::ERR_OK,
          dsn::ERR_BUSY_CREATING},
         FLAGS_ddl_client_retry_interval_ms * 2,
         dsn::ERR_OK,
         dsn::ERR_BUSY_CREATING},
    };

    std::vector<rpc_address> meta_list = {{"127.0.0.1", 34601}};
    auto req = std::make_shared<configuration_create_app_request>();
    for (const auto &test : tests) {
        fail::setup();
        fail::cfg("ddl_client_request_meta", "void()");

        // ERR_UNKNOWN should be the only left error after all errors in sequence have been
        // accepted.
        std::vector<dsn::error_code> mock_errors(test.mock_errors);
        mock_errors.push_back(dsn::ERR_UNKNOWN);

        auto ddl_client = std::make_unique<replication_ddl_client>(meta_list);
        ddl_client->set_mock_errors(mock_errors);

        // Once send error is not ERR_OK, response error would not exist; thus ERR_UNKNOWN should
        // be matched.
        configuration_create_app_response resp;
        resp.err = ERR_UNKNOWN;

        auto start_ms = dsn_now_ms();
        auto resp_task = ddl_client->request_meta_and_wait_response(RPC_CM_CREATE_APP, req, resp);
        uint64_t duration_ms = dsn_now_ms() - start_ms;

        // Check if all the errors have been traversed in sequence and accepted except the last
        // ERR_UNKNOWN.
        EXPECT_EQ(std::deque<dsn::error_code>({dsn::ERR_UNKNOWN}), ddl_client->_mock_errors);

        // For busy error it should have slept for enough time.
        EXPECT_LE(test.expected_sleep_ms, duration_ms);

        // Check if final send error is matched.
        EXPECT_EQ(test.final_send_error, resp_task->error());

        // Check if final response error is matched.
        EXPECT_EQ(test.final_resp_error, resp.err);

        fail::teardown();
    }

    FLAGS_ddl_client_retry_interval_ms = reserved_ddl_client_retry_interval_ms;
    FLAGS_ddl_client_max_attempt_count = reserved_ddl_client_max_attempt_count;
}

} // namespace replication
} // namespace dsn
