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

#include <fmt/core.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;

typedef pegasus_client::internal_info internal_info;

class integration_test : public test_util
{
};

TEST_F(integration_test, write_corrupt_db)
{
    // Inject a write error to a replica server.
    ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
        "curl 'localhost:34801/updateConfig?inject_write_error_for_test=true'"));

    std::string skey = "skey";
    std::string value = "value";
    int ok_count = 0;
    int io_error_count = 0;
    for (int i = 0; i < 100; i++) {
        std::string hkey = fmt::format("hkey1_{}", i);
        int ret = client_->set(hkey, skey, value);
        if (ret == PERR_OK) {
            ok_count++;
        } else if (ret == PERR_IO_ERROR) {
            io_error_count++;
        } else {
            ASSERT_TRUE(false) << ret;
        }
        std::string got_value;
        client_->get(hkey, skey, got_value);
        ASSERT_EQ(value, got_value);
    }

    ASSERT_GT(ok_count, 0);
    ASSERT_GT(io_error_count, 0);
    std::cout << "ok_count: " << ok_count << ", io_error_count: " << io_error_count;

    // Inject a write error to a replica server.
    ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
        "curl 'localhost:34801/updateConfig?inject_write_error_for_test=false'"));

    for (int i = 0; i < 100; i++) {
        std::string hkey = fmt::format("hkey2_{}", i);
        int ret = client_->set(hkey, skey, value);
        ASSERT_EQ(PERR_OK, ret) << ret;
        std::string got_value;
        client_->get(hkey, skey, got_value);
        ASSERT_EQ(value, got_value);
    }
}
