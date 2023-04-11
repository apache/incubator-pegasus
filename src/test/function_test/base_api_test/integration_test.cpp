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
#include <unistd.h>
#include <iostream>
#include <string>

#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "test/function_test/utils/test_util.h"
#include "test/function_test/utils/utils.h"

using namespace ::pegasus;

typedef pegasus_client::internal_info internal_info;

class integration_test : public test_util
{
};

TEST_F(integration_test, write_corrupt_db)
{
    // Inject a write error kCorruption to RS-0.
    ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root(
        "curl 'localhost:34801/updateConfig?inject_write_error_for_test=2'"));

    std::string skey = "skey";
    std::string value = "value";
    int ok_count = 0;
    int corruption_count = 0;
    for (int i = 0; i < 1000; i++) {
        std::string hkey = fmt::format("hkey1_{}", i);
        int ret = PERR_OK;
        do {
            ret = client_->set(hkey, skey, value);
            if (ret == PERR_OK) {
                ok_count++;
                break;
            } else if (ret == PERR_CORRUPTION) {
                // Suppose there must some primaries on RS-0.
                corruption_count++;
                break;
            } else if (ret == PERR_TIMEOUT) {
                // If RS-0 crashed before (learn failed when write storage engine but get
                // kCorruption),
                // a new write operation on the primary replica it ever held will cause timeout.
                // Force to fetch the latest route table.
                client_ =
                    pegasus_client_factory::get_client(cluster_name_.c_str(), app_name_.c_str());
                ASSERT_TRUE(client_ != nullptr);
            } else {
                ASSERT_TRUE(false) << ret;
            }
        } while (true);

        // Since only 1 replica server failed, so we can still get correct value from other replica
        // servers.
        std::string got_value;
        ret = client_->get(hkey, skey, got_value);
        do {
            if (ret == PERR_OK) {
                break;
            }
            ASSERT_EQ(PERR_NOT_FOUND, ret);
            client_ = pegasus_client_factory::get_client(cluster_name_.c_str(), app_name_.c_str());
            ASSERT_TRUE(client_ != nullptr);

            ret = client_->get(hkey, skey, got_value);
        } while (true);
        ASSERT_EQ(value, got_value);
    }

    EXPECT_GT(ok_count, 0);
    EXPECT_GT(corruption_count, 0);
    std::cout << "ok_count: " << ok_count << ", corruption_count: " << corruption_count;

    // Now only 2 RS left.
    std::string rs_count;
    ASSERT_NO_FATAL_FAILURE(run_cmd(
        "ps aux | grep 'pegasus_server config.ini -app_list replica' | grep -v grep | wc -l",
        &rs_count));
    ASSERT_EQ("2", rs_count);

    // Replica server 0 is able to start normally.
    // After restart, the 'inject_write_error_for_test' config value will be reset to 0 (i.e. OK).
    ASSERT_NO_FATAL_FAILURE(run_cmd_from_project_root("./run.sh start_onebox_instance -r 1"));
    ASSERT_NO_FATAL_FAILURE(run_cmd(
        "ps aux | grep 'pegasus_server config.ini -app_list replica' | grep -v grep | wc -l",
        &rs_count));
    ASSERT_EQ("3", rs_count);

    // Make best effort to rebalance the cluster,
    ASSERT_NO_FATAL_FAILURE(
        run_cmd_from_project_root("echo 'set_meta_level lively' | ./run.sh shell"));
    usleep(10 * 1000 * 1000);

    for (int i = 0; i < 1000; i++) {
        std::string hkey = fmt::format("hkey2_{}", i);
        int ret = client_->set(hkey, skey, value);
        ASSERT_EQ(PERR_OK, ret) << ret;
        std::string got_value;
        ASSERT_EQ(PERR_OK, client_->get(hkey, skey, got_value));
        ASSERT_EQ(value, got_value);
    }

    ASSERT_NO_FATAL_FAILURE(run_cmd(
        "ps aux | grep 'pegasus_server config.ini -app_list replica' | grep -v grep | wc -l",
        &rs_count));
    ASSERT_EQ("3", rs_count);
}
