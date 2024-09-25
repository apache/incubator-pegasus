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

#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/core.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/test_util.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/test_macros.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;

class restore_test : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();

        backup_path_ = fmt::format("onebox/block_service/local_service/{}", kClusterName);

        NO_FATALS(write_data(kTestCount));

        std::vector<int32_t> app_ids({table_id_});
        ASSERT_EQ(
            ERR_OK,
            ddl_client_->add_backup_policy("policy_1", "local_service", app_ids, 86400, 6, "24:0"));
    }

    void TearDown() override { ASSERT_EQ(ERR_OK, ddl_client_->drop_app(table_name_, 0)); }

    void restore()
    {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        ASSERT_EQ(ERR_OK,
                  ddl_client_->do_restore("local_service",
                                          kClusterName,
                                          /* policy_name */ "",
                                          first_backup_timestamp_,
                                          table_name_,
                                          table_id_,
                                          kNewTableName,
                                          false,
                                          ""));
        NO_FATALS(wait_table_healthy(kNewTableName));
    }

    void wait_backup_complete()
    {
        ASSERT_IN_TIME(
            [&] {
                first_backup_timestamp_ = get_first_backup_timestamp();
                fmt::print(stdout, "first backup_timestamp = {}", first_backup_timestamp_);

                auto backup_info =
                    fmt::format("{}/{}/backup_info", backup_path_, first_backup_timestamp_);
                ASSERT_TRUE(dsn::utils::filesystem::file_exists(backup_info));
            },
            180);
    }

    int64_t get_first_backup_timestamp()
    {
        std::string pegasus_root_dir = global_env::instance()._pegasus_root;
        CHECK_EQ(0, ::chdir(pegasus_root_dir.c_str()));
        std::string cmd = "cd " + backup_path_ +
                          "; "
                          "ls -c > restore_app_from_backup_test_tmp; "
                          "tail -n 1 restore_app_from_backup_test_tmp; "
                          "rm restore_app_from_backup_test_tmp";
        std::stringstream ss;
        int ret = dsn::utils::pipe_execute(cmd.c_str(), ss);
        std::cout << cmd << " output: " << ss.str() << std::endl;
        CHECK_EQ(ret, 0);
        std::string result = ss.str();
        // should remove \n character
        int32_t index = result.size();
        while (index > 1 && (result[index - 1] < '0' || result[index - 1] > '9')) {
            index -= 1;
        }
        result = result.substr(0, index);
        if (!result.empty()) {
            return boost::lexical_cast<int64_t>(result);
        } else {
            return 0;
        }
    }

public:
    const int kTestCount = 10000;
    const std::string kNewTableName = "backup_test_new";

    int64_t first_backup_timestamp_;
    std::string backup_path_;
};

TEST_F(restore_test, restore)
{
    fmt::print("start testing restore...\n");
    // step1: wait backup complete
    NO_FATALS(wait_backup_complete());
    // step2: test restore
    NO_FATALS(restore());
    // step3: verify_data
    NO_FATALS(verify_data(kNewTableName, kTestCount));
    fmt::print("restore passed...\n");
}
