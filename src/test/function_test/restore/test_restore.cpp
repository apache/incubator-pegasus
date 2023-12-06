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
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/pegasus_const.h"
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

        std::string provider_dir = "block_service/local_service";
        policy_dir = "onebox/" + provider_dir + '/' +
                     dsn::utils::filesystem::path_combine(cluster_name_, policy_name);
        backup_dir = "onebox/" + provider_dir + '/' + cluster_name_;

        NO_FATALS(write_data(kv_pair_cnt));

        std::vector<int32_t> app_ids({table_id_});
        auto err = ddl_client_->add_backup_policy(policy_name,
                                                  backup_provider_name,
                                                  app_ids,
                                                  backup_interval_seconds,
                                                  backup_history_count_to_keep,
                                                  start_time);
        std::cout << "add backup policy complete with err = " << err.to_string() << std::endl;
        ASSERT_EQ(err, ERR_OK);
    }

    void TearDown() override { ASSERT_EQ(ERR_OK, ddl_client_->drop_app(table_name_, 0)); }

    void restore()
    {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        ASSERT_EQ(ERR_OK,
                  ddl_client_->do_restore(backup_provider_name,
                                          cluster_name_,
                                          /* policy_name */ "",
                                          time_stamp,
                                          table_name_,
                                          table_id_,
                                          new_app_name,
                                          false));
        NO_FATALS(wait_table_healthy(new_app_name));
    }

    void wait_backup_complete()
    {
        ASSERT_IN_TIME(
            [&] {
                time_stamp = get_first_backup_timestamp();
                fmt::print(stdout, "first backup_timestamp = {}", time_stamp);

                std::string backup_info =
                    backup_dir + "/" + std::to_string(time_stamp) + "/backup_info";
                ASSERT_TRUE(dsn::utils::filesystem::file_exists(backup_info));
            },
            180);
    }

    int64_t get_first_backup_timestamp()
    {
        std::string pegasus_root_dir = global_env::instance()._pegasus_root;
        CHECK_EQ(0, ::chdir(pegasus_root_dir.c_str()));
        std::string cmd = "cd " + backup_dir + "; "
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
    std::string policy_dir;
    std::string backup_dir;

    const std::string new_app_name = "backup_test_new";
    int64_t time_stamp;

    const std::string policy_name = "policy_1";
    const std::string backup_provider_name = "local_service";
    // NOTICE: we enqueue a time task to check whether policy should start backup periodically, the
    // period is 5min, so the time between two backup is at least 5min, but if we set the
    // backup_interval_seconds smaller enough such as smaller than the time of finishing once
    // backup, we
    // can start next backup immediately when current backup is finished
    // The backup interval must be greater than checkpoint reserve time, see
    // backup_service::add_backup_policy() for details.
    const int backup_interval_seconds = 700;
    const int backup_history_count_to_keep = 6;
    const std::string start_time = "24:0";

    const std::string hash_key_prefix = "hash_key";
    const std::string sort_key_prefix = "sort_key";
    const std::string value_prefix = "value";

    const int kv_pair_cnt = 10000;
};

TEST_F(restore_test, restore)
{
    std::cout << "start testing restore..." << std::endl;
    // step1: wait backup complete
    NO_FATALS(wait_backup_complete());
    // step2: test restore
    NO_FATALS(restore());
    // step3: verify_data
    NO_FATALS(verify_data(new_app_name, kv_pair_cnt));
    std::cout << "restore passed....." << std::endl;
}
