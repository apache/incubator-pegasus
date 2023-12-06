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
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/test_util.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"

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

        write_data();

        std::vector<int32_t> app_ids({app_id_});
        auto err = ddl_client_->add_backup_policy(policy_name,
                                                  backup_provider_name,
                                                  app_ids,
                                                  backup_interval_seconds,
                                                  backup_history_count_to_keep,
                                                  start_time);
        std::cout << "add backup policy complete with err = " << err.to_string() << std::endl;
        ASSERT_EQ(err, ERR_OK);
    }

    void TearDown() override { ASSERT_EQ(ERR_OK, ddl_client_->drop_app(app_name_, 0)); }

    void write_data()
    {
        std::cout << "start to write " << kv_pair_cnt << " key-value pairs, using set().."
                  << std::endl;
        int64_t start = dsn_now_ms();
        int err = PERR_OK;
        ASSERT_NE(client_, nullptr);
        for (int i = 1; i <= kv_pair_cnt; i++) {
            std::string index = std::to_string(i);
            std::string h_key = hash_key_prefix + "_" + index;
            std::string s_key = sort_key_prefix + "_" + index;
            std::string value = value_prefix + "_" + index;
            err = client_->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }
        int64_t end = dsn_now_ms();
        int64_t ts = (end - start) / 1000;
        std::cout << "write data complete, total time = " << ts << "s" << std::endl;
    }

    void verify_data()
    {
        std::cout << "start to get " << kv_pair_cnt << " key-value pairs, using get()..."
                  << std::endl;
        pegasus_client *new_pg_client = pegasus::pegasus_client_factory::get_client(
            cluster_name_.c_str(), new_app_name.c_str());
        ASSERT_NE(nullptr, new_pg_client);

        int64_t start = dsn_now_ms();
        for (int i = 1; i <= kv_pair_cnt; i++) {
            std::string index = std::to_string(i);
            std::string h_key = hash_key_prefix + "_" + index;
            std::string s_key = sort_key_prefix + "_" + index;
            std::string value = value_prefix + "_" + index;
            std::string value_new;
            ASSERT_EQ(PERR_OK, new_pg_client->get(h_key, s_key, value_new)) << h_key << " : "
                                                                            << s_key;
            ASSERT_EQ(value, value_new);
        }
        int64_t end = dsn_now_ms();
        int64_t ts = (end - start) / 1000;
        std::cout << "verify data complete, total time = " << ts << "s" << std::endl;
    }

    void restore()
    {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        ASSERT_EQ(ERR_OK,
                  ddl_client_->do_restore(backup_provider_name,
                                          cluster_name_,
                                          /*old_policy_name=*/"",
                                          time_stamp,
                                          app_name_,
                                          app_id_,
                                          new_app_name,
                                          false));
        ASSERT_NO_FATAL_FAILURE(wait_app_healthy());
    }

    void wait_app_healthy()
    {
        ASSERT_IN_TIME(
            [&] {
                int32_t app_id = 0;
                int32_t partition_cnt = 0;
                std::vector<partition_configuration> p_confs;
                ASSERT_EQ(ERR_OK,
                          ddl_client_->list_app(new_app_name, app_id, partition_cnt, p_confs));
                for (int i = 0; i < p_confs.size(); i++) {
                    const auto &pc = p_confs[i];
                    ASSERT_FALSE(pc.primary.is_invalid());
                    ASSERT_EQ(1 + pc.secondaries.size(), pc.max_replica_count);
                }
            },
            180);
    }

    bool wait_backup_complete(int64_t seconds)
    {
        // wait backup the first backup complete at most (seconds)second
        int64_t sleep_time = 0;
        bool is_backup_complete = false;
        while (seconds > 0 && !is_backup_complete) {
            sleep_time = 0;
            if (seconds >= 3) {
                sleep_time = 3;
            } else {
                sleep_time = seconds;
            }
            seconds -= sleep_time;
            std::cout << "sleep " << sleep_time << "s to wait backup complete..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));

            time_stamp = get_first_backup_timestamp();
            std::cout << "first backup_timestamp = " << time_stamp << std::endl;

            is_backup_complete = is_app_info_backup_complete();
        }
        return is_backup_complete;
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
            auto res = boost::lexical_cast<int64_t>(result);
            return res;
        } else {
            return 0;
        }
    }

    bool find_second_backup_timestamp()
    {
        std::vector<std::string> dirs;
        ::dsn::utils::filesystem::get_subdirectories(policy_dir, dirs, false);
        return (dirs.size() >= 2);
    }

    bool is_app_info_backup_complete()
    {
        std::string backup_info = backup_dir + "/" + std::to_string(time_stamp) + "/backup_info";
        return dsn::utils::filesystem::file_exists(backup_info);
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
    ASSERT_TRUE(wait_backup_complete(180));
    // step2: test restore
    ASSERT_NO_FATAL_FAILURE(restore());
    // step3: verify_data
    ASSERT_NO_FATAL_FAILURE(verify_data());
    std::cout << "restore passed....." << std::endl;
}
