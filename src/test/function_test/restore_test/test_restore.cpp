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

#include <libgen.h>

#include <dsn/utility/filesystem.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>

#include "base/pegasus_const.h"
#include "test/function_test/utils/global_env.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;

// TODO(yingchun): backup & restore festure is on refactoring, we can refactor the related function
// test later.
class restore_test : public testing::Test
{
public:
    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        std::string provider_dir = "block_service/local_service";
        policy_dir = "onebox/" + provider_dir + '/' +
                     dsn::utils::filesystem::path_combine(cluster_name, policy_name);
        backup_dir = "onebox/" + provider_dir + '/' + cluster_name;

        std::vector<dsn::rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), cluster_name.c_str()));
        ASSERT_FALSE(meta_list.empty());
        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(ddl_client != nullptr);
        error_code err =
            ddl_client->create_app(app_name, "pegasus", default_partition_cnt, 3, {}, false);
        ASSERT_EQ(err, ERR_OK);
        int32_t app_id = 0;
        {
            int32_t partition_id;
            std::vector<partition_configuration> partitions;
            err = ddl_client->list_app(app_name, app_id, partition_id, partitions);
            ASSERT_EQ(err, ERR_OK);
            old_app_id = app_id;
        }
        ASSERT_GE(app_id, 0);
        pg_client =
            pegasus::pegasus_client_factory::get_client(cluster_name.c_str(), app_name.c_str());
        ASSERT_NE(pg_client, nullptr);

        write_data();

        std::vector<int32_t> app_ids;
        app_ids.emplace_back(app_id);
        err = ddl_client->add_backup_policy(policy_name,
                                            backup_provider_name,
                                            app_ids,
                                            backup_interval_seconds,
                                            backup_history_count_to_keep,
                                            start_time);
        std::cout << "add backup policy complete with err = " << err.to_string() << std::endl;
        ASSERT_EQ(err, ERR_OK);
    }

    void TearDown() override { ASSERT_EQ(ERR_OK, ddl_client->drop_app(app_name, 0)); }

    void write_data()
    {
        std::cout << "start to write " << kv_pair_cnt << " key-value pairs, using set().."
                  << std::endl;
        int64_t start = dsn_now_ms();
        int err = PERR_OK;
        ASSERT_NE(pg_client, nullptr);
        for (int i = 1; i <= kv_pair_cnt; i++) {
            std::string index = std::to_string(i);
            std::string h_key = hash_key_prefix + "_" + index;
            std::string s_key = sort_key_prefix + "_" + index;
            std::string value = value_prefix + "_" + index;
            err = pg_client->set(h_key, s_key, value);
            ASSERT_EQ(err, PERR_OK);
        }
        int64_t end = dsn_now_ms();
        int64_t ts = (end - start) / 1000;
        std::cout << "write data complete, total time = " << ts << "s" << std::endl;
    }

    bool verify_data()
    {
        int err = PERR_OK;
        std::cout << "start to get " << kv_pair_cnt << " key-value pairs, using get()..."
                  << std::endl;
        new_pg_client =
            pegasus::pegasus_client_factory::get_client(cluster_name.c_str(), new_app_name.c_str());
        if (nullptr == new_pg_client) {
            std::cout << "error to create client for " << new_app_name << std::endl;
            return false;
        }

        int64_t start = dsn_now_ms();
        for (int i = 1; i <= kv_pair_cnt; i++) {
            std::string index = std::to_string(i);
            std::string h_key = hash_key_prefix + "_" + index;
            std::string s_key = sort_key_prefix + "_" + index;
            std::string value = value_prefix + "_" + index;
            std::string value_new;
            err = new_pg_client->get(h_key, s_key, value_new);
            if (err != PERR_OK) {
                std::cout << "get <" << h_key << ">, <" << s_key
                          << "> failed， with err = " << new_pg_client->get_error_string(err)
                          << std::endl;
                return false;
            }
            if (value != value_new) {
                std::cout << "old value = " << value << ", but new value = " << value_new
                          << std::endl;
                return false;
            }
        }
        int64_t end = dsn_now_ms();
        int64_t ts = (end - start) / 1000;
        std::cout << "verify data complete, total time = " << ts << "s" << std::endl;
        return true;
    }

    bool restore()
    {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        error_code err = ddl_client->do_restore(backup_provider_name,
                                                cluster_name,
                                                /*old_policy_name=*/"",
                                                time_stamp,
                                                app_name,
                                                old_app_id,
                                                new_app_name,
                                                false);
        if (err != ERR_OK) {
            std::cout << "restore failed, err = " << err.to_string() << std::endl;
            return false;
        } else {
            // sleep for at most 3 min to wait app is fully healthy
            bool ret = wait_app_healthy(180);
            return ret;
        }
    }

    bool wait_app_healthy(int64_t seconds)
    {
        std::vector<partition_configuration> p_confs;
        int32_t app_id = 0, partition_cnt = 0;
        bool is_app_full_healthy = false;
        error_code err = ERR_OK;
        while (seconds > 0 && !is_app_full_healthy) {
            int64_t sleep_time = 0;
            if (seconds >= 3) {
                sleep_time = 3;
            } else {
                sleep_time = seconds;
            }
            seconds -= sleep_time;
            std::cout << "sleep " << sleep_time << "s to wait app become healthy..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));

            p_confs.clear();
            app_id = 0, partition_cnt = 0;
            err = ddl_client->list_app(new_app_name, app_id, partition_cnt, p_confs);
            if (err != ERR_OK) {
                std::cout << "list app failed, app_name = " << new_app_name
                          << ", with err = " << err.to_string() << std::endl;
                continue;
            }
            int index = 0;
            for (index = 0; index < p_confs.size(); index++) {
                const auto &pc = p_confs[index];
                int replica_count = 0;
                if (pc.primary.is_invalid()) {
                    std::cout << "partition[" << index
                              << "] is unhealthy, coz primary is invalid..." << std::endl;
                    break;
                }
                replica_count += 1;
                replica_count += pc.secondaries.size();
                if (replica_count != pc.max_replica_count) {
                    std::cout << "partition[" << index
                              << "] is unhealthy, coz replica_cont = " << replica_count
                              << ", but max_replica_count = " << pc.max_replica_count << std::endl;
                    break;
                }
            }
            if (index >= p_confs.size()) {
                is_app_full_healthy = true;
            }
        }
        return is_app_full_healthy;
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
        chdir(pegasus_root_dir.c_str());
        std::string cmd = "cd " + backup_dir + "; "
                                               "ls -c > restore_app_from_backup_test_tmp; "
                                               "tail -n 1 restore_app_from_backup_test_tmp; "
                                               "rm restore_app_from_backup_test_tmp";
        std::stringstream ss;
        int ret = dsn::utils::pipe_execute(cmd.c_str(), ss);
        std::cout << cmd << " output: " << ss.str() << std::endl;
        dcheck_eq(ret, 0);
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
    pegasus_client *pg_client;
    pegasus_client *new_pg_client;
    std::shared_ptr<replication_ddl_client> ddl_client;
    std::string policy_dir;
    std::string backup_dir;

    const std::string cluster_name = "mycluster";
    const std::string new_app_name = "backup_test_new";
    int32_t old_app_id;
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

    const std::string app_name = "backup_test";

    const std::string hash_key_prefix = "hash_key";
    const std::string sort_key_prefix = "sort_key";
    const std::string value_prefix = "value";

    const int kv_pair_cnt = 10000;
    const int default_partition_cnt = 8;
};

TEST_F(restore_test, restore)
{
    std::cout << "start testing restore..." << std::endl;
    // step1: wait backup complete
    ASSERT_TRUE(wait_backup_complete(180));
    // step2: test restore
    ASSERT_TRUE(restore());
    // step3: verify_data
    ASSERT_TRUE(verify_data());
    std::cout << "restore passed....." << std::endl;
}
