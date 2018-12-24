#include <libgen.h>

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>

#include "base/pegasus_const.h"
#include "global_env.h"

using namespace ::dsn;
using namespace ::dsn::replication;
using namespace pegasus;

class restore_test : public testing::Test
{
public:
    virtual void SetUp() override
    {
        pegasus_root_dir = global_env::instance()._pegasus_root;
        working_root_dir = global_env::instance()._working_dir;

        chdir(pegasus_root_dir.c_str());
        cluster_name = dsn::utils::filesystem::path_combine(pegasus_root_dir, backup_data_dir);
        system("pwd");
        // modify the config to enable backup, and restart onebox
        system("sed -i \"/^cold_backup_disabled/c cold_backup_disabled = false\" "
               "src/server/config-server.ini");
        std::string cmd = "sed -i \"/^cold_backup_root/c cold_backup_root = " + cluster_name;
        cmd = cmd + std::string("\" src/server/config-server.ini");
        system(cmd.c_str());
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox");
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::vector<dsn::rpc_address> meta_list;
        replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
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
        pg_client = pegasus::pegasus_client_factory::get_client("mycluster", app_name.c_str());
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

    virtual void TearDown() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        system("git checkout -- src/server/config-server.ini");
        system("./run.sh start_onebox -w");
        std::string cmd = "rm -rf " + backup_data_dir;
        system(cmd.c_str());
        chdir(global_env::instance()._working_dir.c_str());
    }

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
        int64_t start = dsn_now_ms();
        for (int i = 1; i <= kv_pair_cnt; i++) {
            std::string index = std::to_string(i);
            std::string h_key = hash_key_prefix + "_" + index;
            std::string s_key = sort_key_prefix + "_" + index;
            std::string value = value_prefix + "_" + index;
            std::string value_new;
            err = pg_client->get(h_key, s_key, value_new);
            if (err != PERR_OK) {
                std::cout << "get <" << h_key << ">, <" << s_key
                          << "> failed， with err = " << pg_client->get_error_string(err)
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
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox");
        std::this_thread::sleep_for(std::chrono::seconds(3));
        time_stamp = get_first_backup_timestamp();
        std::cout << "first backup_timestamp = " << time_stamp << std::endl;
        error_code err = ddl_client->do_restore(backup_provider_name,
                                                cluster_name,
                                                policy_name,
                                                time_stamp,
                                                app_name,
                                                old_app_id,
                                                app_name,
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
            err = ddl_client->list_app(app_name, app_id, partition_cnt, p_confs);
            if (err != ERR_OK) {
                std::cout << "list app failed, app_name = " << app_name
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

            is_backup_complete = find_second_backup_timestamp();
        }
        return is_backup_complete;
    }

    int64_t get_first_backup_timestamp()
    {
        std::string policy_dir = dsn::utils::filesystem::path_combine(cluster_name, policy_name);
        std::string cmd = "cd " + policy_dir + "; "
                                               "ls -c > restore_app_from_backup_test_tmp; "
                                               "tail -n 1 restore_app_from_backup_test_tmp; "
                                               "rm restore_app_from_backup_test_tmp";
        std::stringstream ss;
        assert(dsn::utils::pipe_execute(cmd.c_str(), ss) == 0);
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
        std::string policy_dir = dsn::utils::filesystem::path_combine(cluster_name, policy_name);
        std::vector<std::string> dirs;
        ::dsn::utils::filesystem::get_subdirectories(policy_dir, dirs, false);
        return (dirs.size() >= 2);
    }

public:
    pegasus_client *pg_client;
    std::shared_ptr<replication_ddl_client> ddl_client;
    std::string pegasus_root_dir;
    std::string working_root_dir;

    std::string cluster_name;
    int32_t old_app_id;
    int64_t time_stamp;

    static const std::string policy_name;
    static const std::string backup_provider_name;
    static const int backup_interval_seconds;
    static const int backup_history_count_to_keep;
    static const std::string start_time;
    static const std::string backup_data_dir;

    static const std::string app_name;

    static const std::string hash_key_prefix;
    static const std::string sort_key_prefix;
    static const std::string value_prefix;

    static const int kv_pair_cnt;
    static const int default_partition_cnt;
};

const std::string restore_test::policy_name = "policy_1";
const std::string restore_test::backup_provider_name = "local_service";
// NOTICE: we enqueue a time task to check whether policy should start backup periodically, the
// period is 5min, so the time between two backup is at least 5min, but if we set the
// backup_interval_seconds smaller enough such as smaller than the time of finishing once backup, we
// can start next backup immediately when current backup is finished
const int restore_test::backup_interval_seconds = 1;
const int restore_test::backup_history_count_to_keep = 6;
const std::string restore_test::start_time = "24:0";
const std::string restore_test::backup_data_dir = "backup_data";

const std::string restore_test::app_name = "backup_test";

const std::string restore_test::hash_key_prefix = "hash_key";
const std::string restore_test::sort_key_prefix = "sort_key";
const std::string restore_test::value_prefix = "value";

const int restore_test::kv_pair_cnt = 10000;
const int restore_test::default_partition_cnt = 8;

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
