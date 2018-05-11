// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "integration_test_base.h"
#include <gtest/gtest.h>

using namespace dsn::replication;

class integration_test_compact : public integration_test_base {
public:
    class compact_policy_test : public compact_policy {
    public:
        compact_policy_test() {
            init();
        }

        void init() {
            policy_name = "p1";
            interval_seconds = 3600;
            app_ids = {test_app_id};
            start_time = ::dsn::utils::hm_of_day_to_sec("5:00");
            enable = true;
            opts = {{"target_level",                "2"},
                    {"bottommost_level_compaction", "skip"}};
        }
    };

    friend compact_policy_test;

public:
    void add_compact_policy(const compact_policy_test &policy,
                            bool ok) {
        std::stringstream cmd;
        cmd << "echo \"add_compact_policy"
            << " -p " << policy.policy_name
            << " -a " << ::dsn::utils::sequence_container_to_string(policy.app_ids, ",")
            << " -s " << ::dsn::utils::sec_of_day_to_hm(policy.start_time)
            << " -i " << policy.interval_seconds;
        if (!policy.opts.empty()) {
            cmd << " -o " << ::dsn::utils::kv_map_to_string(policy.opts, ',', '=');
        }
        cmd << "\" | ./run.sh shell";

        std::stringstream ret;
        ASSERT_EQ(dsn::utils::pipe_execute(cmd.str().c_str(), ret), 0);
        if (ok) {
            ASSERT_TRUE(ret.str().find("Add policy result: ERR_OK") != std::string::npos)
                                        << ret.str();
        } else {
            ASSERT_FALSE(ret.str().find("Add policy result: ERR_OK") != std::string::npos)
                                        << ret.str();
        }
    }

    void switch_compact_policy(const std::string &policy_name,
                               bool enable) {
        std::stringstream cmd;
        if (enable) {
            cmd << "echo \"enable_compact_policy ";
        } else {
            cmd << "echo \"disable_compact_policy ";
        }
        cmd << " -p " << policy_name
            << "\" | ./run.sh shell";

        std::stringstream ret;
        ASSERT_EQ(dsn::utils::pipe_execute(cmd.str().c_str(), ret), 0);
        ASSERT_TRUE(ret.str().find("Modify policy result: ERR_OK") != std::string::npos)
                                    << ret.str();
    }

    void check_compact_policy(const std::map<std::string, compact_policy_test> &name_policys) {
        std::set<std::string> policy_names;
        for (const auto &name_policy : name_policys) {
            policy_names.insert(name_policy.first);
        }
        std::vector<compact_policy_records> policy_records;
        ::dsn::error_code ret = ddl_client->query_compact_policy(policy_names, &policy_records);
        ASSERT_EQ(dsn::ERR_OK, ret) << ret.to_string();
        ASSERT_EQ(name_policys.size(), policy_records.size());

        for (const auto &policy_record : policy_records) {
            ASSERT_EQ(name_policys.count(policy_record.policy.policy_name), 1);
            const compact_policy_test &req = name_policys.at(policy_record.policy.policy_name);
            ASSERT_EQ(policy_record.policy.policy_name, req.policy_name);
            ASSERT_EQ(policy_record.policy.interval_seconds, req.interval_seconds);
            ASSERT_EQ(policy_record.policy.app_ids, req.app_ids);
            ASSERT_EQ(policy_record.policy.start_time, req.start_time);
            ASSERT_EQ(policy_record.policy.enable, req.enable);
            ASSERT_EQ(policy_record.policy.opts, req.opts);
        }
    }

    void check_compact_policy_not_exist(const std::string &policy_name) {
        std::set<std::string> policy_names({policy_name});
        std::vector<compact_policy_records> policy_records;
        ::dsn::error_code ret = ddl_client->query_compact_policy(policy_names, &policy_records);
        ASSERT_EQ(dsn::ERR_OK, ret);
        ASSERT_EQ(0, policy_records.size());
    }

    void check_compact_records(const std::string &policy_name, int retry_times = 0) {
        std::set<std::string> policy_names({policy_name});
        std::vector<compact_policy_records> policy_records;
        int times = 0;
        while (true) {
            ::dsn::error_code ret = ddl_client->query_compact_policy(policy_names, &policy_records);
            ASSERT_EQ(dsn::ERR_OK, ret);
            ASSERT_EQ(1, policy_records.size());
            if (retry_times == 0) {
                return;
            }

            if (!policy_records[0].records.empty() &&
                policy_records[0].records[0].end_time != 0) {
                return;
            }

            ++times;
            ASSERT_LE(times, retry_times);
            sleep(5);
        }
    }

    void get_last_compact_finish_time(int app_id, std::list<std::string> &finish_time) {
        std::stringstream cmd;
        cmd << "echo \"remote_command -t replica-server replica.query-compact " << app_id
            << "\" | ./run.sh shell 2>&1";

        std::stringstream ret;
        ASSERT_EQ(dsn::utils::pipe_execute(cmd.str().c_str(), ret), 0);
        std::cout << ret.str() << std::endl;
        std::string line;
        while (getline(ret, line, '\n')) {
            unsigned long found = line.find("last finish at [");
            if (found == std::string::npos) {
                continue;
            }

            unsigned long beg = line.find("[");
            unsigned long end = line.find("]", beg);
            ASSERT_TRUE(end != std::string::npos);

            std::string t = line.substr(beg + 1, end - beg);
            finish_time.push_back(t);
        }
    }
};

TEST_F(integration_test_compact, add_compact_policy) {
    compact_policy_test policy;
    add_compact_policy(policy, true);
}

TEST_F(integration_test_compact, check_compact_policy) {
    compact_policy_test policy;
    std::set<std::string> policy_names({policy.policy_name});

    add_compact_policy(policy, true);
    check_compact_policy({{policy.policy_name, policy}});
}

TEST_F(integration_test_compact, check_compact_policy_after_restart_meta) {
    compact_policy_test policy;
    std::set<std::string> policy_names({policy.policy_name});

    add_compact_policy(policy, true);

    restart_all_meta();
    sleep(5);
    init_ddl_client();
    check_compact_policy({{policy.policy_name, policy}});
}

TEST_F(integration_test_compact, add_compact_policy_bad_params) {
    compact_policy_test policy;

    // interval_seconds
    policy.init();
    policy.interval_seconds = -1;
    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);

    // policy_name
    policy.init();
    policy.policy_name.clear();
    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);

    // app_ids
    policy.init();
    policy.app_ids.clear();
    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);

    policy.init();
    policy.app_ids = {-1};
    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);

    policy.init();
    policy.app_ids = {0};
    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);
}

TEST_F(integration_test_compact, add_compact_policy_empty_opts_ok) {
    compact_policy_test policy;

    policy.opts.clear();
    add_compact_policy(policy, true);

    check_compact_policy({{policy.policy_name, policy}});
}

TEST_F(integration_test_compact, add_compact_policy_dup_policy_name) {
    compact_policy_test policy;

    add_compact_policy(policy, true);
    add_compact_policy(policy, false);

    check_compact_policy({{policy.policy_name, policy}});
}

TEST_F(integration_test_compact, add_compact_policy_no_exist_app) {
    compact_policy_test policy;
    policy.app_ids = {999};

    add_compact_policy(policy, false);
    check_compact_policy_not_exist(policy.policy_name);
}

TEST_F(integration_test_compact, switch_compact_policy_ok) {
    compact_policy_test policy;

    add_compact_policy(policy, true);

    switch_compact_policy(policy.policy_name, false);
    policy.enable = false;
    check_compact_policy({{policy.policy_name, policy}});

    switch_compact_policy(policy.policy_name, true);
    policy.enable = true;
    check_compact_policy({{policy.policy_name, policy}});
}

TEST_F(integration_test_compact, multi_tables_and_policies) {
    compact_policy_test policy;
    policy.policy_name = "p1";
    int32_t appid1 = create_table("test1");
    int32_t appid2 = create_table("test2");
    int32_t appid3 = create_table("test3");
    int32_t appid4 = create_table("test4");
    int32_t appid5 = create_table("test5");
}

TEST_F(integration_test_compact, verify_data_after_compact) {
    int data_count = 10000;
    write_data(data_count);

    compact_policy_test policy;
    policy.start_time = ::dsn::utils::sec_of_day();
    policy.start_time -= (policy.start_time % 60);
    uint64_t start_time = dsn_now_ms();
    add_compact_policy(policy, true);

    check_compact_policy({{policy.policy_name, policy}});

    // test records on meta
    check_compact_records(policy.policy_name, 60);

    // test records on replica
    std::list<std::string> finish_time_1;
    {
        get_last_compact_finish_time(*policy.app_ids.begin(), finish_time_1);
        char begin_time[30] = {0};
        ::dsn::utils::time_ms_to_string(start_time, begin_time);
        char last_time[30] = {0};
        ::dsn::utils::time_ms_to_string(dsn_now_ms(), last_time);
        ASSERT_EQ(finish_time_1.size(), replica_count * partition_count);
        for (const auto &time : finish_time_1) {
            ASSERT_GE(time, std::string(begin_time));
            ASSERT_LE(time, std::string(last_time));
        }
        verify_data(data_count);
    }

    // test records on replica after restart
    restart_all_replica();
    sleep(5);
    wait_util_app_full_health(test_app_name, 30);
    {
        std::list<std::string> finish_time_2;
        get_last_compact_finish_time(*policy.app_ids.begin(), finish_time_2);
        ASSERT_EQ(finish_time_1, finish_time_2);
        verify_data(data_count);
    }
}
