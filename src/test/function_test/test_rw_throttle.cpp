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

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include <dsn/utility/TokenBucket.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>
#include <fstream>

#include "base/pegasus_const.h"
#include "global_env.h"
#include "utils.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

enum class throttle_type
{
    read_by_qps,
    read_by_size,
    write_by_qps,
    write_by_size
};

ENUM_BEGIN2(throttle_type, throttle_type, throttle_type::write_by_size)
ENUM_REG(throttle_type::read_by_qps)
ENUM_REG(throttle_type::read_by_size)
ENUM_REG(throttle_type::write_by_qps)
ENUM_REG(throttle_type::write_by_size)
ENUM_END2(throttle_type, throttle_type)

enum class operation_type
{
    get,
    multi_get,
    set,
    multi_set
};

ENUM_BEGIN2(operation_type, operation_type, operation_type::multi_set)
ENUM_REG(operation_type::get)
ENUM_REG(operation_type::multi_get)
ENUM_REG(operation_type::set)
ENUM_REG(operation_type::multi_set)
ENUM_END2(operation_type, operation_type)

struct throttle_test_plan
{
    std::string test_plan_case;
    operation_type ot;
    int single_value_sz;
    int multi_count;
    int limit_qps;
};

#define ToString(x) #x

#define TIMELY_RECORD(time_interval, is_reject, size)                                              \
    do {                                                                                           \
        records[ToString(time_interval##_query_times)]++;                                          \
        records[ToString(time_interval##_query_size)] += size;                                     \
        if (is_reject) {                                                                           \
            records[ToString(time_interval##_reject_times)]++;                                     \
            records[ToString(time_interval##_reject_size)] += size;                                \
        } else {                                                                                   \
            records[ToString(time_interval##_successful_times)]++;                                 \
            records[ToString(time_interval##_successful_size)] += size;                            \
        }                                                                                          \
    } while (0)

struct throttle_test_recorder
{
    uint64_t start_time_ms;
    uint64_t duration_ms;
    std::map<std::string, uint64_t> records;
    std::string test_name;
    std::vector<std::string> parameter_seq = {"total_qps",
                                              "total_size_per_sec",
                                              "first_10_ms_successful_times",
                                              "first_100_ms_successful_times",
                                              "first_1000_ms_successful_times",
                                              "first_5000_ms_successful_times",
                                              "first_10_ms_successful_size",
                                              "first_100_ms_successful_size",
                                              "first_1000_ms_successful_size",
                                              "first_5000_ms_successful_size"};

    void start_test(const std::string &test_case, uint64_t time_duration_s)
    {
        test_name = test_case;
        start_time_ms = dsn_now_ms();
        duration_ms = time_duration_s * 1000;
        records.emplace(std::make_pair("duration_ms", duration_ms));
    }

    bool is_time_up()
    {
        if (dsn_now_ms() - start_time_ms > duration_ms) {
            return true;
        }
        return false;
    }

    void record(uint64_t size, bool is_reject)
    {
        if (dsn_now_ms() - start_time_ms <= 10) {
            TIMELY_RECORD(first_10_ms, is_reject, size);
        }
        if (dsn_now_ms() - start_time_ms <= 100) {
            TIMELY_RECORD(first_100_ms, is_reject, size);
        }
        if (dsn_now_ms() - start_time_ms <= 1000) {
            TIMELY_RECORD(first_1000_ms, is_reject, size);
        }
        if (dsn_now_ms() - start_time_ms <= 5000) {
            TIMELY_RECORD(first_5000_ms, is_reject, size);
        }
        TIMELY_RECORD(total, is_reject, size);

        records["total_qps"] = records["total_successful_times"] / (double)(duration_ms / 1000);
        records["total_size_per_sec"] =
            records["total_successful_size"] / (double)(duration_ms / 1000);
    }

    void print_results(const std::string &dir)
    {
        std::streambuf *psbuf, *backup;
        std::ofstream file;
        file.open(dir);
        backup = std::cout.rdbuf();
        psbuf = file.rdbuf();
        std::cout.rdbuf(psbuf);

        std::cout << "test case: " << test_name << std::endl;
        for (const auto &iter : parameter_seq) {
            std::cout << iter << ": " << records[iter] << std::endl;
        }
        std::cout << std::endl;

        std::cout.rdbuf(backup);
        file.close();

        return;
    }
};

const int test_hashkey_len = 50;
const int test_sortkey_len = 50;

class test_rw_throttle : public testing::Test
{
public:
    virtual void SetUp() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("pwd");
        system("./run.sh clear_onebox");
        system("cp src/server/config.min.ini config-server-test-hotspot.ini");
        system("sed -i \"/^\\s*enable_detect_hotkey/c enable_detect_hotkey = "
               "true\" config-server-test-hotspot.ini");
        system("./run.sh start_onebox -c -w --config_path config-server-test-hotspot.ini");
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::vector<dsn::rpc_address> meta_list;
        replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster");

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        pg_client =
            pegasus::pegasus_client_factory::get_client("single_master_cluster", app_name.c_str());

        auto err = ddl_client->create_app(app_name.c_str(), "pegasus", 4, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);
    }

    virtual void TearDown() override
    {
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox -w");
        chdir(global_env::instance()._working_dir.c_str());
    }

    void set_throttle(throttle_type type, uint64_t value)
    {
        std::vector<std::string> keys, values;
        if (type == throttle_type::read_by_qps) {
            keys.emplace_back("replica.read_throttling");
            values.emplace_back(fmt::format("{}*reject*200", value));
        } else if (type == throttle_type::read_by_size) {
            keys.emplace_back("replica.read_throttling_by_size");
            values.emplace_back(fmt::format("{}", value));
        } else if (type == throttle_type::write_by_qps) {
            keys.emplace_back("replica.write_throttling");
            values.emplace_back(fmt::format("{}*reject*10", value));
        } else if (type == throttle_type::write_by_size) {
            keys.emplace_back("replica.write_throttling_by_size");
            values.emplace_back(fmt::format("{}*reject*200", value));
        }
        auto resp = ddl_client->set_app_envs(app_name, keys, values);
        dassert_f(
            resp.get_error().code() == ERR_OK, "Set env failed: {}", resp.get_value().hint_message);
    }

    void restore_throttle(throttle_type type)
    {
        std::vector<std::string> keys;
        if (type == throttle_type::read_by_qps) {
            keys.emplace_back("replica.read_throttling");
        } else if (type == throttle_type::read_by_size) {
            keys.emplace_back("replica.read_throttling_by_size");
        } else if (type == throttle_type::write_by_qps) {
            keys.emplace_back("replica.write_throttling");
        } else if (type == throttle_type::write_by_size) {
            keys.emplace_back("replica.write_throttling_by_size");
        }
        auto resp = ddl_client->del_app_envs(app_name, keys);
        dassert_f(resp == ERR_OK, "Del env failed");
    }

    throttle_test_recorder start_test(throttle_test_plan test_plan, uint64_t time_duration_s = 10)
    {
        std::cout << fmt::format("start test, on {}", test_plan.test_plan_case) << std::endl;

        dassert(pg_client, "pg_client is nullptr");

        throttle_test_recorder r;
        r.start_test(test_plan.test_plan_case, time_duration_s);

        bool is_running = true;
        while (!r.is_time_up()) {
            auto h_key = generate_hash_key(test_hashkey_len);
            auto s_key = generate_hash_key(test_sortkey_len);
            auto value = generate_hash_key(test_plan.single_value_sz);
            auto sortkey_value_pairs = generate_sortkey_value_map(
                generate_str_vector_by_random(test_sortkey_len, test_plan.multi_count),
                generate_str_vector_by_random(test_plan.single_value_sz, test_plan.multi_count));

            if (test_plan.ot == operation_type::set) {
                pg_client->async_set(
                    h_key, s_key, value, [&](int ec, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            return;
                        }
                        dassert_f(ec == PERR_OK || ec == PERR_APP_BUSY,
                                  "get/set data failed, error code:{}",
                                  ec);
                        r.record(value.size(), ec == PERR_APP_BUSY);
                    });
            }
            //            else if (test_plan.ot == operation_type::multi_set) {
            //                err = pg_client->multi_set(h_key, sortkey_value_pairs);
            //            }
            else if (test_plan.ot == operation_type::get) {
                pg_client->async_set(
                    h_key,
                    s_key,
                    value,
                    [&, h_key, s_key, value](int ec_write, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            return;
                        }
                        dassert_f(ec_write == PERR_OK, "set data failed, error code:{}", ec_write);
                        dassert(pg_client, "client is nullptr");
                        pg_client->async_get(
                            h_key,
                            s_key,
                            [&, h_key, s_key, value](int ec_read,
                                                     std::string &&val,
                                                     pegasus_client::internal_info &&info) {
                                if (!is_running) {
                                    return;
                                }
                                dassert_f(ec_read == PERR_OK || ec_read == PERR_APP_BUSY,
                                          "get data failed, error code:{}",
                                          ec_read);
                                r.record(value.size(), ec_read == PERR_APP_BUSY);
                            });
                    });
            }
            //              else if (test_plan.ot == operation_type::multi_get) {
            //                err = pg_client->multi_set(h_key, sortkey_value_pairs);
            //                std::set<std::string> sortkeys;
            //                err = pg_client->multi_get(h_key, sortkeys, sortkey_value_pairs);
            //            }
        }
        is_running = false;

        r.print_results(fmt::format("./src/builder/test/function_test/throttle_test_{}_result.txt",
                                    r.test_name));
        return r;
    }

    const std::string app_name = "throttle_test";
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
};

TEST_F(test_rw_throttle, test_rw_throttle)
{
    throttle_test_plan plan = {"set test / throttle by qps ", operation_type::set, 1024, 1, 50};
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    start_test(plan);
    restore_throttle(throttle_type::write_by_qps);

    plan = {"set test / throttle by size", operation_type::set, 102400, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    start_test(plan);
    restore_throttle(throttle_type::write_by_size);

    //    plan = {"set test / throttle by qps&size", operation_type::set, 102400, 1, 50};
    //    set_throttle(throttle_type::write_by_size,
    //                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    //    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    //    std::cout << "wait 30s for setting env" << std::endl;
    //    sleep(30);
    //    start_test(plan);
    //    restore_throttle(throttle_type::write_by_size);
    //    restore_throttle(throttle_type::write_by_qps);
    //
    //    plan = {"get qps test", operation_type::get, 1024, 1, 50};
    //    set_throttle(throttle_type::read_by_size, plan.limit_qps * plan.single_value_sz);
    //    std::cout << "wait 30s for setting env" << std::endl;
    //    sleep(30);
    //    r = start_test(plan);
    //    print_results(r, plan);
    //    restore_throttle(throttle_type::read_by_size);
}
