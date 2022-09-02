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
#include "include/pegasus/client.h"
#include <gtest/gtest.h>
#include <dsn/utility/TokenBucket.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/fmt_logging.h>
#include <fstream>

#include "base/pegasus_const.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/utils.h"

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

enum class operation_type
{
    get,
    multi_get,
    set,
    multi_set
};

struct throttle_test_plan
{
    std::string test_plan_case = "";
    operation_type ot = operation_type::get;
    int single_value_sz = 0;
    int multi_count = 0;
    int limit_qps = 0;
    bool random_value_size = false;
    bool is_hotkey = false;
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

    throttle_test_recorder()
    {
        for (const auto &key : parameter_seq) {
            records[key] = 0;
        }
    }

    void start_test(const std::string &test_case, uint64_t time_duration_s)
    {
        test_name = test_case;
        start_time_ms = dsn_now_ms();
        duration_ms = time_duration_s * 1000;
        records.emplace(std::make_pair("duration_ms", duration_ms));
    }

    bool is_time_up() { return dsn_now_ms() - start_time_ms > duration_ms; }

    void record(uint64_t size, bool is_reject)
    {
        if (is_time_up()) {
            return;
        }
        auto now_ns = dsn_now_ms();
        if (now_ns - start_time_ms <= 10) {
            TIMELY_RECORD(first_10_ms, is_reject, size);
        }
        if (now_ns - start_time_ms <= 100) {
            TIMELY_RECORD(first_100_ms, is_reject, size);
        }
        if (now_ns - start_time_ms <= 1000) {
            TIMELY_RECORD(first_1000_ms, is_reject, size);
        }
        if (now_ns - start_time_ms <= 5000) {
            TIMELY_RECORD(first_5000_ms, is_reject, size);
        }
        TIMELY_RECORD(total, is_reject, size);

        records["total_qps"] = records["total_successful_times"] / (duration_ms / 1000);
        records["total_size_per_sec"] = records["total_successful_size"] / (duration_ms / 1000);
    }

    void print_results(const std::string &dir)
    {
        std::streambuf *psbuf, *backup;
        std::ofstream file;
        file.open(dir, std::ios::out | std::ios::app);
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

// read/write throttle function test
// the details of records are saved in
// `./src/builder/test/function_test/throttle_test/throttle_test_result.txt`
class throttle_test : public testing::Test
{
public:
    static void SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

    void SetUp() override
    {
        std::vector<dsn::rpc_address> meta_list;
        ASSERT_TRUE(replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "single_master_cluster"));
        ASSERT_FALSE(meta_list.empty());

        ddl_client = std::make_shared<replication_ddl_client>(meta_list);
        ASSERT_TRUE(ddl_client != nullptr);
        pg_client =
            pegasus::pegasus_client_factory::get_client("single_master_cluster", app_name.c_str());
        ASSERT_TRUE(pg_client != nullptr);

        auto err = ddl_client->create_app(app_name.c_str(), "pegasus", 4, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);
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

    void restore_throttle()
    {
        std::map<std::string, std::string> envs;
        ddl_client->get_app_envs(app_name, envs);
        std::vector<std::string> keys;
        for (const auto &iter : envs) {
            keys.emplace_back(iter.first);
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
        std::atomic<int64_t> ref_count(0);

        while (!r.is_time_up()) {
            auto h_key = generate_hotkey(test_plan.is_hotkey, 75, test_hashkey_len);
            auto s_key = generate_random_string(test_sortkey_len);
            auto value = generate_random_string(test_plan.random_value_size
                                                    ? dsn::rand::next_u32(test_plan.single_value_sz)
                                                    : test_plan.single_value_sz);
            auto sortkey_value_pairs = generate_sortkey_value_map(
                generate_str_vector_by_random(test_sortkey_len, test_plan.multi_count),
                generate_str_vector_by_random(
                    test_plan.single_value_sz, test_plan.multi_count, test_plan.random_value_size));

            if (test_plan.ot == operation_type::set) {
                ref_count++;
                pg_client->async_set(
                    h_key,
                    s_key,
                    value,
                    [&, h_key, s_key, value](int ec, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        dassert_f(ec == PERR_OK || ec == PERR_APP_BUSY,
                                  "get/set data failed, error code:{}",
                                  ec);
                        r.record(value.size() + h_key.size() + s_key.size(), ec == PERR_APP_BUSY);
                        ref_count--;
                    });
            } else if (test_plan.ot == operation_type::multi_set) {
                ref_count++;
                pg_client->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key, sortkey_value_pairs](int ec, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        dassert_f(ec == PERR_OK || ec == PERR_APP_BUSY,
                                  "get/set data failed, error code:{}",
                                  ec);
                        int total_size = 0;
                        for (const auto &iter : sortkey_value_pairs) {
                            total_size += iter.second.size();
                        }
                        r.record(total_size + h_key.size(), ec == PERR_APP_BUSY);
                        ref_count--;
                    });
            } else if (test_plan.ot == operation_type::get) {
                ref_count++;
                pg_client->async_set(
                    h_key,
                    s_key,
                    value,
                    [&, h_key, s_key, value](int ec_write, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        dassert_f(ec_write == PERR_OK, "set data failed, error code:{}", ec_write);
                        ref_count++;
                        pg_client->async_get(
                            h_key,
                            s_key,
                            [&, h_key, s_key, value](int ec_read,
                                                     std::string &&val,
                                                     pegasus_client::internal_info &&info) {
                                if (!is_running) {
                                    ref_count--;
                                    return;
                                }
                                dassert_f(ec_read == PERR_OK || ec_read == PERR_APP_BUSY,
                                          "get data failed, error code:{}",
                                          ec_read);
                                r.record(value.size() + h_key.size() + s_key.size(),
                                         ec_read == PERR_APP_BUSY);
                                ref_count--;
                            });
                        ref_count--;
                    });
            } else if (test_plan.ot == operation_type::multi_get) {
                ref_count++;
                pg_client->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key](int ec_write, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        dassert_f(ec_write == PERR_OK, "set data failed, error code:{}", ec_write);
                        ref_count++;
                        std::set<std::string> empty_sortkeys;
                        pg_client->async_multi_get(
                            h_key,
                            empty_sortkeys,
                            [&, h_key](int ec_read,
                                       std::map<std::string, std::string> &&values,
                                       pegasus_client::internal_info &&info) {
                                if (!is_running) {
                                    ref_count--;
                                    return;
                                }
                                dassert_f(ec_read == PERR_OK || ec_read == PERR_APP_BUSY,
                                          "get data failed, error code:{}",
                                          ec_read);
                                int total_size = 0;
                                for (const auto &iter : values) {
                                    total_size += iter.second.size();
                                }
                                r.record(total_size + h_key.size(), ec_read == PERR_APP_BUSY);
                                ref_count--;
                            });
                        ref_count--;
                    });
            }
        }
        is_running = false;
        while (ref_count.load() != 0) {
            sleep(1);
        }

        r.print_results("./src/builder/test/function_test/throttle_test/throttle_test_result.txt");
        return r;
    }

    const std::string app_name = "throttle_test";
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
};

TEST_F(throttle_test, test)
{
    throttle_test_plan plan;
    throttle_test_recorder result;

    plan = {"set test / throttle by size / normal value size", operation_type::set, 1024, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {"set test / throttle by qps / normal value size", operation_type::set, 1024, 1, 50};
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env " << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {"get test / throttle by size / normal value size", operation_type::get, 1024, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by qps", operation_type::get, 1024, 1, 50};
    set_throttle(throttle_type::read_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {"multi_get test / throttle by size / normal value size",
            operation_type::multi_get,
            1024,
            50,
            50};
    set_throttle(throttle_type::read_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"multi_set test / throttle by size / normal value size",
            operation_type::multi_set,
            1024,
            50,
            50};
    set_throttle(throttle_type::write_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);
    plan = {
        "set test / throttle by qps&size / normal value size", operation_type::set, 1024, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {
        "get test / throttle by qps&size / normal value size", operation_type::get, 1024, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    set_throttle(throttle_type::read_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    // mix throttle case
    plan = {"set test / throttle by qps&size,loose size throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps *
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * 1000);
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {"get test / throttle by qps&size,loose size throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    set_throttle(throttle_type::read_by_size,
                 plan.limit_qps *
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * 1000);
    set_throttle(throttle_type::read_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {"set test / throttle by qps&size,loose qps throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    set_throttle(throttle_type::write_by_qps, plan.limit_qps * 1000);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by qps&size,loose qps throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    set_throttle(throttle_type::read_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    set_throttle(throttle_type::read_by_qps, plan.limit_qps * 1000);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    // big value test can't run normally in the function test
    plan = {"set test / throttle by size / 20kb value size", operation_type::set, 1024 * 20, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by size / 20kb value size", operation_type::get, 1024 * 20, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"set test / throttle by size / 50kb value size", operation_type::set, 1024 * 50, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by size / 50kb value size", operation_type::get, 1024 * 50, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"set test / throttle by size / 100b value size", operation_type::set, 100, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by size / 100b value size", operation_type::get, 100, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                     plan.multi_count * plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"set test / throttle by size / 10b value size", operation_type::set, 10, 1, 50};
    set_throttle(throttle_type::write_by_size,
                 (plan.single_value_sz + test_hashkey_len + test_sortkey_len) * plan.multi_count *
                     plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    plan = {"get test / throttle by size / 10b value size", operation_type::get, 10, 1, 50};
    set_throttle(throttle_type::read_by_size,
                 (plan.single_value_sz + test_hashkey_len + test_sortkey_len) * plan.multi_count *
                     plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                  plan.multi_count * plan.limit_qps * 0.7);

    //  random value case
    plan = {"multi_get test / throttle by size / random value size",
            operation_type::multi_get,
            1024 * 5,
            50,
            50,
            true};
    set_throttle(throttle_type::read_by_size, 5000000);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"], (uint64_t)5000000 * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"], (uint64_t)5000000 * 0.7);

    plan = {"multi_set test / throttle by size / random value size",
            operation_type::multi_set,
            1024 * 5,
            50,
            50,
            true};
    set_throttle(throttle_type::write_by_size, 5000000);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"], (uint64_t)5000000 * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"], (uint64_t)5000000 * 0.7);

    // hotkey test
    plan = {
        "get test / throttle by qps / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    set_throttle(throttle_type::read_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {
        "set test / throttle by qps / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    set_throttle(throttle_type::write_by_qps, plan.limit_qps);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 15);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 15);

    plan = {
        "set test / throttle by size / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    set_throttle(throttle_type::write_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count * 0.7);

    plan = {
        "get test / throttle by size / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    set_throttle(throttle_type::read_by_size,
                 plan.limit_qps * plan.single_value_sz * plan.multi_count);
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_size_per_sec"],
              (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count * 1.3);
    ASSERT_GT(result.records["total_size_per_sec"],
              (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count * 0.7);

    // mix delay&reject test
    plan = {
        "set test / throttle by qps 500 / no delay throttle", operation_type::set, 1024, 1, 500};
    ddl_client->set_app_envs(app_name, {"replica.write_throttling"}, {"500*reject*200"});
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 100);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 100);

    plan = {
        "get test / throttle by qps 500 / no delay throttle", operation_type::get, 1024, 1, 500};
    ddl_client->set_app_envs(app_name, {"replica.read_throttling"}, {"500*reject*200"});
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 100);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 100);

    plan = {"set test / throttle by qps 500 / delay throttle", operation_type::set, 1024, 1, 500};
    ddl_client->set_app_envs(
        app_name, {"replica.write_throttling"}, {"300*delay*100,500*reject*200"});
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 100);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 100);

    plan = {"get test / throttle by qps 500 / delay throttle", operation_type::get, 1024, 1, 500};
    ddl_client->set_app_envs(
        app_name, {"replica.read_throttling"}, {"300*delay*100,500*reject*200"});
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    result = start_test(plan);
    restore_throttle();
    ASSERT_LE(result.records["total_qps"], plan.limit_qps + 100);
    ASSERT_GT(result.records["total_qps"], plan.limit_qps - 100);
}
