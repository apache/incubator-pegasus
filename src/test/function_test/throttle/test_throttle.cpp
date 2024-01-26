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
#include <stdint.h>
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "runtime/api_layer1.h"
#include "test/function_test/utils/test_util.h"
#include "test/function_test/utils/utils.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"
#include "utils/test_macros.h"

DSN_DEFINE_int32(function_test.throttle_test,
                 throttle_test_medium_value_kb,
                 20,
                 "The size of generated medium value for test");

DSN_DEFINE_int32(function_test.throttle_test,
                 throttle_test_large_value_kb,
                 50,
                 "The size of generated large value for test");

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;
using std::string;

static const uint64_t kLimitDurationMs = 10 * 1000;

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
    string test_plan_case;
    operation_type ot = operation_type::get;
    int single_value_sz = 0;
    int multi_count = 0;
    int limit_qps = 0;
    bool random_value_size = false;
    bool is_hotkey = false;
};

struct throttle_test_recorder
{
    string test_name;
    uint64_t start_time_ms;
    std::atomic<uint64_t> total_successful_times;
    std::atomic<uint64_t> total_successful_size;
    uint64_t total_qps = 0;
    uint64_t total_size_per_sec = 0;

    throttle_test_recorder() {}

    void reset(const string &test_case)
    {
        test_name = test_case;
        start_time_ms = dsn_now_ms();
        total_successful_times = 0;
        total_successful_size = 0;
        total_qps = 0;
        total_size_per_sec = 0;
    }

    bool is_time_up() { return dsn_now_ms() - start_time_ms > kLimitDurationMs; }

    void record(uint64_t size, bool is_reject)
    {
        if (!is_reject) {
            total_successful_times++;
            total_successful_size += size;
        }
    }

    void finalize()
    {
        total_qps = total_successful_times / (kLimitDurationMs / 1000);
        total_size_per_sec = total_successful_size / (kLimitDurationMs / 1000);
    }
};

// read/write throttle function test
// the details of records are saved in
// `./src/builder/test/function_test/throttle/throttle_test_result.txt`
class throttle_test : public test_util
{
public:
    const int test_hashkey_len = 50;
    const int test_sortkey_len = 50;
    throttle_test_recorder result;

public:
    throttle_test() { TRICKY_CODE_TO_AVOID_LINK_ERROR; }

    void set_throttle(throttle_type type, uint64_t value)
    {
        std::vector<string> keys, values;
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
        NO_FATALS(update_table_env(keys, values));
    }

    void restore_throttle()
    {
        std::map<string, string> envs;
        ASSERT_EQ(ERR_OK, ddl_client_->get_app_envs(table_name_, envs));
        std::vector<string> keys;
        for (const auto &env : envs) {
            keys.emplace_back(env.first);
        }
        ASSERT_EQ(ERR_OK, ddl_client_->del_app_envs(table_name_, keys));
    }

    void start_test(const throttle_test_plan &test_plan)
    {
        fmt::print(
            "start test, on {}, duration {} ms\n", test_plan.test_plan_case, kLimitDurationMs);

        result.reset(test_plan.test_plan_case);

        std::atomic<bool> is_running(true);
        std::atomic<int64_t> ref_count(0);
        std::atomic<int> last_error(PERR_OK);

        while (!result.is_time_up() && (last_error == PERR_OK || last_error == PERR_APP_BUSY)) {
            auto h_key = generate_hotkey(test_plan.is_hotkey, 75, test_hashkey_len);
            auto s_key = generate_random_string(test_sortkey_len);
            auto value = generate_random_string(test_plan.random_value_size
                                                    ? dsn::rand::next_u32(test_plan.single_value_sz)
                                                    : test_plan.single_value_sz);
            auto sortkey_value_pairs = generate_sortkey_value_map(
                generate_str_vector_by_random(test_sortkey_len, test_plan.multi_count),
                generate_str_vector_by_random(
                    test_plan.single_value_sz, test_plan.multi_count, test_plan.random_value_size));

            switch (test_plan.ot) {
            case operation_type::set: {
                ref_count++;
                client_->async_set(
                    h_key,
                    s_key,
                    value,
                    [&, h_key, s_key, value](int ec, pegasus_client::internal_info &&info) {
                        ref_count--;
                        if (!is_running) {
                            return;
                        }
                        if (ec != PERR_OK && ec != PERR_APP_BUSY) {
                            last_error = ec;
                            return;
                        }
                        result.record(value.size() + h_key.size() + s_key.size(),
                                      ec == PERR_APP_BUSY);
                    });
                break;
            }
            case operation_type::multi_set: {
                ref_count++;
                client_->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key, sortkey_value_pairs](int ec, pegasus_client::internal_info &&info) {
                        ref_count--;
                        if (!is_running) {
                            return;
                        }
                        if (ec != PERR_OK && ec != PERR_APP_BUSY) {
                            last_error = ec;
                            return;
                        }
                        int total_size = 0;
                        for (const auto &iter : sortkey_value_pairs) {
                            total_size += iter.second.size();
                        }
                        result.record(total_size + h_key.size(), ec == PERR_APP_BUSY);
                    });
                break;
            }
            case operation_type::get: {
                ref_count++;
                client_->async_set(
                    h_key,
                    s_key,
                    value,
                    [&, h_key, s_key, value](int ec_write, pegasus_client::internal_info &&info) {
                        ref_count--;
                        if (!is_running) {
                            return;
                        }
                        if (ec_write != PERR_OK) {
                            last_error = ec_write;
                            return;
                        }
                        ref_count++;
                        client_->async_get(
                            h_key,
                            s_key,
                            [&, h_key, s_key, value](
                                int ec_read, string &&val, pegasus_client::internal_info &&info) {
                                ref_count--;
                                if (!is_running) {
                                    return;
                                }
                                if (ec_read != PERR_OK && ec_read != PERR_APP_BUSY) {
                                    last_error = ec_read;
                                    return;
                                }
                                result.record(value.size() + h_key.size() + s_key.size(),
                                              ec_read == PERR_APP_BUSY);
                            });
                    });
                break;
            }
            case operation_type::multi_get: {
                ref_count++;
                client_->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key](int ec_write, pegasus_client::internal_info &&info) {
                        ref_count--;
                        if (!is_running) {
                            return;
                        }
                        if (ec_write != PERR_OK) {
                            last_error = ec_write;
                            return;
                        }
                        ref_count++;
                        client_->async_multi_get(
                            h_key,
                            {},
                            [&, h_key](int ec_read,
                                       std::map<string, string> &&values,
                                       pegasus_client::internal_info &&info) {
                                ref_count--;
                                if (!is_running) {
                                    return;
                                }
                                if (ec_read != PERR_OK && ec_read != PERR_APP_BUSY) {
                                    last_error = ec_read;
                                    return;
                                }
                                int total_size = 0;
                                for (const auto &iter : values) {
                                    total_size += iter.second.size();
                                }
                                result.record(total_size + h_key.size(), ec_read == PERR_APP_BUSY);
                            });
                    });
                break;
            }
            }
        }
        is_running = false;
        int try_times = 0;
        ASSERT_IN_TIME(
            [&] {
                LOG_INFO("Start to try the {}th time", ++try_times);
                ASSERT_EQ(0, ref_count.load());
                ASSERT_TRUE(last_error == PERR_OK || last_error == PERR_APP_BUSY) << last_error;
            },
            3 * kLimitDurationMs / 1000);
    }
};

#define TEST_PLAN_DESC_custom_kb(op, sz)                                                           \
    fmt::format(#op " test / throttle by size / {}kb value size", sz)

TEST_F(throttle_test, test)
{
    throttle_test_plan plan;

    plan = {"set test / throttle by size / normal value size", operation_type::set, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"set test / throttle by qps / normal value size", operation_type::set, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"get test / throttle by size / normal value size", operation_type::get, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    auto actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                        plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by qps", operation_type::get, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"multi_get test / throttle by size / normal value size",
            operation_type::multi_get,
            1024,
            50,
            50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"multi_set test / throttle by size / normal value size",
            operation_type::multi_set,
            1024,
            50,
            50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);
    plan = {
        "set test / throttle by qps&size / normal value size", operation_type::set, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "get test / throttle by qps&size / normal value size", operation_type::get, 1024, 1, 50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    // mix throttle case
    plan = {"set test / throttle by qps&size,loose size throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    NO_FATALS(set_throttle(
        throttle_type::write_by_size,
        plan.limit_qps * (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
            plan.multi_count * 1000));
    NO_FATALS(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"get test / throttle by qps&size,loose size throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    NO_FATALS(set_throttle(
        throttle_type::read_by_size,
        plan.limit_qps * (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
            plan.multi_count * 1000));
    NO_FATALS(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"set test / throttle by qps&size,loose qps throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(set_throttle(throttle_type::write_by_qps, plan.limit_qps * 1000));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by qps&size,loose qps throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(set_throttle(throttle_type::read_by_qps, plan.limit_qps * 1000));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    // big value test can't run normally in the function test
    plan = {TEST_PLAN_DESC_custom_kb(set, FLAGS_throttle_test_medium_value_kb),
            operation_type::set,
            1024 * FLAGS_throttle_test_medium_value_kb,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {TEST_PLAN_DESC_custom_kb(get, FLAGS_throttle_test_medium_value_kb),
            operation_type::get,
            1024 * FLAGS_throttle_test_medium_value_kb,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {TEST_PLAN_DESC_custom_kb(set, FLAGS_throttle_test_large_value_kb),
            operation_type::set,
            1024 * FLAGS_throttle_test_large_value_kb,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {TEST_PLAN_DESC_custom_kb(get, FLAGS_throttle_test_large_value_kb),
            operation_type::get,
            1024 * FLAGS_throttle_test_large_value_kb,
            1,
            50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"set test / throttle by size / 100b value size", operation_type::set, 100, 1, 50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 100b value size", operation_type::get, 100, 1, 50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"set test / throttle by size / 10b value size", operation_type::set, 10, 1, 50};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           (plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 10b value size", operation_type::get, 10, 1, 50};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           (plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                               plan.multi_count * plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    //  random value case
    plan = {"multi_get test / throttle by size / random value size",
            operation_type::multi_get,
            1024 * 5,
            50,
            50,
            true};
    NO_FATALS(set_throttle(throttle_type::read_by_size, 5000000));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, (uint64_t)5000000, (uint64_t)5000000 * 0.3);

    plan = {"multi_set test / throttle by size / random value size",
            operation_type::multi_set,
            1024 * 5,
            50,
            50,
            true};
    NO_FATALS(set_throttle(throttle_type::write_by_size, 5000000));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, (uint64_t)5000000, (uint64_t)5000000 * 0.3);

    // hotkey test
    plan = {
        "get test / throttle by qps / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    NO_FATALS(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "set test / throttle by qps / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    NO_FATALS(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "set test / throttle by size / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    NO_FATALS(set_throttle(throttle_type::write_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {
        "get test / throttle by size / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    NO_FATALS(set_throttle(throttle_type::read_by_size,
                           plan.limit_qps * plan.single_value_sz * plan.multi_count));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    actual_value = (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    // mix delay&reject test
    plan = {
        "set test / throttle by qps 500 / no delay throttle", operation_type::set, 1024, 1, 500};
    NO_FATALS(update_table_env({"replica.write_throttling"}, {"500*reject*200"}));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {
        "get test / throttle by qps 500 / no delay throttle", operation_type::get, 1024, 1, 500};
    NO_FATALS(update_table_env({"replica.read_throttling"}, {"500*reject*200"}));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {"set test / throttle by qps 500 / delay throttle", operation_type::set, 1024, 1, 500};
    NO_FATALS(update_table_env({"replica.write_throttling"}, {"300*delay*100,500*reject*200"}));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {"get test / throttle by qps 500 / delay throttle", operation_type::get, 1024, 1, 500};
    NO_FATALS(update_table_env({"replica.read_throttling"}, {"300*delay*100,500*reject*200"}));
    NO_FATALS(start_test(plan));
    NO_FATALS(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);
}
