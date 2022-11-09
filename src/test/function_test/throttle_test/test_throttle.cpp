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

#include <atomic>

#include "utils/filesystem.h"
#include "client/replication_ddl_client.h"
#include "include/pegasus/client.h"
#include <gtest/gtest.h>
#include "utils/TokenBucket.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "utils/fmt_logging.h"
#include <fstream>

#include "base/pegasus_const.h"
#include "test/function_test/utils/global_env.h"
#include "test/function_test/utils/utils.h"
#include "test/function_test/utils/test_util.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;
using std::string;

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
    static const uint64_t limit_duration_ms = 10 * 1000;

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

    bool is_time_up() { return dsn_now_ms() - start_time_ms > limit_duration_ms; }

    void record(uint64_t size, bool is_reject)
    {
        if (!is_reject) {
            total_successful_times++;
            total_successful_size += size;
        }
    }

    void finalize()
    {
        total_qps = total_successful_times / (limit_duration_ms / 1000);
        total_size_per_sec = total_successful_size / (limit_duration_ms / 1000);
    }
};

// read/write throttle function test
// the details of records are saved in
// `./src/builder/test/function_test/throttle_test/throttle_test_result.txt`
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
        ASSERT_EQ(ERR_OK, ddl_client_->set_app_envs(app_name_, keys, values).get_error().code());
    }

    void restore_throttle()
    {
        std::map<string, string> envs;
        ASSERT_EQ(ERR_OK, ddl_client_->get_app_envs(app_name_, envs));
        std::vector<string> keys;
        for (const auto &env : envs) {
            keys.emplace_back(env.first);
        }
        ASSERT_EQ(ERR_OK, ddl_client_->del_app_envs(app_name_, keys));
    }

    void start_test(const throttle_test_plan &test_plan)
    {
        std::cout << fmt::format("start test, on {}", test_plan.test_plan_case) << std::endl;

        result.reset(test_plan.test_plan_case);

        bool is_running = true;
        std::atomic<int64_t> ref_count(0);

        while (!result.is_time_up()) {
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
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        ASSERT_TRUE(ec == PERR_OK || ec == PERR_APP_BUSY) << ec;
                        result.record(value.size() + h_key.size() + s_key.size(),
                                      ec == PERR_APP_BUSY);
                        ref_count--;
                    });
                break;
            }
            case operation_type::multi_set: {
                ref_count++;
                client_->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key, sortkey_value_pairs](int ec, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        ASSERT_TRUE(ec == PERR_OK || ec == PERR_APP_BUSY) << ec;
                        int total_size = 0;
                        for (const auto &iter : sortkey_value_pairs) {
                            total_size += iter.second.size();
                        }
                        result.record(total_size + h_key.size(), ec == PERR_APP_BUSY);
                        ref_count--;
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
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        ASSERT_EQ(PERR_OK, ec_write);
                        ref_count++;
                        client_->async_get(
                            h_key,
                            s_key,
                            [&, h_key, s_key, value](
                                int ec_read, string &&val, pegasus_client::internal_info &&info) {
                                if (!is_running) {
                                    ref_count--;
                                    return;
                                }
                                ASSERT_TRUE(ec_read == PERR_OK || ec_read == PERR_APP_BUSY)
                                    << ec_read;
                                result.record(value.size() + h_key.size() + s_key.size(),
                                              ec_read == PERR_APP_BUSY);
                                ref_count--;
                            });
                        ref_count--;
                    });
                break;
            }
            case operation_type::multi_get: {
                ref_count++;
                client_->async_multi_set(
                    h_key,
                    sortkey_value_pairs,
                    [&, h_key](int ec_write, pegasus_client::internal_info &&info) {
                        if (!is_running) {
                            ref_count--;
                            return;
                        }
                        ASSERT_EQ(PERR_OK, ec_write);
                        ref_count++;
                        client_->async_multi_get(
                            h_key,
                            {},
                            [&, h_key](int ec_read,
                                       std::map<string, string> &&values,
                                       pegasus_client::internal_info &&info) {
                                if (!is_running) {
                                    ref_count--;
                                    return;
                                }
                                ASSERT_TRUE(ec_read == PERR_OK || ec_read == PERR_APP_BUSY)
                                    << ec_read;
                                int total_size = 0;
                                for (const auto &iter : values) {
                                    total_size += iter.second.size();
                                }
                                result.record(total_size + h_key.size(), ec_read == PERR_APP_BUSY);
                                ref_count--;
                            });
                        ref_count--;
                    });
                break;
            }
            }
        }
        is_running = false;
        while (ref_count.load() != 0) {
            sleep(1);
        }
    }
};

TEST_F(throttle_test, test)
{
    throttle_test_plan plan;

    plan = {"set test / throttle by size / normal value size", operation_type::set, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"set test / throttle by qps / normal value size", operation_type::set, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env " << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"get test / throttle by size / normal value size", operation_type::get, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    auto actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                        plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by qps", operation_type::get, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"multi_get test / throttle by size / normal value size",
            operation_type::multi_get,
            1024,
            50,
            50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::read_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"multi_set test / throttle by size / normal value size",
            operation_type::multi_set,
            1024,
            50,
            50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::write_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);
    plan = {
        "set test / throttle by qps&size / normal value size", operation_type::set, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "get test / throttle by qps&size / normal value size", operation_type::get, 1024, 1, 50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    // mix throttle case
    plan = {"set test / throttle by qps&size,loose size throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(
        throttle_type::write_by_size,
        plan.limit_qps * (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
            plan.multi_count * 1000));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"get test / throttle by qps&size,loose size throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(
        throttle_type::read_by_size,
        plan.limit_qps * (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
            plan.multi_count * 1000));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {"set test / throttle by qps&size,loose qps throttle / normal value size",
            operation_type::set,
            1024,
            1,
            50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_qps, plan.limit_qps * 1000));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by qps&size,loose qps throttle/normal value size",
            operation_type::get,
            1024,
            1,
            50};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_qps, plan.limit_qps * 1000));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    // big value test can't run normally in the function test
    plan = {"set test / throttle by size / 20kb value size", operation_type::set, 1024 * 20, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::write_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 20kb value size", operation_type::get, 1024 * 20, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::read_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"set test / throttle by size / 50kb value size", operation_type::set, 1024 * 50, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::write_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 50kb value size", operation_type::get, 1024 * 50, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::read_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"set test / throttle by size / 100b value size", operation_type::set, 100, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::write_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 100b value size", operation_type::get, 100, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::read_by_size,
                     (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"set test / throttle by size / 10b value size", operation_type::set, 10, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::write_by_size,
                     (plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)(plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                   plan.multi_count * plan.limit_qps;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {"get test / throttle by size / 10b value size", operation_type::get, 10, 1, 50};
    ASSERT_NO_FATAL_FAILURE(
        set_throttle(throttle_type::read_by_size,
                     (plan.single_value_sz + test_hashkey_len + test_sortkey_len) *
                         plan.multi_count * plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
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
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_size, 5000000));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, (uint64_t)5000000, (uint64_t)5000000 * 0.3);

    plan = {"multi_set test / throttle by size / random value size",
            operation_type::multi_set,
            1024 * 5,
            50,
            50,
            true};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_size, 5000000));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, (uint64_t)5000000, (uint64_t)5000000 * 0.3);

    // hotkey test
    plan = {
        "get test / throttle by qps / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "set test / throttle by qps / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_qps, plan.limit_qps));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 15);

    plan = {
        "set test / throttle by size / hotkey test", operation_type::set, 1024, 1, 50, false, true};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::write_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    plan = {
        "get test / throttle by size / hotkey test", operation_type::get, 1024, 1, 50, false, true};
    ASSERT_NO_FATAL_FAILURE(set_throttle(throttle_type::read_by_size,
                                         plan.limit_qps * plan.single_value_sz * plan.multi_count));
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    actual_value = (uint64_t)plan.limit_qps * plan.single_value_sz * plan.multi_count;
    result.finalize();
    ASSERT_NEAR(result.total_size_per_sec, actual_value, actual_value * 0.3);

    // mix delay&reject test
    plan = {
        "set test / throttle by qps 500 / no delay throttle", operation_type::set, 1024, 1, 500};
    ASSERT_EQ(ERR_OK,
              ddl_client_->set_app_envs(app_name_, {"replica.write_throttling"}, {"500*reject*200"})
                  .get_error()
                  .code());
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {
        "get test / throttle by qps 500 / no delay throttle", operation_type::get, 1024, 1, 500};
    ASSERT_EQ(ERR_OK,
              ddl_client_->set_app_envs(app_name_, {"replica.read_throttling"}, {"500*reject*200"})
                  .get_error()
                  .code());
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {"set test / throttle by qps 500 / delay throttle", operation_type::set, 1024, 1, 500};
    ASSERT_EQ(ERR_OK,
              ddl_client_
                  ->set_app_envs(
                      app_name_, {"replica.write_throttling"}, {"300*delay*100,500*reject*200"})
                  .get_error()
                  .code());
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);

    plan = {"get test / throttle by qps 500 / delay throttle", operation_type::get, 1024, 1, 500};
    ASSERT_EQ(
        ERR_OK,
        ddl_client_
            ->set_app_envs(app_name_, {"replica.read_throttling"}, {"300*delay*100,500*reject*200"})
            .get_error()
            .code());
    std::cout << "wait 30s for setting env" << std::endl;
    sleep(30);
    ASSERT_NO_FATAL_FAILURE(start_test(plan));
    ASSERT_NO_FATAL_FAILURE(restore_throttle());
    result.finalize();
    ASSERT_NEAR(result.total_qps, plan.limit_qps, 100);
}
