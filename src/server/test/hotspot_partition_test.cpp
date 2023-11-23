// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <array>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pegasus_server_test_base.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "server/hotspot_partition_calculator.h"
#include "server/hotspot_partition_stat.h"
#include "shell/command_helper.h"
#include "utils/fail_point.h"
#include "utils/flags.h"

namespace pegasus {
namespace server {

DSN_DECLARE_int32(occurrence_threshold);
DSN_DECLARE_bool(enable_detect_hotkey);

class hotspot_partition_test : public pegasus_server_test_base
{
public:
    hotspot_partition_test() : calculator("TEST", 8, nullptr)
    {
        dsn::fail::setup();
        dsn::fail::cfg("send_detect_hotkey_request", "return()");
        FLAGS_enable_detect_hotkey = true;
    };
    ~hotspot_partition_test()
    {
        FLAGS_enable_detect_hotkey = false;
        dsn::fail::teardown();
    }

    hotspot_partition_calculator calculator;

    std::vector<row_data> generate_row_data()
    {
        std::vector<row_data> test_rows;
        test_rows.resize(8);
        for (int i = 0; i < 8; i++) {
            test_rows[i].get_qps = 1000.0;
            test_rows[i].put_qps = 1000.0;
        }
        return test_rows;
    }

    std::vector<std::array<int, 2>> generate_result()
    {
        return {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
    }

    std::vector<std::vector<double>> get_calculator_result(const hot_partition_counters &counters)
    {
        std::vector<std::vector<double>> result;
        result.resize(2);
        for (int i = 0; i < counters.size(); i++) {
            result[READ_HOTSPOT_DATA].push_back(counters[i][READ_HOTSPOT_DATA].get()->get_value());
            result[WRITE_HOTSPOT_DATA].push_back(
                counters[i][WRITE_HOTSPOT_DATA].get()->get_value());
        }
        return result;
    }

    std::array<uint32_t, 2>
    get_calculator_total_hotspot_cnt(const std::array<dsn::perf_counter_wrapper, 2> &cnts)
    {
        std::array<uint32_t, 2> result;
        result[READ_HOTSPOT_DATA] = cnts[READ_HOTSPOT_DATA].get()->get_value();
        result[WRITE_HOTSPOT_DATA] = cnts[WRITE_HOTSPOT_DATA].get()->get_value();
        return result;
    }

    void test_policy_in_scenarios(std::vector<row_data> scenario,
                                  std::vector<std::vector<double>> &expect_result,
                                  std::array<uint32_t, 2> expect_cnt)
    {
        calculator.data_aggregate(std::move(scenario));
        calculator.data_analyse();
        std::vector<std::vector<double>> result = get_calculator_result(calculator._hot_points);
        auto cnt = get_calculator_total_hotspot_cnt(calculator._total_hotspot_cnt);

        ASSERT_EQ(result, expect_result);
        ASSERT_EQ(cnt, expect_cnt);
    }

    void aggregate_analyse_data(std::vector<row_data> scenario,
                                std::vector<std::array<int, 2>> &expect_result,
                                int loop_times)
    {
        for (int i = 0; i < loop_times; i++) {
            calculator.data_aggregate(scenario);
            calculator.data_analyse();
        }
        ASSERT_EQ(calculator._hotpartition_counter, expect_result);
    }

    void clear_calculator_histories() { calculator._partitions_stat_histories.clear(); }
};

INSTANTIATE_TEST_CASE_P(, hotspot_partition_test, ::testing::Values(false, true));

TEST_P(hotspot_partition_test, hotspot_partition_policy)
{
    // Insert normal scenario data to test
    std::vector<row_data> test_rows = generate_row_data();
    std::vector<std::vector<double>> expect_vector = {{0, 0, 0, 0, 0, 0, 0, 0},
                                                      {0, 0, 0, 0, 0, 0, 0, 0}};

    std::array<uint32_t, 2> expect_hotspot_cnt = {0, 0};
    test_policy_in_scenarios(test_rows, expect_vector, expect_hotspot_cnt);

    // Insert hotspot scenario_0 data to test
    test_rows = generate_row_data();
    const int HOT_SCENARIO_0_READ_HOT_PARTITION = 7;
    const int HOT_SCENARIO_0_WRITE_HOT_PARTITION = 0;
    test_rows[HOT_SCENARIO_0_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_0_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 0, 0, 0, 0, 4}, {4, 0, 0, 0, 0, 0, 0, 0}};
    expect_hotspot_cnt = {1, 1};
    test_policy_in_scenarios(test_rows, expect_vector, expect_hotspot_cnt);

    // Insert hotspot scenario_0 data to test again
    test_rows = generate_row_data();
    test_rows[HOT_SCENARIO_0_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_0_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 0, 0, 0, 0, 4}, {4, 0, 0, 0, 0, 0, 0, 0}};
    expect_hotspot_cnt = {1, 1};
    test_policy_in_scenarios(test_rows, expect_vector, expect_hotspot_cnt);

    // Insert hotspot scenario_1 data to test again
    test_rows = generate_row_data();
    const int HOT_SCENARIO_1_READ_HOT_PARTITION = 3;
    const int HOT_SCENARIO_1_WRITE_HOT_PARTITION = 2;
    test_rows[HOT_SCENARIO_1_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_1_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 4, 0, 0, 0, 0}, {0, 0, 4, 0, 0, 0, 0, 0}};
    expect_hotspot_cnt = {1, 1};
    test_policy_in_scenarios(test_rows, expect_vector, expect_hotspot_cnt);

    test_rows = generate_row_data();
    const int HOT_SCENARIO_2_READ_HOT_PARTITION_0 = 3;
    const int HOT_SCENARIO_2_READ_HOT_PARTITION_1 = 5;
    const int HOT_SCENARIO_2_WRITE_HOT_PARTITION = 2;

    test_rows[HOT_SCENARIO_2_READ_HOT_PARTITION_0].get_qps = 7000.0;
    test_rows[HOT_SCENARIO_2_READ_HOT_PARTITION_1].get_qps = 8000.0;
    test_rows[HOT_SCENARIO_2_WRITE_HOT_PARTITION].put_qps = 7000.0;

    expect_vector = {{0, 0, 0, 4, 0, 4, 0, 0}, {0, 0, 4, 0, 0, 0, 0, 0}};
    expect_hotspot_cnt = {2, 1};
    test_policy_in_scenarios(test_rows, expect_vector, expect_hotspot_cnt);
    clear_calculator_histories();
}

TEST_P(hotspot_partition_test, send_detect_hotkey_request)
{
    const int READ_HOT_PARTITION = 7;
    const int WRITE_HOT_PARTITION = 0;
    std::vector<row_data> test_rows = generate_row_data();
    test_rows[READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[WRITE_HOT_PARTITION].put_qps = 5000.0;
    auto expect_result = generate_result();
    expect_result[READ_HOT_PARTITION][0] = FLAGS_occurrence_threshold;
    expect_result[WRITE_HOT_PARTITION][1] = FLAGS_occurrence_threshold;
    aggregate_analyse_data(test_rows, expect_result, FLAGS_occurrence_threshold);
    const int back_to_normal = 30;
    expect_result[READ_HOT_PARTITION][0] = 0;
    expect_result[WRITE_HOT_PARTITION][1] = 0;
    aggregate_analyse_data(generate_row_data(), expect_result, back_to_normal);
}

} // namespace server
} // namespace pegasus
