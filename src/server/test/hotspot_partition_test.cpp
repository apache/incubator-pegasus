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

#include "server/hotspot_partition_calculator.h"

#include "pegasus_server_test_base.h"
#include <gtest/gtest.h>
#include <dsn/utility/fail_point.h>

namespace pegasus {
namespace server {

DSN_DECLARE_int32(occurrence_threshold);

class hotspot_partition_test : public pegasus_server_test_base
{
public:
    hotspot_partition_test() : calculator("TEST", 8)
    {
        dsn::fail::setup();
        dsn::fail::cfg("send_hotkey_detect_request", "return()");
    };
    ~hotspot_partition_test() { dsn::fail::teardown(); }

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

    void test_policy_in_scenarios(std::vector<row_data> scenario,
                                  std::vector<std::vector<double>> &expect_result,
                                  hotspot_partition_calculator &calculator)
    {
        calculator.data_aggregate(std::move(scenario));
        calculator.data_analyse();
        std::vector<std::vector<double>> result = get_calculator_result(calculator._hot_points);
        ASSERT_EQ(result, expect_result);
    }

    void aggregate_analyse_data(hotspot_partition_calculator &calculator,
                                std::vector<row_data> scenario,
                                std::vector<std::array<int, 2>> &expect_result,
                                int loop_times)
    {
        for (int i = 0; i < loop_times; i++) {
            calculator.data_aggregate(scenario);
            calculator.data_analyse();
        }
        ASSERT_EQ(calculator._hotpartition_counter, expect_result);
    }

    void clear_calculator_histories(hotspot_partition_calculator &calculator)
    {
        calculator._partitions_stat_histories.clear();
    }
};

TEST_F(hotspot_partition_test, hotspot_partition_policy)
{
    // Insert normal scenario data to test
    std::vector<row_data> test_rows = generate_row_data();
    std::vector<std::vector<double>> expect_vector = {{0, 0, 0, 0, 0, 0, 0, 0},
                                                      {0, 0, 0, 0, 0, 0, 0, 0}};
    test_policy_in_scenarios(test_rows, expect_vector, calculator);

    // Insert hotspot scenario_0 data to test
    test_rows = generate_row_data();
    const int HOT_SCENARIO_0_READ_HOT_PARTITION = 7;
    const int HOT_SCENARIO_0_WRITE_HOT_PARTITION = 0;
    test_rows[HOT_SCENARIO_0_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_0_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 0, 0, 0, 0, 4}, {4, 0, 0, 0, 0, 0, 0, 0}};
    test_policy_in_scenarios(test_rows, expect_vector, calculator);

    // Insert hotspot scenario_0 data to test again
    test_rows = generate_row_data();
    test_rows[HOT_SCENARIO_0_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_0_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 0, 0, 0, 0, 4}, {4, 0, 0, 0, 0, 0, 0, 0}};
    test_policy_in_scenarios(test_rows, expect_vector, calculator);

    // Insert hotspot scenario_1 data to test again
    test_rows = generate_row_data();
    const int HOT_SCENARIO_1_READ_HOT_PARTITION = 3;
    const int HOT_SCENARIO_1_WRITE_HOT_PARTITION = 2;
    test_rows[HOT_SCENARIO_1_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_1_WRITE_HOT_PARTITION].put_qps = 5000.0;
    expect_vector = {{0, 0, 0, 4, 0, 0, 0, 0}, {0, 0, 4, 0, 0, 0, 0, 0}};
    test_policy_in_scenarios(test_rows, expect_vector, calculator);
    clear_calculator_histories(calculator);
}

TEST_F(hotspot_partition_test, send_hotkey_detect_request)
{
    const int READ_HOT_PARTITION = 7;
    const int WRITE_HOT_PARTITION = 0;
    std::vector<row_data> test_rows = generate_row_data();
    test_rows[READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[WRITE_HOT_PARTITION].put_qps = 5000.0;
    int hotpartition_count = FLAGS_occurrence_threshold;
    std::vector<std::array<int, 2>> expect_result = {{0, hotpartition_count},
                                                     {0, 0},
                                                     {0, 0},
                                                     {0, 0},
                                                     {0, 0},
                                                     {0, 0},
                                                     {0, 0},
                                                     {hotpartition_count, 0}};
    aggregate_analyse_data(calculator, test_rows, expect_result, FLAGS_occurrence_threshold);
    const int back_to_normal = 30;
    hotpartition_count = FLAGS_occurrence_threshold - back_to_normal;
    expect_result = {{0, hotpartition_count},
                     {0, 0},
                     {0, 0},
                     {0, 0},
                     {0, 0},
                     {0, 0},
                     {0, 0},
                     {hotpartition_count, 0}};
    aggregate_analyse_data(calculator, generate_row_data(), expect_result, back_to_normal);
}

} // namespace server
} // namespace pegasus
