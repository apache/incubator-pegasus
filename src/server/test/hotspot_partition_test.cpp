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

#include <gtest/gtest.h>

namespace pegasus {
namespace server {

const int HOT_SCENARIO_0_READ_HOT_PARTITION = 7;
const int HOT_SCENARIO_0_WRITE_HOT_PARTITION = 0;
std::vector<row_data> generate_hot_scenario_0()
{
    std::vector<row_data> test_rows;
    test_rows.resize(8);
    for (int i = 0; i < 8; i++) {
        test_rows[i].get_qps = 1000.0;
        test_rows[i].put_qps = 1000.0;
    }
    test_rows[HOT_SCENARIO_0_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_0_WRITE_HOT_PARTITION].put_qps = 5000.0;
    return test_rows;
}

const int HOT_SCENARIO_1_READ_HOT_PARTITION = 3;
const int HOT_SCENARIO_1_WRITE_HOT_PARTITION = 2;
std::vector<row_data> generate_hot_scenario_1()
{
    std::vector<row_data> test_rows;
    test_rows.resize(8);
    for (int i = 0; i < 8; i++) {
        test_rows[i].get_qps = 1000.0;
        test_rows[i].put_qps = 1000.0;
    }
    test_rows[HOT_SCENARIO_1_READ_HOT_PARTITION].get_qps = 5000.0;
    test_rows[HOT_SCENARIO_1_WRITE_HOT_PARTITION].put_qps = 5000.0;
    return test_rows;
}

std::vector<row_data> generate_normal_scenario_0()
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
        result[READ_HOTSPOT_DATA].push_back(counters[i][READ_HOTSPOT_DATA]->get()->get_value());
        result[WRITE_HOTSPOT_DATA].push_back(counters[i][WRITE_HOTSPOT_DATA]->get()->get_value());
    }
    return result;
}

TEST(hotspot_partition_calculator, hotspot_partition_policy)
{
    hotspot_partition_calculator test_hotspot_calculator("TEST", 8);
    {
        // Insert normal scenario data to test
        test_hotspot_calculator.data_aggregate(std::move(generate_normal_scenario_0()));
        test_hotspot_calculator.data_analyse();
        std::vector<std::vector<double>> result =
            get_calculator_result(test_hotspot_calculator._hot_points);
        std::vector<double> read_expect_vector{0, 0, 0, 0, 0, 0, 0, 0};
        std::vector<double> write_expect_vector{0, 0, 0, 0, 0, 0, 0, 0};
        ASSERT_EQ(result[READ_HOTSPOT_DATA], read_expect_vector);
        ASSERT_EQ(result[WRITE_HOTSPOT_DATA], write_expect_vector);
    }

    {
        // Insert hot scenario 0 data to test
        test_hotspot_calculator.data_aggregate(std::move(generate_hot_scenario_0()));
        test_hotspot_calculator.data_analyse();
        std::vector<std::vector<double>> result =
            get_calculator_result(test_hotspot_calculator._hot_points);
        std::vector<double> read_expect_vector{0, 0, 0, 0, 0, 0, 0, 4};
        std::vector<double> write_expect_vector{4, 0, 0, 0, 0, 0, 0, 0};
        ASSERT_EQ(result[READ_HOTSPOT_DATA], read_expect_vector);
        ASSERT_EQ(result[WRITE_HOTSPOT_DATA], write_expect_vector);
    }

    {
        // Insert hot scenario 0 data to test again
        test_hotspot_calculator.data_aggregate(std::move(generate_hot_scenario_0()));
        test_hotspot_calculator.data_analyse();
        std::vector<std::vector<double>> result =
            get_calculator_result(test_hotspot_calculator._hot_points);
        std::vector<double> read_expect_vector{0, 0, 0, 0, 0, 0, 0, 4};
        std::vector<double> write_expect_vector{4, 0, 0, 0, 0, 0, 0, 0};
        ASSERT_EQ(result[READ_HOTSPOT_DATA], read_expect_vector);
        ASSERT_EQ(result[WRITE_HOTSPOT_DATA], write_expect_vector);
    }

    {
        // Insert hot scenario 1 data to test
        test_hotspot_calculator.data_aggregate(std::move(generate_hot_scenario_1()));
        test_hotspot_calculator.data_analyse();
        std::vector<std::vector<double>> result =
            get_calculator_result(test_hotspot_calculator._hot_points);
        std::vector<double> read_expect_vector{0, 0, 0, 4, 0, 0, 0, 0};
        std::vector<double> write_expect_vector{0, 0, 4, 0, 0, 0, 0, 0};
        ASSERT_EQ(result[READ_HOTSPOT_DATA], read_expect_vector);
        ASSERT_EQ(result[WRITE_HOTSPOT_DATA], write_expect_vector);
    }
}

} // namespace server
} // namespace pegasus
