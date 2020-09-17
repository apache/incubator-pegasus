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

namespace pegasus {
namespace server {

class hotspot_partition_test : public pegasus_server_test_base
{
public:
    hotspot_partition_test() : calculator("TEST", 8){};
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
}

} // namespace server
} // namespace pegasus
