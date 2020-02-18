// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/table_hotspot_policy.h"

#include <gtest/gtest.h>

namespace pegasus {
namespace server {
/*
TEST(table_hotspot_policy, hotspot_algo_qps_skew)
{
    std::vector<row_data> test_rows(2);
    test_rows[0].get_qps = 1234.0;
    test_rows[1].get_qps = 4321.0;
    hotspot_calculator test_hotspot_calculator("TEST", 2);
    test_hotspot_calculator.aggregate(test_rows);
    test_hotspot_calculator.start_alg();
    std::vector<double> result(2);
    for (int i = 0; i < test_hotspot_calculator._points.size(); i++) {
        result[i] = test_hotspot_calculator._points[i]->get_value();
    }
    std::vector<double> expect_vector{1, 3};
    ASSERT_EQ(expect_vector, result);
}
*/

TEST(table_hotspot_policy, hotspot_algo_qps_variance)
{
    std::vector<row_data> test_rows(8);
    test_rows[0].get_qps = 1000.0;
    test_rows[1].get_qps = 1000.0;
    test_rows[2].get_qps = 1000.0;
    test_rows[3].get_qps = 1000.0;
    test_rows[4].get_qps = 1000.0;
    test_rows[5].get_qps = 1000.0;
    test_rows[6].get_qps = 1000.0;
    test_rows[7].get_qps = 5000.0;
    hotspot_calculator test_hotspot_calculator("TEST", 2);
    test_hotspot_calculator.aggregate(test_rows);
    test_hotspot_calculator.start_alg();
    std::vector<double> result(8);
    for (int i = 0; i < test_hotspot_calculator._points.size(); i++) {
        result[i] = test_hotspot_calculator._points[i]->get_value();
    }
    std::vector<double> expect_vector{0, 0, 0, 0, 0, 0, 0, 6};
    ASSERT_EQ(expect_vector, result);
}

} // namespace server
} // namespace pegasus
