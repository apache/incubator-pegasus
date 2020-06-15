// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/table_hotspot_policy.h"

#include "server/hotspot_algo_qps_variance.h"
#include <gtest/gtest.h>

namespace pegasus {
namespace server {

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

    test_rows[0].put_qps = 5000.0;
    test_rows[1].put_qps = 1000.0;
    test_rows[2].put_qps = 1000.0;
    test_rows[3].put_qps = 1000.0;
    test_rows[4].put_qps = 1000.0;
    test_rows[5].put_qps = 1000.0;
    test_rows[6].put_qps = 1000.0;
    test_rows[7].put_qps = 1000.0;

    std::unique_ptr<hotspot_policy> policy(new hotspot_algo_qps_variance());
    hotspot_calculator test_hotspot_calculator("TEST", 8, std::move(policy));
    test_hotspot_calculator.aggregate(test_rows);
    test_hotspot_calculator.start_alg();
    std::vector<double> read_result(8), write_result(8);
    for (int i = 0; i < test_hotspot_calculator._hot_partition_points.size(); i++) {
        read_result[i] =
            test_hotspot_calculator._hot_partition_points[i][READ_HOTSPOT_DATA]->get()->get_value();
        write_result[i] = test_hotspot_calculator._hot_partition_points[i][WRITE_HOTSPOT_DATA]
                              ->get()
                              ->get_value();
    }
    std::vector<double> read_expect_vector{0, 0, 0, 0, 0, 0, 0, 3};
    std::vector<double> write_expect_vector{3, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_EQ(read_expect_vector, read_result);
    ASSERT_EQ(write_expect_vector, write_result);
}

} // namespace server
} // namespace pegasus
