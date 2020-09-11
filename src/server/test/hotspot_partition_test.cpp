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

#include "message_utils.h"
#include "base/pegasus_rpc_types.h"

namespace pegasus {
namespace server {

TEST(hotspot_partition_calculator, hotspot_partition_policy)
{
    // TODO: refactor the unit test
    std::vector<row_data> test_rows(8);
    test_rows[0].get_qps = 1000.0;
    test_rows[1].get_qps = 1000.0;
    test_rows[2].get_qps = 1000.0;
    test_rows[3].get_qps = 1000.0;
    test_rows[4].get_qps = 1000.0;
    test_rows[5].get_qps = 1000.0;
    test_rows[6].get_qps = 1000.0;
    test_rows[7].get_qps = 5000.0;
    hotspot_partition_calculator test_hotspot_calculator("TEST", 8);
    test_hotspot_calculator.data_aggregate(test_rows);
    test_hotspot_calculator.data_analyse();
    std::vector<double> result(8);
    for (int i = 0; i < test_hotspot_calculator._hot_points.size(); i++) {
        result[i] = test_hotspot_calculator._hot_points[i]->get_value();
    }
    std::vector<double> expect_vector{0, 0, 0, 0, 0, 0, 0, 3};
    ASSERT_EQ(expect_vector, result);
}

TEST(hotspot_partition_calculator, server_hotkey_detect_send)
{
    RPC_MOCKING(detect_hotkey_rpc)
    {

    }
}

} // namespace server
} // namespace pegasus
