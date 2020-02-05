#include "server/tableHotspotPolicy.h"
#include <gtest/gtest.h>

namespace pegasus {
namespace server {

TEST(tableHotspotPolicy, Algo1)
{
    std::vector<row_data> test_rows;
    row_data test_row1,test_row2;
    test_row1.get_qps = 1234.0;
    test_rows.push_back(test_row1);
    test_row2.get_qps = 4321.0;
    test_rows.push_back(test_row2);
    Hotpot_calculator test_hotpot_calculator("TEST",1);
    test_hotpot_calculator.aggregate(test_rows);
    test_hotpot_calculator.start_alg();
    std::vector<double> result ;
    test_hotpot_calculator.get_hotpot_point_value(result);
    std::vector<double> expect_vector{1234.0,4321.0};
    ASSERT_EQ((expect_vector==result),1);
}
}
}