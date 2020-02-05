#include "TableHotspotPolicy.h"
#include <gtest/gtest.h>

namespace pegasus {
namespace server {

TEST(TableHotspotPolicy, Algo1)
{
    std::vector<row_data> test_rows;
    row_data test_row;
    test_row.get_qps = 1234;
    test_rows.push_back(test_row);
    TableHotspotPolicy app_hotpot_calculator
}
}
}