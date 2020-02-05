
#include "TableHotspotPolicy.h"

#include <gtest/gtest.h>

namespace pegasus {
namespace server {

TEST(TableHotspotPolicy, Algo1)
{
    std::vector<row_data> app_rows;
    
    Hotpot_calculator app_hotpot_calculator("test",app_rows.size);
    app_hotpot_calculator.aggregate(app_rows.second);

}
}
}