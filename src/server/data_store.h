#pragma once

#include "shell/commands.h"

namespace pegasus {
namespace server {

struct data_store
{
    data_store(const row_data &row, const std::string app_name_out)
        : app_name(app_name_out),
          total_qps(row.get_total_qps()),
          total_cu(row.get_total_cu()),
          partition_name(row.row_name){};
    data_store() {}
    std::string app_name;
    double total_qps;
    double total_cu;
    std::string partition_name;
};
}
}
