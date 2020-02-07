// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "shell/commands.h"

namespace pegasus {
namespace server {

//data_store is the minimum storage cell of hotspot_calculator, 
//which stores the data of single partition at single time of collection

struct data_store
{
    data_store(const row_data &row, const std::string name)
        : app_name(name),
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
