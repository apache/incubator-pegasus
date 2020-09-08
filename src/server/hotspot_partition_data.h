// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "shell/commands.h"

namespace pegasus {
namespace server {

struct hotspot_partition_data
{
    hotspot_partition_data(const row_data &row) : total_qps(row.get_total_qps()){};
    hotspot_partition_data() {}
    double total_qps;
};

} // namespace server
} // namespace pegasus
