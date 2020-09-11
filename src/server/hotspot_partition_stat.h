// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "shell/command_helper.h"

namespace pegasus {
namespace server {

enum partition_qps_type
{
    READ_HOTSPOT_DATA = 0,
    WRITE_HOTSPOT_DATA
};

struct hotspot_partition_stat
{
    hotspot_partition_stat(const row_data &row)
    {
        total_qps[READ_HOTSPOT_DATA] = row.get_total_read_qps();
        total_qps[WRITE_HOTSPOT_DATA] = row.get_total_write_qps();
    }
    hotspot_partition_stat() {}
    double total_qps[2];
};

} // namespace server
} // namespace pegasus
