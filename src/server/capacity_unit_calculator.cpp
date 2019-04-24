// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "capacity_unit_calculator.h"

#include <dsn/utility/config_api.h>
#include <fmt/format.h>

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(const dsn::gpid &pid)
{
    _read_capacity_unit_size =
        dsn_config_get_value_int64("pegasus.server",
                                   "perf_counter_read_capacity_unit_size",
                                   1024,
                                   "capacity unit size of read requests, default 1KB");
    _write_capacity_unit_size =
        dsn_config_get_value_int64("pegasus.server",
                                   "perf_counter_write_capacity_unit_size",
                                   1024,
                                   "capacity unit size of write requests, default 1KB");

    char str_gpid[128], buf[256];
    snprintf(str_gpid, 128, "%d.%d", pid.get_app_id(), pid.get_partition_index());

    snprintf(buf, 255, "recent.read.cu@%s", str_gpid);
    _pfc_recent_read_cu.init_app_counter("app.pegasus",
                                         buf,
                                         COUNTER_TYPE_VOLATILE_NUMBER,
                                         "statistic the recent read capacity units");
    snprintf(buf, 255, "recent.write.cu@%s", str_gpid);
    _pfc_recent_write_cu.init_app_counter("app.pegasus",
                                          buf,
                                          COUNTER_TYPE_VOLATILE_NUMBER,
                                          "statistic the recent write capacity units");
}

void capacity_unit_calculator::add_read(int64_t data_len)
{
    if (data_len > 0) {
        int64_t read_cu = (data_len + _read_capacity_unit_size - 1) / _read_capacity_unit_size;
        _pfc_recent_read_cu->add(read_cu);
    }
}

void capacity_unit_calculator::add_write(int64_t data_len)
{
    int64_t write_cu =
        data_len > 0 ? (data_len + _write_capacity_unit_size - 1) / _write_capacity_unit_size : 1;
    _pfc_recent_write_cu->add(write_cu);
}

} // namespace server
} // namespace pegasus
