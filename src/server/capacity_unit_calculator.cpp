// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "capacity_unit_calculator.h"

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(size_t read_cu_size,
                                                   size_t write_cu_size,
                                                   std::string str_gpid)
    : _read_capacity_unit_size(read_cu_size), _write_capacity_unit_size(write_cu_size)
{
    std::string name;

    name = fmt::format("recent.read.cu@{}", str_gpid);
    _pfc_recent_read_cu.init_app_counter("app.pegasus",
                                         name.c_str(),
                                         COUNTER_TYPE_VOLATILE_NUMBER,
                                         "statistic the recent read capacity units");
    name = fmt::format("recent.write.cu@{}", str_gpid);
    _pfc_recent_write_cu.init_app_counter("app.pegasus",
                                          name.c_str(),
                                          COUNTER_TYPE_VOLATILE_NUMBER,
                                          "statistic the recent write capacity units");
}

void capacity_unit_calculator::add_read(size_t data_len)
{
    if (data_len > 0) {
        size_t read_cu = (data_len + _read_capacity_unit_size - 1) / _read_capacity_unit_size;
        _pfc_recent_read_cu->add(read_cu);
    }
}

void capacity_unit_calculator::add_write(size_t data_len)
{
    size_t write_cu =
        data_len == 0 ? (data_len + _read_capacity_unit_size - 1) / _read_capacity_unit_size : 1;
    _pfc_recent_write_cu->add(write_cu);
}

} // namespace server
} // namespace pegasus
