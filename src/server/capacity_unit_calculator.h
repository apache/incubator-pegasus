// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <fmt/format.h>

namespace pegasus {
namespace server {

class capacity_unit_calculator
{
public:
    capacity_unit_calculator(size_t read_cu_size, size_t write_cu_size, std::string str_gpid);

    void add_read(size_t data_len);
    void add_write(size_t data_len);

private:
    size_t _read_capacity_unit_size;
    size_t _write_capacity_unit_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;
};

} // namespace server
} // namespace pegasus
