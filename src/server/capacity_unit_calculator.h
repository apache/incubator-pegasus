// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/tool-api/gpid.h>

namespace pegasus {
namespace server {

class capacity_unit_calculator
{
public:
    capacity_unit_calculator(const dsn::gpid &pid);

    void add_read(int64_t data_len);
    void add_write(int64_t data_len);

private:
    int64_t _read_capacity_unit_size;
    int64_t _write_capacity_unit_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;
};

} // namespace server
} // namespace pegasus
