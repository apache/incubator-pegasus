// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/utils.h>
#include <dsn/c/api_utilities.h>
#include "builtin_counters.h"

namespace dsn {

builtin_counters::builtin_counters()
{
    _memused_virt.init_global_counter("replica",
                                      "server",
                                      "memused.virt(MB)",
                                      COUNTER_TYPE_NUMBER,
                                      "virtual memory usages in MB");
    _memused_res.init_global_counter("replica",
                                     "server",
                                     "memused.res(MB)",
                                     COUNTER_TYPE_NUMBER,
                                     "physically memory usages in MB");
}

builtin_counters::~builtin_counters() {}

void builtin_counters::update_counters()
{
    double vm_usage;
    double resident_set;
    utils::process_mem_usage(vm_usage, resident_set);
    uint64_t memused_virt = (uint64_t)vm_usage / 1024;
    uint64_t memused_res = (uint64_t)resident_set / 1024;
    _memused_virt->set(memused_virt);
    _memused_res->set(memused_res);
    ddebug("memused_virt = %" PRIu64 " MB, memused_res = %" PRIu64 "MB", memused_virt, memused_res);
}
}
