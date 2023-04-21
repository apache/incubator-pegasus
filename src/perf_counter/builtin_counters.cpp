// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "builtin_counters.h"

#include <stdint.h>

#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"

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
    LOG_INFO("memused_virt = {} MB, memused_res = {} MB", memused_virt, memused_res);
}

} // namespace dsn
