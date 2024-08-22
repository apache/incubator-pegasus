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

#include "utils/builtin_metrics.h"

#include <string_view>
#include <stdint.h>
#include <functional>

#include "utils/autoref_ptr.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"

METRIC_DEFINE_gauge_int64(server,
                          virtual_mem_usage_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The total amount of virtual memory usage in MB");

METRIC_DEFINE_gauge_int64(server,
                          resident_mem_usage_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The total amount of physical memory usage in MB");

DSN_DEFINE_uint64(metrics,
                  builtin_metrics_update_interval_ms,
                  10 * 1000,
                  "The interval (milliseconds) at which builtin metrics are updated.");

namespace dsn {

builtin_metrics::builtin_metrics()
    : METRIC_VAR_INIT_server(virtual_mem_usage_mb), METRIC_VAR_INIT_server(resident_mem_usage_mb)
{
}

builtin_metrics::~builtin_metrics()
{
    CHECK(!_timer, "timer should have been destroyed by stop()");
}

void builtin_metrics::on_close() {}

void builtin_metrics::start()
{
    CHECK(!_timer, "timer should not have been initialized before start()");

    _timer.reset(new metric_timer(FLAGS_builtin_metrics_update_interval_ms,
                                  std::bind(&builtin_metrics::update, this),
                                  std::bind(&builtin_metrics::on_close, this)));
}

void builtin_metrics::stop()
{
    CHECK(_timer, "timer should have been initialized before stop()");

    // Close the timer synchronously.
    _timer->close();
    _timer->wait();

    // Reset the timer to mark that it has been stopped, now it could be started.
    _timer.reset();
}

void builtin_metrics::update()
{
    double vm_usage;
    double resident_set;
    utils::process_mem_usage(vm_usage, resident_set);

    auto virt_mb = static_cast<uint64_t>(vm_usage) >> 10;
    auto res_mb = static_cast<uint64_t>(resident_set) >> 10;
    METRIC_VAR_SET(virtual_mem_usage_mb, virt_mb);
    METRIC_VAR_SET(resident_mem_usage_mb, res_mb);
    LOG_INFO("virt = {} MB, res = {} MB", virt_mb, res_mb);
}

} // namespace dsn
