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

#pragma once

#include <memory>

#include "utils/metrics.h"
#include "utils/ports.h"

namespace dsn {

class builtin_metrics
{
public:
    builtin_metrics();
    ~builtin_metrics();

    void start();
    void stop();

private:
    void on_close();
    void update();

    METRIC_VAR_DECLARE_gauge_int64(virtual_mem_usage_mb);
    METRIC_VAR_DECLARE_gauge_int64(resident_mem_usage_mb);

    std::unique_ptr<metric_timer> _timer;

    DISALLOW_COPY_AND_ASSIGN(builtin_metrics);
};

} // namespace dsn
