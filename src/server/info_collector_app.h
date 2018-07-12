// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/service_app.h>
#include "info_collector.h"
#include "available_detector.h"

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_PEGASUS_COLLECTOR_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

class info_collector_app : public ::dsn::service_app
{
public:
    info_collector_app(const dsn::service_app_info *info);
    virtual ~info_collector_app(void);

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override;
    virtual ::dsn::error_code stop(bool cleanup = false) override;

private:
    info_collector _collector;
    available_detector _detector;
    bool _updater_started;
};
}
} // namespace
