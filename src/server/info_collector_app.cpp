// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "info_collector_app.h"
#include "reporter/pegasus_counter_reporter.h"

#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication_other_types.h>

#include <iostream>
#include <fstream>
#include <iomanip>

namespace pegasus {
namespace server {

info_collector_app::info_collector_app(const dsn::service_app_info *info)
    : service_app(info), _updater_started(false)
{
}

info_collector_app::~info_collector_app() {}

::dsn::error_code info_collector_app::start(const std::vector<std::string> &args)
{
    pegasus_counter_reporter::instance().start();
    _updater_started = true;

    _collector.start();
    _detector.start();
    return ::dsn::ERR_OK;
}

::dsn::error_code info_collector_app::stop(bool cleanup)
{
    if (_updater_started) {
        pegasus_counter_reporter::instance().stop();
    }

    _collector.stop();
    _detector.stop();
    return ::dsn::ERR_OK;
}
}
} // namespace
