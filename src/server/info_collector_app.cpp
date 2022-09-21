/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "info_collector_app.h"
#include "reporter/pegasus_counter_reporter.h"

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"
#include "common/replication_other_types.h"

#include <iostream>
#include <fstream>
#include <iomanip>

namespace pegasus {
namespace server {

class collector_http_service : public ::dsn::http_server_base
{
};

info_collector_app::info_collector_app(const dsn::service_app_info *info)
    : service_app(info), _updater_started(false)
{
    register_http_service(new collector_http_service());
    dsn::start_http_server();
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
