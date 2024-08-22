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

#include <unistd.h>
#include <memory>

#include "meta/meta_service_app.h"
#include "pegasus_service_app.h"
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "server/server_utils.h"
#include "utils/fmt_logging.h"

namespace dsn {
class command_deregister;
} // namespace dsn

int main(int argc, char **argv)
{
    static const char server_name[] = "Meta server";
    if (help(argc, argv, server_name)) {
        dsn_exit(0);
    }
    LOG_INFO("{} starting, pid({}), version({})", server_name, getpid(), pegasus_server_rcsid());

    // Register meta service.
    dsn::service::meta_service_app::register_components();
    dsn::service_app::register_factory<pegasus::server::pegasus_meta_service_app>("meta");

    auto server_info_cmd = register_server_info_cmd();

    dsn_run(argc, argv, true);

    return 0;
}
