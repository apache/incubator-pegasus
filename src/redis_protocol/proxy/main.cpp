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

#include <pegasus/version.h>
#include <s2/third_party/absl/base/port.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

#include "proxy_layer.h"
#include "redis_parser.h"
#include "reporter/pegasus_counter_reporter.h"
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "utils/error_code.h"
#include "utils/strings.h"

namespace pegasus {
class message_ex;
} // namespace pegasus

namespace pegasus {
namespace proxy {

class proxy_app : public service_app
{
public:
    explicit proxy_app(const service_app_info *info) : service_app(info) {}

    error_code start(const std::vector<std::string> &args) override
    {
        if (args.size() < 2) {
            return ERR_INVALID_PARAMETERS;
        }

        proxy_session::factory f = [](proxy_stub *p, message_ex *m) {
            return std::make_shared<redis_parser>(p, m);
        };
        _proxy = std::make_unique<proxy_stub>(
            f, args[1].c_str(), args[2].c_str(), args.size() > 3 ? args[3].c_str() : "");

        server::pegasus_counter_reporter::instance().start();

        return ERR_OK;
    }

    error_code stop(bool) final { return ERR_OK; }

private:
    std::unique_ptr<proxy_stub> _proxy;
};
} // namespace proxy
} // namespace pegasus

void register_apps() { pegasus::service_app::register_factory<pegasus::proxy::proxy_app>("proxy"); }

volatile int exit_flags(0);
void signal_handler(int signal_id)
{
    if (signal_id == SIGTERM) {
        dsn_exit(0);
    }
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        if (pegasus::utils::equals(argv[i], "-v") || pegasus::utils::equals(argv[i], "-version") ||
            pegasus::utils::equals(argv[i], "--version")) {
            printf("Pegasus Redis Proxy %s\n", PEGASUS_VERSION);
            return 0;
        }
    }

    register_apps();
    signal(SIGTERM, signal_handler);

    if (argc == 1) {
        dsn_run_config("config.ini", false);
    } else {
        dsn_run(argc, argv, false);
    }

    while (exit_flags == 0) {
        pause();
    }
    return 0;
}
