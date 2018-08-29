// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <atomic>
#include <unistd.h>
#include <memory>
#include <signal.h>

#include <pegasus/version.h>

#include "reporter/pegasus_counter_reporter.h"
#include "redis_parser.h"

namespace pegasus {
namespace proxy {

class proxy_app : public ::dsn::service_app
{
public:
    explicit proxy_app(const dsn::service_app_info *info) : service_app(info) {}

    ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        if (args.size() < 2) {
            return ::dsn::ERR_INVALID_PARAMETERS;
        }

        proxy_session::factory f = [](proxy_stub *p, dsn::message_ex *m) {
            return std::make_shared<redis_parser>(p, m);
        };
        _proxy = dsn::make_unique<proxy_stub>(
            f, args[1].c_str(), args[2].c_str(), args.size() > 3 ? args[3].c_str() : "");

        pegasus::server::pegasus_counter_reporter::instance().start();

        return ::dsn::ERR_OK;
    }

    ::dsn::error_code stop(bool) final { return ::dsn::ERR_OK; }

private:
    std::unique_ptr<proxy_stub> _proxy;
};
} // namespace proxy
} // namespace pegasus

void register_apps() { ::dsn::service_app::register_factory<::pegasus::proxy::proxy_app>("proxy"); }

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
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "-version") == 0 ||
            strcmp(argv[i], "--version") == 0) {
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

#if defined(__linux__)
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <dsn/version.h>
#include <dsn/git_commit.h>
#define STR_I(var) #var
#define STR(var) STR_I(var)
static char const rcsid[] =
    "$Version: Pegasus Redis Proxy " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
    " " STR(DSN_BUILD_TYPE)
#endif
        ", built with rDSN " DSN_CORE_VERSION " (" DSN_GIT_COMMIT ")"
        ", built by gcc " STR(__GNUC__) "." STR(__GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
            ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                ", built at " __DATE__ " " __TIME__ " $";
const char *pegasus_rproxy_rcsid() { return rcsid; }
#endif
