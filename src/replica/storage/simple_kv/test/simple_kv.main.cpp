/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "checker.h"
#include "client.h"
#include "http/http_server.h"
#include "injector.h"
#include "meta/meta_service_app.h"
#include "replica/replication_service_app.h"
#include "replica/storage/simple_kv/test/common.h"
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "runtime/tool_api.h"
#include "simple_kv.server.impl.h"
#include "utils/fmt_logging.h"

void dsn_app_registration_simple_kv()
{
    dsn::FLAGS_enable_http_server = false;
    dsn::replication::test::simple_kv_service_impl::register_service();

    dsn::service::meta_service_app::register_all();
    dsn::replication::replication_service_app::register_all();

    dsn::service_app::register_factory<dsn::replication::test::simple_kv_client_app>("client");
    dsn::tools::register_toollet<dsn::replication::test::test_injector>("test_injector");
    dsn::replication::test::install_checkers();
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        std::cerr << "USGAE: " << argv[0] << " <config-file> <case-input>" << std::endl;
        std::cerr << " e.g.: " << argv[0] << " case-000.ini case-000.act" << std::endl;
        return -1;
    }

    dsn::replication::test::g_case_input = argv[2];

    dsn_app_registration_simple_kv();

    // specify what services and tools will run in config file, then run
    dsn_run(argc - 1, argv, false);

    while (!dsn::replication::test::g_done) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    LOG_INFO("=== exiting ...");

    dsn::replication::test::test_checker::instance().exit();

    if (dsn::replication::test::g_fail) {
#ifndef ENABLE_GCOV
        dsn_exit(-1);
#endif
        return -1;
    }

#ifndef ENABLE_GCOV
    dsn_exit(0);
#endif
    return 0;
}
