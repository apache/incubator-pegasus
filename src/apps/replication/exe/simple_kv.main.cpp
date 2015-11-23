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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

// apps
# include "simple_kv.app.example.h"
# include "simple_kv.server.impl.h"

// framework specific tools
# include <dsn/dist/replication/replication.global_check.h>

void module_init()
{
    // register replication application provider
    dsn::replication::register_replica_provider< ::dsn::replication::application::simple_kv_service_impl>("simple_kv");

    // register all possible services
    dsn::register_app< ::dsn::service::meta_service_app>("meta");
    dsn::register_app< ::dsn::replication::replication_service_app>("replica");
    dsn::register_app< ::dsn::replication::application::simple_kv_client_app>("client");
    dsn::register_app< ::dsn::replication::application::simple_kv_perf_test_client_app>("client.perf.test");

    dsn::replication::install_checkers();
}


# ifndef DSN_RUN_USE_SVCHOST

int main(int argc, char** argv)
{
    module_init();

    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, true);
    return 0;
}

# else

# include <dsn/internal/module_int.cpp.h>

# endif
