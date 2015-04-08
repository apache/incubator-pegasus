/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

# include "meta_service_app.h"
# include "replication_service_app.h"
# include "app_client_example1_service.h"
# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>

# include "app_example1.h"
# include "replication_app_factory.h"

# include <thread>
# include <boost/filesystem.hpp>

using namespace dsn::service;
char s_temp[512];

int main(int argc, char * argv[])
{
    const char* config = "metaserver.ini";
    if (argc > 1)
    {
        config = argv[1];
    }

    if (argc > 2)
    {
        std::string cdir = argv[2];
        if (!boost::filesystem::exists(cdir))
        {
            boost::filesystem::create_directory(cdir);
        }

        auto s = boost::filesystem::current_path().string();
        boost::filesystem::current_path(cdir);
        
        sprintf(s_temp, "%s\\%s", s.c_str(), config);
        config = s_temp;
    }

    dsn::replication::replication_app_factory::instance().register_factory(
        "simple_kv",
        create_simplekv_app
        );

    // register all possible services
    dsn::service::system::register_service<meta_service_app>("meta");
    dsn::service::system::register_service<replication_service_app>("replica");
    dsn::service::system::register_service<app_client_example1_service>("client");

    // register all possible tools and toollets
    dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
    dsn::tools::register_tool<dsn::tools::simulator>("simulator");
    dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
    dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
    dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
    
    // specify what services and tools will run in config file, then run
    system::run(config);
    ::getchar();

    return 0;
}
