/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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

# include "echo_service.h"
# include <rdsn/tool/simulator.h>
# include <rdsn/tool/nativerun.h>
# include <rdsn/toollet/tracer.h>
# include <rdsn/toollet/profiler.h>
# include <rdsn/toollet/fault_injector.h>

using namespace rdsn::service;

int __cdecl main(int argc, char * argv[])
{
    // register all possible services
    rdsn::service::system::register_service<echo_client_app>("echo_client_app");
    rdsn::service::system::register_service<echo_server_app>("echo_server_app");

    // register all possible tools and toollets
    rdsn::tools::register_tool<rdsn::tools::nativerun>("nativerun");
    rdsn::tools::register_tool<rdsn::tools::simulator>("simulator");    
    rdsn::tools::register_toollet<rdsn::tools::tracer>("tracer");
    rdsn::tools::register_toollet<rdsn::tools::profiler>("profiler");
    rdsn::tools::register_toollet<rdsn::tools::fault_injector>("fault_injector");
        
    // specify what services and tools will run in config file, then run
    system::run("echo.ini");
    ::getchar();

    return 0;
}
