
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