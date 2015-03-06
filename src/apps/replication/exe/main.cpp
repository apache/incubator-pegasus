
# include "meta_service_app.h"
# include "replication_service_app.h"
# include "app_client_example1_service.h"
# include <rdsn/tool/simulator.h>
# include <rdsn/tool/nativerun.h>
# include <rdsn/toollet/tracer.h>
# include <rdsn/toollet/profiler.h>
# include <rdsn/toollet/fault_injector.h>

//# include "default_providers.h"

//# include <rdsn/toollet/GlobalPredicate.h>
# include "app_example1.h"
# include "replication_app_factory.h"

# include <thread>

using namespace rdsn::service;

int __cdecl main(int argc, char * argv[])
{
    const char* config = "metaserver.ini";
    if (argc > 1)
    {
        config = argv[1];
    }

    rdsn::replication::replication_app_factory::instance().register_factory(
        "SimpleKV",
        create_simplekv_app
        );

    // register all possible services
    rdsn::service::system::register_service<meta_service_app>("meta");
    rdsn::service::system::register_service<replication_service_app>("replica");
    rdsn::service::system::register_service<app_client_example1_service>("client");

    // register all possible tools and toollets
    rdsn::tools::register_tool<rdsn::tools::nativerun>("nativerun");
    rdsn::tools::register_tool<rdsn::tools::simulator>("simulator");
    rdsn::tools::register_toollet<rdsn::tools::tracer>("tracer");
    rdsn::tools::register_toollet<rdsn::tools::profiler>("profiler");
    rdsn::tools::register_toollet<rdsn::tools::fault_injector>("fault_injector");
    
    // specify what services and tools will run in config file, then run
    system::run(config);
    ::getchar();

    return 0;
}