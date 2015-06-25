// apps
# include "cli_app.h"

// tools
# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>

int main(int argc, char** argv)
{
    // register all possible service apps
    dsn::service::system::register_service<::dsn::service::cli>("cli");

    // register all possible tools and toollets
    dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
    dsn::tools::register_tool<dsn::tools::simulator>("simulator");
    dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
    dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
    dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
        
    // register necessary components
#ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION
    dsn::tools::register_component_provider<dsn::thrift_binary_message_parser>("thrift");
#endif

    // specify what services and tools will run in config file, then run
    dsn::service::system::run("config.ini", true);
    return 0;
}
