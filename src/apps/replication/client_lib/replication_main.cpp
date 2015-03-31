# include "replication_examples.h"

# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>
// more tools and toollets here

using namespace  ::dsn::replication;

int replication_main(int argc, char** argv)
{
	// register all possible service apps
	dsn::service::system::register_service<replication_server_app>("replication_server");
	dsn::service::system::register_service<replication_client_app>("replication_client");

	// register all possible tools and toollets
	dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
	dsn::tools::register_tool<dsn::tools::simulator>("simulator");
	dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
	dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
	dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");

	// specify what services and tools will run in config file, then run
	dsn::service::system::run("replication.ini");
	::getchar();

	return 0;
}
