<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
// apps
# include "<?=$file_prefix?>.app.example.h"
# include "<?=$file_prefix?>.server.impl.h"

// tools
# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>

int main(int argc, char** argv)
{
    // register replication application provider
    dsn::replication::register_replica_provider<<?=$_PROG->get_cpp_namespace().$_PROG->name?>_service_impl>("<?=$_PROG->name?>");

    // register all possible services
    dsn::service::system::register_service<::dsn::replication::meta_service_app>("meta");
    dsn::service::system::register_service<::dsn::replication::replication_service_app>("replica");
    dsn::service::system::register_service<<?=$_PROG->get_cpp_namespace().$_PROG->name?>_client_app>("client");
<?php foreach ($_PROG->services as $svc) { ?>
    dsn::service::system::register_service<<?=$_PROG->get_cpp_namespace().$svc->name?>_perf_test_client_app>("client.<?=$svc->name?>.perf.test");
<?php } ?>

    // register all possible tools and toollets
    dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
    dsn::tools::register_tool<dsn::tools::simulator>("simulator");
    dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
    dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
    dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");

    // specify what services and tools will run in config file, then run
    dsn::service::system::run(--argc, ++argv, true);
    return 0;
}
