<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
// apps
# include "<?=$file_prefix?>.app.example.h"

// tools
# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>

int main(int argc, char** argv)
{
    // register all possible service apps
    dsn::service::system::register_service<<?=$_PROG->get_cpp_namespace().$_PROG->name?>_server_app>("server");
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

    // register necessary components
#ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION
<?php if ($idl_type == "thrift" ) { ?>
    dsn::tools::register_component_provider<dsn::thrift_binary_message_parser>("thrift");
<?php } ?>
#endif

    // specify what services and tools will run in config file, then run
    dsn::service::system::run(--argc, ++argv, true);
    return 0;
}
