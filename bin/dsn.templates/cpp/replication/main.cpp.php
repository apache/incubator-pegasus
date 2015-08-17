<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
// apps
# include "<?=$file_prefix?>.app.example.h"
# include "<?=$file_prefix?>.server.impl.h"

void module_init()
{
    // register replication application provider
    dsn::replication::register_replica_provider<<?=$_PROG->get_cpp_namespace().$_PROG->name?>_service_impl>("<?=$_PROG->name?>");

    // register all possible services
    dsn::register_app<::dsn::replication::meta_service_app>("meta");
    dsn::register_app<::dsn::replication::replication_service_app>("replica");
    dsn::register_app<<?=$_PROG->get_cpp_namespace().$_PROG->name?>_client_app>("client");
<?php foreach ($_PROG->services as $svc) { ?>
    dsn::register_app<<?=$_PROG->get_cpp_namespace().$svc->name?>_perf_test_client_app>("client.perf.<?=$svc->name?>");
<?php } ?>
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

