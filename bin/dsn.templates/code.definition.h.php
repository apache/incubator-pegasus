<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include <dsn/service_api.h>
# include "<?=$_PROG->name?>.types.h"

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

foreach ($_PROG->services as $svc)
{
    echo "\t// define RPC task code for service '". $svc->name ."'". PHP_EOL;
    foreach ($svc->functions as $f)
    {
        echo "\tDEFINE_TASK_CODE_RPC(". $f->get_rpc_code() 
            . ", ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)".PHP_EOL;
    }    
}
echo "\t// test timer task code".PHP_EOL; 
echo "\tDEFINE_TASK_CODE(". $_PROG->get_test_task_code() 
        . ", ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)".PHP_EOL;

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>
