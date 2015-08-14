<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
using System;
using System.IO;
using dsn.dev.csharp;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
    public static partial class <?=$_PROG->name?>Helper
    {
<?php
foreach ($_PROG->services as $svc)
{
    echo "        // define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)". PHP_EOL;    
    echo "        // define RPC task code for service '". $svc->name ."'". PHP_EOL;
    foreach ($svc->functions as $f)
    {
        echo "        public static TaskCode ". $f->get_rpc_code(). ";".PHP_EOL;
    }    
    echo "        public static TaskCode ". $_PROG->get_test_task_code(). ";".PHP_EOL;
}    
?>    

        public static void InitCodes()
        {
<?php
foreach ($_PROG->services as $svc)
{
    foreach ($svc->functions as $f)
    {
        echo "            ". $f->get_rpc_code(). " = new TaskCode(\"".$f->get_rpc_code()."\", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);".PHP_EOL;
    }    
}    
    echo "            ". $_PROG->get_test_task_code(). " = new TaskCode(\"".$_PROG->get_test_task_code()."\", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);".PHP_EOL;
?>
        }
    }    
}

