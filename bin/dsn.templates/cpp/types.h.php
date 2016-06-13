<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];

$dsf_object = array();
foreach ($_PROG->structs as $s)
{
    array_push($dsf_object, $s->name);
}
foreach ($_PROG->enums as $e)
{
    array_push($dsf_object, $e->name);
}
$idl_name = "";
if ($idl_type == "thrift")
{
    $idl_name = "THRIFT";
} else if ($idl_type == "proto")
{
    $idl_name = "PROTOBUF";
}
?>
# pragma once
# include <dsn/service_api_cpp.h>
# include <dsn/cpp/serialization.h>

<?php if ($idl_type == "thrift") { ?>

# include "thrift/<?=$_PROG->name?>_types.h" 
<?php foreach ($_PROG->services as $svc) { ?>
# include "thrift/<?=$svc->name?>.h"
<?php } ?>

<?php } else if ($idl_type == "proto") {?>

# include "<?=$_PROG->name?>.pb.h"

<?php } ?>

<?=$_PROG->get_cpp_namespace_begin()?>

<?php
foreach ($_PROG->structs as $s){
    Echo "    GENERATED_TYPE_SERIALIZATION(".$s->name.", ".$idl_name.")".PHP_EOL;
} ?>

<?=$_PROG->get_cpp_namespace_end()?>
