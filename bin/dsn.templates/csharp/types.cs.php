<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
using System;
using System.IO;
using dsn.dev.csharp;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
<?php
foreach ($_PROG->enums as $em) 
{
    echo "    // ---------- ". $em->name . " -------------". PHP_EOL;
    echo "    public enum ". $em->get_csharp_name() .PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($em->values as $k => $v) {
        echo "        ". $k . " = " .$v ."," .PHP_EOL;
    }
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}

foreach ($_PROG->structs as $s) 
{
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    public class ". $s->get_csharp_name() .PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        public ". $fld->get_csharp_type() . " " .$fld->name .";" .PHP_EOL;
    }
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}
?>
} 
