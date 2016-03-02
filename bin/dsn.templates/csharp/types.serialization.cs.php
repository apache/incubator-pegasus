<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
<?php if ($idl_type == "thrift") { ?>
using System;
using System.IO;

namespace <?=$_PROG->get_csharp_namespace()?> 
{
<?php
foreach ($_PROG->enums as $em) 
{
    echo "    // ---------- ". $em->name . " -------------". PHP_EOL;
    echo "    public static partial class ".$_PROG->name."Helper".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        public static void Read(this Stream rs, out ".$em->get_csharp_name()." val)".PHP_EOL;
    echo "        {".PHP_EOL;
    echo "            UInt16 val2;".PHP_EOL;
    echo "            rs.Read(out val2);".PHP_EOL;
    echo "            val = (".$em->get_csharp_name().")val2;".PHP_EOL;
    echo "        }".PHP_EOL;
    echo PHP_EOL;
    echo "        public static void Write(this Stream ws, ".$em->get_csharp_name()." val)".PHP_EOL;
    echo "        {".PHP_EOL;
    echo "            ws.Write((UInt16)val);".PHP_EOL;
    echo "        }".PHP_EOL;
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}

foreach ($_PROG->structs as $s) 
{
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    public static partial class ".$_PROG->name."Helper".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        public static void Read(this Stream rs, out ".$s->get_csharp_name()." val)".PHP_EOL;
    echo "        {".PHP_EOL;
    echo "            val = new ".$s->get_csharp_name()."();" .PHP_EOL;
    foreach ($s->fields as $fld) {
        if (!$fld->is_base_type())
        {
            echo "            val." .$fld->name ." = new ".$_PROG->types[$fld->type_name]->get_csharp_name()."();" .PHP_EOL;
        }
        echo "            rs.Read(out val." .$fld->name .");" .PHP_EOL;
    }
    echo "        }".PHP_EOL;
    echo PHP_EOL;
    echo "        public static void Write(this Stream ws, ".$s->get_csharp_name()." val)".PHP_EOL;
    echo "        {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "            ws.Write(val." .$fld->name .");" .PHP_EOL;
    }
    echo "        }".PHP_EOL;
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}
?>
} 
<?php } else if ($idl_type == "proto") {?>
using System;
using System.IO;
using dsn.dev.csharp;

<?php } else { ?>
# error not supported idl type <?=$idl_type?>
<?php } ?>