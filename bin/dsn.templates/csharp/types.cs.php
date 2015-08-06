<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>

<?php
echo "namespace ".$_PROG->get_csharp_namespace(). "{".PHP_EOL;

foreach ($_PROG->enums as $em) 
{
    echo "    // ---------- ". $em->name . " -------------". PHP_EOL;
    echo "    enum ". $em->get_cpp_name() .PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($em->values as $k => $v) {
        echo "        ". $k . " = " .$v ."," .PHP_EOL;
    }
    echo "    };".PHP_EOL;
    echo PHP_EOL;
    echo "    DEFINE_POD_SERIALIZATION(". $em->get_cpp_name() .");".PHP_EOL;
    echo PHP_EOL;
}

foreach ($_PROG->structs as $s) 
{
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    struct ". $s->get_cpp_name() .PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        ". $fld->get_cpp_type() . " " .$fld->name .";" .PHP_EOL;
    }
    echo "    };".PHP_EOL;
    echo PHP_EOL;
    echo "    inline void marshall(::dsn::binary_writer& writer, const ". $s->get_cpp_name() . "& val, uint16_t pos = 0xffff)".PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        marshall(writer, val." .$fld->name .", pos);" .PHP_EOL;
    }
    echo "    };".PHP_EOL;
    echo PHP_EOL;
    echo "    inline void unmarshall(::dsn::binary_reader& reader, __out_param ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        unmarshall(reader, val." .$fld->name .");" .PHP_EOL;
    }
    echo "    };".PHP_EOL;
    echo PHP_EOL;
}

?>
} 
