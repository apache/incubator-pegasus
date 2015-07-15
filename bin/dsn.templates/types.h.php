<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
# pragma once

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
// # define DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/internal/serialization.h>

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

<?php if ($idl_type == "thrift") { ?>
# include <dsn/thrift_helper.h>
# include "<?=$_PROG->name?>_types.h" 

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

foreach ($_PROG->structs as $s) 
{
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    inline void marshall(::dsn::binary_writer& writer, const ". $s->get_cpp_name() . "& val, uint16_t pos = 0xffff)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));".PHP_EOL;
    echo "        ::apache::thrift::protocol::TBinaryProtocol proto(transport);".PHP_EOL;
    echo "        ::dsn::marshall_rpc_args<".$s->get_cpp_name().">(&proto, val, &".$s->get_cpp_name()."::write);".PHP_EOL;
    echo "    };".PHP_EOL;
    echo PHP_EOL;
    echo "    inline void unmarshall(::dsn::binary_reader& reader, __out_param ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));".PHP_EOL;
    echo "        ::apache::thrift::protocol::TBinaryProtocol proto(transport);".PHP_EOL;
    echo "        ::dsn::unmarshall_rpc_args<".$s->get_cpp_name().">(&proto, val, &".$s->get_cpp_name()."::read);".PHP_EOL;
    echo "    };".PHP_EOL;
    echo PHP_EOL;
}

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>

<?php } else if ($idl_type == "proto") {?>

# include "<?=$_PROG->name?>.pb.h"
# include <dsn/gproto_helper.h>

<?php } else { ?>
# error not supported idl type <?=$idl_type?> 
<?php } ?>

# else // use rDSN's data encoding/decoding

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

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

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>

#endif 
