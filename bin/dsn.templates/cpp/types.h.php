<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
$idl_type = $argv[4];
?>
# pragma once
# include <dsn/service_api_cpp.h>

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
// # define DSN_NOT_USE_DEFAULT_SERIALIZATION

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

<?php if ($idl_type == "thrift") { ?>
# include <dsn/thrift_helper.h>
# include "<?=$_PROG->name?>_types.h" 

<?php 
echo "namespace dsn {".PHP_EOL;
foreach ($_PROG->structs as $s) 
{
    $full_name = $_PROG->get_cpp_namespace().$s->get_cpp_name();
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    template<>". PHP_EOL;
    echo "    inline uint32_t marshall_base< ". $full_name . ">(::apache::thrift::protocol::TProtocol* oprot, const ". $full_name . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        uint32_t xfer = 0;".PHP_EOL;
    echo "        oprot->incrementInputRecursionDepth();".PHP_EOL;
    echo "        xfer += oprot->writeStructBegin(\"rpc_message\");".PHP_EOL;
    echo "        xfer += oprot->writeFieldBegin(\"msg\", ::apache::thrift::protocol::T_STRUCT, 1);".PHP_EOL;
    echo PHP_EOL;
    echo "        xfer += val.write(oprot);".PHP_EOL;
    echo PHP_EOL;
    echo "        xfer += oprot->writeFieldEnd();".PHP_EOL;
    echo PHP_EOL;
    echo "        xfer += oprot->writeFieldStop();".PHP_EOL;
    echo "        xfer += oprot->writeStructEnd();".PHP_EOL;
    echo "        oprot->decrementInputRecursionDepth();".PHP_EOL;
    echo "        return xfer;".PHP_EOL;
    echo "    }".PHP_EOL;
    echo PHP_EOL;
    echo "    template<>". PHP_EOL;
    echo "    inline uint32_t unmarshall_base< ". $full_name . ">(::apache::thrift::protocol::TProtocol* iprot, /*out*/ ". $full_name . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        uint32_t xfer = 0;".PHP_EOL;
    echo "        std::string fname;".PHP_EOL;
    echo "        ::apache::thrift::protocol::TType ftype;".PHP_EOL;
    echo "        int16_t fid;".PHP_EOL;
    echo "        xfer += iprot->readStructBegin(fname);".PHP_EOL;
    echo "        using ::apache::thrift::protocol::TProtocolException;".PHP_EOL;
    echo "        while (true)".PHP_EOL;
    echo "        {".PHP_EOL;
    echo "            xfer += iprot->readFieldBegin(fname, ftype, fid);".PHP_EOL;
    echo "            if (ftype == ::apache::thrift::protocol::T_STOP) {".PHP_EOL;
    echo "                break;".PHP_EOL;
    echo "            }".PHP_EOL;
    echo "            switch (fid)".PHP_EOL;
    echo "            {".PHP_EOL;
    echo "            case 1:".PHP_EOL;
    echo "                if (ftype == ::apache::thrift::protocol::T_STRUCT) {".PHP_EOL;
    echo "                    xfer += val.read(iprot);".PHP_EOL;
    echo "                }".PHP_EOL;
    echo "                else {".PHP_EOL;
    echo "                    xfer += iprot->skip(ftype);".PHP_EOL;
    echo "                }".PHP_EOL;
    echo "                break;".PHP_EOL;
    echo "            default:".PHP_EOL;
    echo "                xfer += iprot->skip(ftype);".PHP_EOL;
    echo "                break;".PHP_EOL;
    echo "            }".PHP_EOL;
    echo "            xfer += iprot->readFieldEnd();".PHP_EOL;
    echo "        }".PHP_EOL;
    echo "        xfer += iprot->readStructEnd();".PHP_EOL;
    echo "        iprot->readMessageEnd();".PHP_EOL;
    echo "        iprot->getTransport()->readEnd();".PHP_EOL;
    echo "        return xfer;".PHP_EOL;
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}
echo "}".PHP_EOL;
?>

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

foreach ($_PROG->structs as $s) 
{
    echo "    // ---------- ". $s->name . " -------------". PHP_EOL;
    echo "    inline void marshall(::dsn::binary_writer& writer, const ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));".PHP_EOL;
    echo "        ::apache::thrift::protocol::TBinaryProtocol proto(transport);".PHP_EOL;
    echo "        ::dsn::marshall_base<".$s->get_cpp_name().">(&proto, val);".PHP_EOL;
    echo "    }".PHP_EOL;
    echo PHP_EOL;
    echo "    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    echo "        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));".PHP_EOL;
    echo "        ::apache::thrift::protocol::TBinaryProtocol proto(transport);".PHP_EOL;
    echo "        ::dsn::unmarshall_base<".$s->get_cpp_name().">(&proto, val);".PHP_EOL;
    echo "    }".PHP_EOL;
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
    echo "    inline void marshall(::dsn::binary_writer& writer, const ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        marshall(writer, val." .$fld->name .");" .PHP_EOL;
    }
    echo "    }".PHP_EOL;
    echo PHP_EOL;
    echo "    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ ". $s->get_cpp_name() . "& val)".PHP_EOL;
    echo "    {".PHP_EOL;
    foreach ($s->fields as $fld) {
        echo "        unmarshall(reader, val." .$fld->name .");" .PHP_EOL;
    }
    echo "    }".PHP_EOL;
    echo PHP_EOL;
}

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>

#endif 
