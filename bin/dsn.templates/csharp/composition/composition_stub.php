<?php
function write_wrapper($file) {
    unlink($file);
    return function($str) use ($file) {
        file_put_contents($file, $str."\n", FILE_APPEND);
    };
}
require_once(__DIR__."/../../type.php"); // type.php
$structs = array();
$last_proto_name = "";
$last_thrift_name = "";
$comstub = write_wrapper("tmp/compo.cs");
$comstub("
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using rDSN.Tron.Utility;
using rDSN.Tron.Contract;
using rDSN.Tron;
");

foreach (array_slice($argv, 1) as $arg) {
    $idl_type = pathinfo(basename(basename($arg), ".".pathinfo($arg, PATHINFO_EXTENSION)), PATHINFO_EXTENSION);
    print($idl_type);
    require_once($arg);
    foreach ($_PROG->structs as $key => $struct) {
        if (array_key_exists($_PROG->get_csharp_namespace().$struct->name, $structs)) {
            unset($_PROG->structs[$key]);
        } else {
            $structs[$_PROG->get_csharp_namespace().$struct->name] = true;
        }
    }
    if ($idl_type == "pb") {
        $idl_type = "proto";
        gen_proto($_PROG, $last_proto_name);
        $last_proto_name = $_PROG->name.".proto";
    } else {
        $idl_type = "thrift";
        $last_thrift_name = $_PROG->name.".thrift";
    }
    gen_compo($_PROG, $comstub, $idl_type);
}

function gen_proto($prog, $last) {
    $out = write_wrapper("tmp/".$prog->name.".proto");
    $out("syntax = \"proto3\";");
    $out("package ".$prog->get_csharp_namespace().";");
    if ($last != "") {
        $out("import public \"".$last."\";");
    }
    foreach ($prog->structs as $s) {
        $out("message ".$s->name."{");
        $id = 1;
        foreach ($s->fields as $fld) {
            $out($fld->type_name." ".$fld->name." = ".$fld->id.";");
            $id += 1;
        }
        $out("}");
    }
    foreach ($prog->services as $svc) {
        $out("service ".$svc->name."{");
        foreach ($svc->functions as $f) {
            $out("rpc ".$f->name." (".$f->params[0]->type_name.") returns (".$f->ret.");");
        }
        $out("}");
    }
}

function gen_thrift($prog, $last) {
    $out = write_wrapper("tmp/".$prog->name.".thrift");
    $out("namespace csharp ".$prog->get_csharp_namespace());
    if ($last != "") {
        $out("include \"".$last."\"");
    }
    foreach ($prog->structs as $s) {
        $out("struct ".$s->name."{");
        foreach ($s->fields as $fld) {
            $out($fld->id.": ".$fld->type_name." ".$fld->name.";");
        }
        $out("}");
    }
    foreach ($prog->services as $svc) {
        $out("service ".$svc->name."{");
        foreach ($svc->functions as $f) {
            $out($f->ret." ".$f->name."(1:".$f->params[0]->type_name." ".$f->params[0]->name.");");
        }
        $out("}");
    }
}

function gen_compo($prog, $out, $idl_type) {
    $out("namespace ".$prog->get_csharp_namespace()." {");
    foreach ($prog->structs as $s) {
        $out("public class ".$s->get_csharp_name()."{ ");
        foreach ($s->fields as $fld) {
            $out("[IDL.FieldAttribute(".$fld->id.")]");
            $out("public ".$fld->get_csharp_type()." ".$fld->name.";");
        }
        $out("}");
    }
    foreach ($prog->services as $svc) {
        $out("public sealed class Service_".$svc->name." : rDSN.Tron.Contract.Service {");
        $out("public  Service_".$svc->name."(string serviceUri, string name = \"\") : base(\"".$prog->name."\", serviceUri, name) {");
        $out("Spec.SType = ServiceSpecType.".$idl_type.";");
        $out("Spec.MainSpecFile = \"".$prog->name.".".$idl_type."\";");
        $out("Spec.ReferencedSpecFiles = new List<string>();");
        $out("Spec.IsRdsnRpc = true;");
        $out("}");
        foreach ($svc->functions as $f) {
            $out("public ".$f->get_csharp_return_type()." ".$f->name."(".$f->get_first_param()->get_csharp_type()." ".$f->get_first_param()->name.") {");
            $out("throw new NotImplementedException(\"no need to implement, this is just a placeholder\");");
            $out("}");
        }
        $out("}");
    }
    $out("}");
}
?>
