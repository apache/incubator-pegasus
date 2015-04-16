<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include <dsn/internal/serialization.h>

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

foreach ($_PROG->enums as $em) 
{
	echo "\t// ---------- ". $em->name . " -------------". PHP_EOL;
	echo "\tenum ". $em->get_cpp_name() .PHP_EOL;
	echo "\t{".PHP_EOL;
	foreach ($em->values as $k => $v) {
		echo "\t\t". $k . " = " .$v ."," .PHP_EOL;
	}
	echo "\t};".PHP_EOL;
	echo PHP_EOL;
	echo "\tDEFINE_POD_SERIALIZATION(". $em->get_cpp_name() .");".PHP_EOL;
	echo PHP_EOL;
}

foreach ($_PROG->structs as $s) 
{
	echo "\t// ---------- ". $s->name . " -------------". PHP_EOL;
	echo "\tstruct ". $s->get_cpp_name() .PHP_EOL;
	echo "\t{".PHP_EOL;
	foreach ($s->fields as $fld) {
		echo "\t\t". $fld->get_cpp_type() . " " .$fld->name .";" .PHP_EOL;
	}
	echo "\t};".PHP_EOL;
	echo PHP_EOL;
	echo "\tinline void marshall(::dsn::binary_writer& writer, const ". $s->get_cpp_name() . "& val)".PHP_EOL;
	echo "\t{".PHP_EOL;
	foreach ($s->fields as $fld) {
		echo "\t\tmarshall(writer, val." .$fld->name .");" .PHP_EOL;
	}
	echo "\t};".PHP_EOL;
	echo PHP_EOL;
	echo "\tinline void unmarshall(::dsn::binary_reader& reader, __out_param ". $s->get_cpp_name() . "& val)".PHP_EOL;
	echo "\t{".PHP_EOL;
	foreach ($s->fields as $fld) {
		echo "\t\tunmarshall(reader, val." .$fld->name .");" .PHP_EOL;
	}
	echo "\t};".PHP_EOL;
	echo PHP_EOL;
}

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>
