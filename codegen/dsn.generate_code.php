<?php

function usage()
{
	echo "dsn.cg %name%.thrift|.proto %out_dir%".PHP_EOL;
}

if (count($argv) < 3)
{
	usage();
	exit(0);
}

global $g_idl;
global $g_out_dir;
global $g_cg_dir;
global $g_cg_libs;
global $g_idl_type;
global $g_idl_post;
global $g_program;
global $g_idl_php;

$g_idl = $argv[1];
$g_out_dir = $argv[2];
$g_cg_dir = __DIR__;
$g_cg_libs = $g_cg_dir."/dsn.templates";
$g_idl_type = "";
$g_idl_post = "";
$g_program = "";
$g_idl_php = "";

if (!file_exists($g_idl))
{
	echo "input file '". $g_idl ."' is not found.".PHP_EOL;
	exit(0);
}
else
{
	if (strlen($g_idl) > strlen(".thrift")
	  && substr($g_idl, strlen($g_idl) - strlen(".thrift")) == ".thrift")
	{
		$g_idl_type = "thrift";
		$g_idl_post = ".php";
	}
	else if (strlen($g_idl) > strlen(".proto")
	  && substr($g_idl, strlen($g_idl) - strlen(".proto")) == ".proto")
	{
		$g_idl_type = "proto";
		$g_idl_post = ".pb.php";
	}
	else
	{
		echo "unknown idl type for input file '".$g_idl."'".PHP_EOL;
		exit(0);
	}
}

$pos = strrpos($g_idl, "\\");
$pos2 = strrpos($g_idl, "/");
if ($pos == FALSE && $pos2 == FALSE)
{
	$g_program = substr($g_idl, 0, strlen($g_idl) - strlen($g_idl_type) - 1);
}
else if ($pos != FALSE)
{
	$g_program = substr($g_idl, $pos + 1, strlen($g_idl) - $pos - 1  - strlen($g_idl_type) - 1);
}
else
{
	$g_program = substr($g_idl, $pos2 + 1, strlen($g_idl) - $pos2 - 1  - strlen($g_idl_type) - 1);
}

$g_idl_php = $g_out_dir."/".$g_program.$g_idl_post;

if (!file_exists($g_out_dir))
{
	if (!mkdir($g_out_dir))
	{
		echo "create output directory '". $g_out_dir ."' failed.".PHP_EOL;
		exit(0);
	}
	else
	{
		echo "output directory '". $g_out_dir ."' created.".PHP_EOL;
	}
}

// generate service definition file from input idl file using the code generation tools
switch ($g_idl_type)
{
case "thrift":
	{
		$command = "thrift --gen rdsn -out ".$g_out_dir." ".$g_idl;
		echo "exec: ".$command.PHP_EOL;
		system($command);
		if (!file_exists($g_idl_php))
		{
			echo "failed invoke thrift tool to generate '".$g_idl_php."'".PHP_EOL;
			exit(0);
		}
	}
	break;
case "proto":
	{
		$command = "protoc --rdsn_out=".$g_out_dir." ".$g_idl;
		echo "exec: ".$command.PHP_EOL;
		system($command);
		if (!file_exists($g_idl_php))
		{
			echo "failed invoke protoc tool to generate '".$g_idl_php."'".PHP_EOL;
			exit(0);
		}
	}
	break;
default:
	echo "idl type '". $g_idl_type ."' not supported yet!".PHP_EOL;
	exit(0);
}

foreach (scandir($g_cg_libs) as $template)
{
	if ($template == "type.php" 
		|| $template == "." 
		|| $template == ".." 
		)
		continue;
		
	$output_file = $g_out_dir."/".$g_program.".".substr($template, 0, strlen($template)-4);
	$command = "php -f ".$g_cg_libs."/".$template
				." ".$g_cg_libs."/type.php"
				." ".$g_idl_php
				." ".$g_program
				." >".$output_file
				;

	echo "exec: ".$command.PHP_EOL;
	system($command);
	if (!file_exists($output_file))
	{
		echo "failed to generate '".$output_file."'".PHP_EOL;
		exit(0);
	}
	else
	{
		echo "generate '".$output_file."' successfully!".PHP_EOL;
	}
}

?>
