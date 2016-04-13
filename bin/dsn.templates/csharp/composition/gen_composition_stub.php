<?php

global $idl;
global $out_dir;
global $cdir;
global $clibs;
global $idl_type;
global $idl_post;
global $program;
global $idl_php;
global $is_replicated;
global $idl_format;

$out_dir = "tmp";

foreach(array_slice($argv, 1) as $idl) {
    if (!file_exists($idl)) {
        echo("input file".$idl."not found".PHP_EOL);
    } else {
        if (strlen($idl) > strlen(".thrift")
          && substr($idl, strlen($idl) - strlen(".thrift")) == ".thrift")
        {
            $idl_type = "thrift";
            $idl_post = ".php";
        }
        else if (strlen($idl) > strlen(".proto")
          && substr($idl, strlen($idl) - strlen(".proto")) == ".proto")
        {
            $idl_type = "proto";
            $idl_post = ".pb.php";
        }
        else
        {
            echo "unknown idl type for input file '".$idl."'".PHP_EOL;
            exit(0);
        }

        $pos = strrpos($idl, "\\");
        $pos2 = strrpos($idl, "/");
        if ($pos == FALSE && $pos2 == FALSE)
        {
            $program = substr($idl, 0, strlen($idl) - strlen($idl_type) - 1);
        }
        else if ($pos != FALSE)
        {
            $program = substr($idl, $pos + 1, strlen($idl) - $pos - 1  - strlen($idl_type) - 1);
        }
        else
        {
            $program = substr($idl, $pos2 + 1, strlen($idl) - $pos2 - 1  - strlen($idl_type) - 1);
        }
        $idl_php = $out_dir."/".$program.$idl_post;
        $os_name = explode(" ", php_uname())[0];
        switch ($idl_type)
        {
        case "thrift":
            {
                $command = __DIR__."/".$os_name."/thrift --gen rdsn -out ".$out_dir." ".$idl;
                echo "exec: ".$command.PHP_EOL;
                system($command);
                if (!file_exists($idl_php))
                {
                    echo "failed invoke thrift tool to generate '".$idl_php."'".PHP_EOL;
                    exit(0);
                }
            }
            break;
        case "proto":
            {
                $idl_dir = dirname($idl);
                $command = __DIR__."/".$os_name."/protoc --rdsn_out=".$out_dir." ".$idl." -I=".$idl_dir;
                echo "exec: ".$command.PHP_EOL;
                system($command);
                if (!file_exists($idl_php))
                {
                    echo "failed invoke protoc tool to generate '".$idl_php."'".PHP_EOL;
                    exit(0);
                }
            }
            break;
        default:
            echo "idl type '". $idl_type ."' not supported yet!".PHP_EOL;
            exit(0);
        }
        $idls[] = $idl_php;
    }
}

$command = "php -f ".__DIR__."/composition_stub.php";

foreach ($idls as $idl) {
    $command = $command." ".$idl;
}

system($command);
?>
