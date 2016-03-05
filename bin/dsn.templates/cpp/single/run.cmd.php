<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>


@echo on
REM
REM This configuration file is used by app store to automatically
REM deployed a single rDSN application in a rDSN cluster as L2 services.
REM The following environment variables are defined and passed to this config file
REM by rDSN.
REM
REM  - %port%, which defines which service port to listen to.
REM  - %package_dir%, which contains run.cmd and the related resources (e.g., config files, executables)
REM

echo port = %port% >> run.txt
echo package_dir = %package_dir% >> run.txt

CALL %package_dir%\<?=$_PROG->name?>.exe %package_dir%\config.appstore.ini -cargs port=%port% -app_list server  >> run.txt


