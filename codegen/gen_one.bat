@ECHO OFF
IF "%4" == "" ECHO "gen_one.bat codegen.libs.path template.php program.php program_prefix" && GOTO :EOF
IF NOT EXIST %1 ECHO "%1 does not exist " && GOTO :EOF
IF NOT EXIST %1\%2 ECHO "%1\%2 does not exist " && GOTO :EOF
IF NOT EXIST %3 ECHO "%3 does not exist " && GOTO :EOF
IF NOT EXIST %1\type.php ECHO "%1\type.php does not exist " && GOTO :EOF
CALL e:\tools\php.exe -f %1\%2 %1\type.php %3 %4
:EOF
