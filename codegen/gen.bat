@ECHO OFF
IF "%3" == "" ECHO "gen.bat program_name program.php output.dir" && GOTO :EOF
IF NOT EXIST %2 ECHO "%2 does not exist " && GOTO :EOF
IF NOT EXIST %3 MKDIR %3
IF NOT EXIST %3 ECHO "%3 does not exist " && GOTO :EOF

SET CODEGEN_ROOT=e:\rdsn\codegen
SET CODEGEN_LIBS=%CODEGEN_ROOT%\libs

CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% cpp.code.definition.php %2 %1 > %3\%1.code.definition.h
ECHO %3\%1.code.definition.h is ready
CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% cpp.client.php %2 %1 > %3\%1.client.h
ECHO %3\%1.client.h is ready
CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% cpp.server.php %2 %1 > %3\%1.server.h
ECHO %3\%1.server.h is ready
CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% cpp.app.example.php %2 %1 > %3\%1.app.example.h
ECHO %3\%1.app.example.h is ready
CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% cpp.main.php %2 %1 > %3\%1.main.cpp
ECHO %3\%1.main.cpp is ready
CALL %CODEGEN_ROOT%\gen_one.bat %CODEGEN_LIBS% config.php %2 %1 > %3\%1.ini
ECHO %3\%1.ini is ready
:EOF
