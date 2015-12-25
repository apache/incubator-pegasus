SET bin_dir=%~dp0
SET INSTALL_DIR=%~f1
SET PORT=%2
SET zk=zookeeper-3.4.6

IF "%PORT%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 PORT not specified
    CALL :usage
    GOTO exit
)

TASKKILL /F /FI "WINDOWTITLE eq zk-%PORT% - %INSTALL_DIR%\%zk%\bin\zkServer.cmd"
TASKKILL /F /FI "WINDOWTITLE eq zk-%PORT% - %INSTALL_DIR%\%zk%\bin\zkServer.cmd"
GOTO exit

:usage
    ECHO run.cmd stop_zk INSTALL_DIR PORT
    GOTO:EOF
    
:exit
