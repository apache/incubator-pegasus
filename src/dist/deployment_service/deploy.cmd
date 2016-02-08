SET bin_dir=%~dp0
SET cmd=%1
SET src_dir=%~f2

IF EXIST "%src_dir%\apps.txt" (
    FOR /F "tokens=*" %%A IN (%src_dir%\apps.txt) DO CALL %bin_dir%\deploy.cmd %1 %2\%%A %3 %4 %5
    GOTO exit
)

SET deploy_name=%~n2
SET ldst_dir=%3\%deploy_name%

SET rdst_dir=%ldst_dir::=$%
SET machine_list=%src_dir%\machines.txt

IF "%src_dir%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 source directory not specified
    CALL :usage
    GOTO exit
)

IF NOT EXIST "%src_dir%" (
    CALL %bin_dir%\echoc.exe 4 cannot find source directory '%src_dir%'
    CALL :usage
    GOTO exit
)

IF NOT EXIST "%src_dir%\start.cmd" (
    CALL %bin_dir%\echoc.exe 4 please create start.cmd in source directory '%src_dir%'
    GOTO exit
)

IF NOT EXIST "%machine_list%" (
    CALL %bin_dir%\echoc.exe 4 cannot find machine_list file '%machine_list%'
    CALL :usage
    GOTO exit
)

IF "%3" EQU "" (
    CALL %bin_dir%\echoc.exe 4 destination dir not specified
    CALL :usage
    GOTO exit
)

::SET cmd=%1
::SET src_dir=%2
::SET ldst_dir=%3
::SET deploy_name=%4
::SET rdst_dir=%ldst_dir::=$%
::SET machine=%5
FOR /F %%i IN (%machine_list%) DO ECHO %cmd% %%i ... && CALL %bin_dir%\deploy.one.cmd %cmd% %src_dir% %ldst_dir% %deploy_name% %%i

IF ERRORLEVEL 1 (
    CALL %bin_dir%\echoc.exe 4 unknow command '%cmd%'
    CALL :usage
    GOTO exit
)

GOTO exit

:usage
    ECHO run.cmd deploy^|start^|stop^|cleanup^|quick-cleanup^|scds(stop-cleanup-deploy-start) source-dir target-dir
    ECHO  source-dir is a directory which contains a start.cmd, machines.txt, and other resource files/dirs
    ECHO  or, a directory which contains a set of directories above which will be deployed simultaneously.
    ECHO  Example: "run deploy .\skv\meta d:\zhenyug" deploys meta to d:\zhenyug\meta
    ECHO  Example: "run deploy .\skv d:\zhenyug" deploys skv\meta, skv\replica, ... to d:\zhenyug\meta, replica, ...    
    GOTO:EOF

REM  
REM |-source-dir|target-dir
REM   - start.cmd
REM   - machines.txt
REM   - other dependent files or dirs
REM 

    
:exit

