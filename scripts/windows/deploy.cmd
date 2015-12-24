SET bin_dir=%~dp0
SET cmd=%1
SET src_dir=%~f2
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

IF "%cmd%" EQU "deploy" (
    @RD /s /q %deploy_name%.pack
)

::SET cmd=%1
::SET src_dir=%2
::SET ldst_dir=%3
::SET deploy_name=%4
::SET rdst_dir=%ldst_dir::=$%
::SET machine=%5
FOR /F %%i IN (%machine_list%) DO ECHO %cmd% %%i ... && start %bin_dir%\deploy.one.cmd %cmd% %src_dir% %ldst_dir% %deploy_name% %%i

IF ERRORLEVEL 1 (
    CALL %bin_dir%\echoc.exe 4 unknow command '%cmd%'
    CALL :usage
    GOTO exit
)

GOTO exit

:usage
    ECHO run.cmd deploy^|start^|stop^|cleanup^|quick-cleanup source-dir target-dir
    ECHO  Example: "run deploy .\skv-meta d:\zhenyug" deploys skv-meta to d:\zhenyug\skv-meta.
    ECHO  source-dir is a directory which contains a start.cmd, machines.txt, and other resource files/dirs
    GOTO:EOF

REM  
REM |-source-dir|target-dir
REM   - start.cmd
REM   - machines.txt
REM   - other dependent files or dirs
REM 

    
:exit

