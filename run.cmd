@ECHO OFF
SET TOP_DIR=%~dp0
SET bin_dir=%TOP_DIR%\scripts\windows
if "%1" EQU "" GOTO usage
IF "%DSN_ROOT%" NEQ "" GOTO main

SET /p DSN_ROOT=Please enter your DSN_ROOT (default is %TOP_DIR%\install):
IF "%DSN_ROOT%" EQU "" SET DSN_ROOT=%TOP_DIR%\install
@mkdir "%DSN_ROOT%"
IF "%DSN_ROOT%" NEQ "" IF exist "%DSN_ROOT%" GOTO install_env
CALL %bin_dir%\echoc.exe 4 %DSN_ROOT% does not exist
GOTO exit

:usage
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd pre-require|build|install|test|publish|deploy|start|stop|cleanup"
    GOTO:EOF

:install_env
SET DSN_ROOT=%DSN_ROOT:\=/%
reg add HKCU\Environment /f /v DSN_ROOT /d %DSN_ROOT% 1>nul
CALL %bin_dir%\flushenv.exe
CALL %bin_dir%\echoc.exe 2 DSN_ROOT (%DSN_ROOT%) is added as env var, and rDSN SDK will be installed there.

:main
CALL :%1 %1 %2 %3 %4 %5 %6 %7 %8 %9

IF ERRORLEVEL 1 (
    CALL %bin_dir%\echoc.exe 4 unknow command '%1'
    CALL :usage
    GOTO exit
)

GOTO exit

:pre-require
:build
:install
:test
    CALL %bin_dir%\%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF

:publish
    CALL %bin_dir%\%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF
    
:deploy
:start
:stop
:cleanup
    CALL %bin_dir%\deploy.cmd %1 %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF
    
:exit


