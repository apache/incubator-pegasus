@ECHO OFF
SET TOP_DIR=%~dp0
SET bin_dir=%TOP_DIR%\scripts\windows
SET old_dsn_root=%DSN_ROOT%
if "%1" EQU "" GOTO usage
IF "%1" NEQ "setup-env" IF "%DSN_ROOT%" NEQ "" GOTO main
IF "%DSN_AUTO_TEST%" NEQ "" (
    SET DSN_ROOT=%TOP_DIR%\install
    GOTO install_env
)

SET /p DSN_ROOT=Please enter your DSN_ROOT (e.g., %TOP_DIR%\install):
IF "%DSN_ROOT%" EQU "" SET DSN_ROOT=%TOP_DIR%\install
@mkdir "%DSN_ROOT%"
IF "%DSN_ROOT%" NEQ "" IF exist "%DSN_ROOT%" GOTO install_env
CALL %bin_dir%\echoc.exe 4 %DSN_ROOT% does not exist
GOTO exit

:usage
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd setup-env|pre-require|build|install|test|publish|republish|deploy|start|stop|cleanup|scds(stop-cleanup-deploy-start)|start_zk|stop_zk"
    GOTO:EOF

:install_env
setlocal enabledelayedexpansion
SET old_path_appendix=;%old_dsn_root:/=\%\bin;%old_dsn_root:/=\%\lib;
SET new_path_appendix=;%DSN_ROOT:/=\%\bin;%DSN_ROOT:/=\%\lib;
SET lpath=%PATH%
SET lpath=%lpath:!old_path_appendix!=%
endlocal & SET PATH=%lpath%%new_path_appendix%
SETX PATH "%PATH%"
CALL reg add HKCU\Environment /f /v PATH /d "%PATH%" 1>nul
CALL %bin_dir%\flushenv.exe

SET DSN_ROOT=%DSN_ROOT:\=/%
CALL reg add HKCU\Environment /f /v DSN_ROOT /d %DSN_ROOT% 1>nul
CALL %bin_dir%\flushenv.exe

CALL %bin_dir%\echoc.exe 2 DSN_ROOT (%DSN_ROOT%) is setup, and rDSN SDK will be installed there.
CALL %bin_dir%\echoc.exe 2 DSN_ROOT\lib and DSN_ROOT\bin are added to PATH env.

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
:start_zk
:stop_zk
    CALL %bin_dir%\%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF

:publish
:republish
    CALL %bin_dir%\publish.cmd %1 %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF
    
:setup-env
    GOTO:EOF
    
:deploy
:start
:stop
:cleanup
:quick-cleanup
:scds
    CALL %bin_dir%\deploy.cmd %1 %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF
    
:exit


