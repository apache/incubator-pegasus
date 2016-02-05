@echo off

if "%1" EQU "deploy_and_start" (
        CALL %~dp0\deploy.cmd deploy %2 %3 %4 %5 %6 %7 %8 %9
        if NOT ERRORLEVEL 0 (
            GOTO:error
        )
        CALL %~dp0\deploy.cmd start %2 %3 %4 %5 %6 %7 %8 %9
        if NOT ERRORLEVEL 0 (
            GOTO:error
        )
        GOTO:exit
    )
    
if "%1%" EQU "stop_and_cleanup" (
        CALL %~dp0\deploy.cmd stop %2 %3 %4 %5 %6 %7 %8 %9
        if NOT ERRORLEVEL 0 (
            GOTO:error
        )
        CALL %~dp0\deploy.cmd cleanup %2 %3 %4 %5 %6 %7 %8 %9
        if NOT ERRORLEVEL 0 (
            GOTO:error
        )
        GOTO:exit
    )

GOTO:error

:exit
exit 0


:error
exit 1
