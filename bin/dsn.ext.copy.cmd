ECHO OFF
SET PROJ_LIB_DIR=%1
SET DST_LIB_DIR=%2
SET max_time=
SET max_config=

IF "%PROJ_LIB_DIR%" EQU "" ECHO "project lib dir is not set" && GOTO exit
IF "%DST_LIB_DIR%" EQU "" ECHO "destination lib dir is not set" && GOTO exit

SET PROJ_LIB_DIR=%PROJ_LIB_DIR:/=\%
SET DST_LIB_DIR=%DST_LIB_DIR:/=\%

FOR %%i IN (Debug Release RelWithDebInfo MinSizeRel) DO CALL :check_latest %%i

GOTO copy_latest

:check_latest    
    SET lconfig=%1
    SET ltime=
    IF EXIST "%PROJ_LIB_DIR%\%lconfig%" (
        ECHO CHECK dir %PROJ_LIB_DIR%\%lconfig% ... 
        FOR /f "tokens=1,2,3" %%i IN ('dir /o:d /TW "%PROJ_LIB_DIR%\%lconfig%" ^| find "/"' ) DO (
            SET valid_k=0
            if "%%k" EQU "AM" SET valid_k=1
            if "%%k" EQU "PM" SET valid_k=1
            if "valid_k" EQU "1" (
                SET ltime=%%i %%j %%k
            ) else (
                SET ltime=%%i %%j
            )
        )
        IF "%max_time%" EQU "" (
            SET max_time=%ltime%
            SET max_config=%lconfig%
        ) ELSE (
            IF "%max_time%" lss "%ltime%" (
                SET max_time=%ltime%
                SET max_config=%lconfig%
            )
        )
    )
    REM ECHO max_time=%max_time%
    REM ECHO max_config=%max_config%
    GOTO:EOF
    
:copy_latest

IF "%max_config%" EQU "" (
    ECHO "cannot find latest build"
    GOTO exit
)

ECHO latest build is in dir %PROJ_LIB_DIR%\%max_config%

@ECHO ON
XCOPY /F /Y /S %PROJ_LIB_DIR%\%max_config% %DST_LIB_DIR%
    
:exit
