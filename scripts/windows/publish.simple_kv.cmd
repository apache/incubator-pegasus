SET cmd=%1
SET build_dir=%~f2
SET build_type=%3
SET monitor_url=%4
SET bin_dir=%~dp0

:start_build
CALL :build_app meta
CALL :build_app replica
CALL :build_app client
CALL :build_app client.perf.test

(
    ECHO meta
    ECHO replica
    ECHO client
    ECHO client.perf.test
)  > .\skv\apps.txt

GOTO exit

:build_app
    set app=%1
    @MKDIR .\skv\%app%

    IF "%monitor_url%" NEQ "" (
        SET monitor_str=;monitor
        COPY /Y %monitor_url% .\skv\%app%\MonitorPack.7z
    )
    
    CALL %bin_dir%\copy_dsn_shared.cmd .\skv\%app%
    COPY /Y %build_dir%\bin\dsn.replication.simple_kv\%build_type%\dsn.replication.simple_kv.* .\skv\%app%
    if "%cmd%" NEQ "republish" (
        COPY /Y %build_dir%\bin\dsn.replication.simple_kv\config.ini .\skv\%app%
    )

    SET has_monitor=
    FOR /F "tokens=* USEBACKQ" %%F IN (`findstr apps.monitor .\skv\%app%\config.ini`) DO (
        SET has_monitor=%%F
    )
    
    IF "%monitor_url%" NEQ "" IF "%has_monitor%" EQU "" (
        ECHO rewrite config.ini for starting embedded monitor ...
        (
            ECHO[
            ECHO [apps.monitor]
            ECHO type = monitor
            ECHO arguments = 8088
            ECHO pools = THREAD_POOL_DEFAULT
            ECHO dmodule = dsn.dev.python_helper
            ECHO dmodule_bridge_arguments = rDSN.monitor\rDSN.Monitor.py
        ) >> .\skv\%app%\config.ini
    )
 
    ECHO write start.cmd ...
    (
        ECHO SET ldir=%%~dp0
        ECHO cd /d ldir
        ECHO IF NOT EXIST "rDSN.monitor" (
        ECHO    CALL .\7z.exe x -y MonitorPack.7z 
        ECHO    XCOPY /Y /E /I MonitorPack\* .\
        ECHO    rmdir /s /q MonitorPack
        ECHO ^)
        ECHO set i=0
        ECHO :loop
        ECHO     set /a i=%%i%%+1
        ECHO     echo run %%i%%th ... ^>^> ./running.txt
        ECHO     .\dsn.replication.simple_kv.exe config.ini -app_list %app%@1%monitor_str%
        ECHO     ping -n 16 127.0.0.1 ^>nul
        ECHO goto loop
    )  > .\skv\%app%\start.cmd

    GOTO:EOF

:error
    CALL %bin_dir%\echoc.exe 4  Usage: run publish^|republish build_dir build_type(Debug^|Release^|RelWithDebInfo^|MinSizeRel) [monitor_package_url]

:exit
