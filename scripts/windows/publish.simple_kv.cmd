SET build_dir=%~f1
SET build_type=%2
SET bin_dir=%~dp0
SET monitor_url=%3

IF "%monitor_url%" EQU "" GOTO start_build
SET monitor_str=;monitor
xcopy /z %monitor_url% .\
CALL %bin_dir%\7z.exe x -y MonitorPack.7z
del MonitorPack.7z

:start_build
CALL :build_app meta
CALL :build_app replica
CALL :build_app client
CALL :build_app client.perf.test
GOTO exit

:build_app
    set app=%1
    MKDIR .\skv-%app%
    CALL %bin_dir%\copy_dsn_shared.cmd .\skv-%app%
    COPY /Y %build_dir%\bin\dsn.replication.simple_kv\%build_type%\dsn.replication.simple_kv.* .\skv-%app%
    COPY /Y %build_dir%\bin\dsn.replication.simple_kv\config.ini .\skv-%app%
    (
        ECHO[
        ECHO [apps.monitor]
        ECHO type = monitor
        ECHO arguments = 8088
        ECHO pools = THREAD_POOL_DEFAULT
        ECHO dmodule = dsn.dev.python_helper
        ECHO dmodule_bridge_arguments = rDSN.monitor\rDSN.Monitor.py
    ) >> .\skv-%app%\config.ini
    (
        ECHO cd /d %%~dp0
        ECHO set i=0
        ECHO :loop
        ECHO     set /a i=%%i%%+1
        ECHO     .\dsn.replication.simple_kv.exe config.ini -app_list %app%@1%monitor_str%
        ECHO     ping -n 16 127.0.0.1 ^>nul
        ECHO goto loop
    )  > .\skv-%app%\start.cmd
    IF "%monitor%" NEQ "-m" GOTO:EOF
    XCOPY /Y /E /I MonitorPack\* .\skv-%app% 
    GOTO:EOF

:error
    CALL %bin_dir%\echoc.exe 4  Usage: deploy.simple_kv.cmd build_dir build_type(Debug^|Release^|RelWithDebInfo^|MinSizeRel) [monitor_package_url]

:exit
