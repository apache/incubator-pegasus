SET build_dir=%~f1
SET build_type=%2
SET bin_dir=%~dp0

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
        ECHO cd /d %%~dp0
        ECHO set i=0
        ECHO :loop
        ECHO     set /a i=%%i%%+1
        ECHO     .\dsn.replication.simple_kv.exe config.ini -app %app% -app_index 1
        ECHO     ping -n 16 127.0.0.1 ^>nul
        ECHO goto loop
    )  > .\skv-%app%\start.cmd
    GOTO:EOF

:error    
    CALL %bin_dir%\echoc.exe 4  Usage: deploy.simple_kv.cmd build_dir build_type(Debug^|Release^|RelWithDebInfo^|MinSizeRel)

:exit


