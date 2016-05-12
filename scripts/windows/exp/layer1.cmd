SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET cluster=%2
SET ifauto=%3
SET toollets=%4

if "%1" EQU "memcached" SET test_app_upper=MemCached
if "%1" EQU "thumbnail" SET test_app_upper=ThumbnailServe
if "%1" EQU "xlock" SET test_app_upper=XLock
if "%1" EQU "leveldb" SET test_app_upper=LevelDb
if "%1" EQU "kyotocabinet" SET test_app_upper=KyotoCabinet
if "%1" EQU "redis" (
    SET test_app_upper=redis
    SET l1_try_fetch_log_times=300
)
if "%test_app_upper%" EQU "" (
    CALL %bin_dir%\echoc.exe 4  no such app %1 for perf test, please check spelling
    GOTO:EOF
)


IF NOT EXIST "%bin_root%\dsn.core.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.core.dll not exist, please copy it from DSN_ROOT repo
    GOTO:EOF
)

IF "%test_app_upper%" NEQ "redis" (
    IF NOT EXIST "%bin_root%\dsn.apps.%test_app_upper%.dll" (
        CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.apps.%test_app_upper%.dll not exist, please copy it from bondex repo
        GOTO:EOF
    )
)

IF NOT EXIST "%bin_root%\dsn.svchost.exe" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.svchost.exe not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

IF NOT EXIST "%exp_dir%cluster\%cluster%.txt" (
    CALL %bin_dir%\echoc.exe 4  %exp_dir%cluster\%cluster%.txt not exist, please see the detailed instructions in the cluster folder
    GOTO:EOF
)

< %exp_dir%cluster\%cluster%.txt (
    set /p server_address=
    set /p clientperf_address=
)

@mkdir "%exp_dir%log"
@mkdir "%exp_dir%log\layer1"
@mkdir "%exp_dir%log\layer1\%test_app_upper%"
SET log_dir=%exp_dir%log\layer1\%test_app_upper%

@mkdir "%exp_dir%layer1_test"
@rmdir /Q /S "%exp_dir%layer1_test\%test_app_upper%"
@mkdir "%exp_dir%layer1_test\%test_app_upper%"
SET app_dir=%exp_dir%layer1_test\%test_app_upper%
@mkdir "%app_dir%\server"
@mkdir "%app_dir%\client.perf"

(
    ECHO server
    ECHO client.perf
)  > %app_dir%\apps.txt
(
    ECHO %server_address%
)  > %app_dir%\server\machines.txt
(
    ECHO %clientperf_address%
)  > %app_dir%\client.perf\machines.txt

set default_config=0
if "%toollets%" EQU "bare" set default_config=1
if "%toollets%" EQU "" set default_config=1
if "%default_config%" EQU "1" (
    copy /Y %exp_dir%config\layer1\%test_app_upper%\config.ini %app_dir%\server\config.ini
    copy /Y %exp_dir%config\layer1\%test_app_upper%\config.ini %app_dir%\client.perf\config.ini
) else (
    copy /Y %exp_dir%config\layer1\%test_app_upper%\config-%toollets%.ini %app_dir%\server\config.ini
    copy /Y %exp_dir%config\layer1\%test_app_upper%\config-%toollets%.ini %app_dir%\client.perf\config.ini
)

copy /Y %bin_root%\*.* %app_dir%\server
copy /Y %bin_root%\*.* %app_dir%\client.perf

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list server -cargs server_address=%server_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\server\start.cmd

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list client.perf.%test_app% -cargs server_address=%server_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\client.perf\start.cmd

CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER1] [%test_app_upper% %toollets%] BEGIN***** 

CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l1_stop_and_cleanup_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)

CALL %bin_dir%"\deploy.cmd" deploy %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****DEPOLYING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l1_deploy_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)

CALL %bin_dir%"\deploy.cmd" start %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STARTING...***** 

set /a counter=0
CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING LOG IN ROUND*****

:loop
    ping -n %l1_fetch_wait_duration% 127.0.0.1
    if exist \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.txt (
        mkdir %app_dir%\log\
        xcopy  /F /Y /S \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.* %app_dir%\log
        cd /d %app_dir%\log
        for %%a in (*.*) do ren "%%a" "%toollets%-%%a"
        cd /d %exp_dir%
        xcopy  /F /Y /S %app_dir%\log\*.* %exp_dir%log\layer1\%test_app_upper%\

        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER1] [%test_app_upper%] SUCCESS***** 
        CALL %bin_dir%\echoc.exe 3 *****LOG AND CONFIG SAVDED IN %exp_dir%log\layer1\%test_app_upper%\***** 
        CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
        CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 
        ::CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
        if "%test_app%" EQU "redis" (
            CALL %bin_dir%\echoc.exe 3 *****KILLING REDIS-SERVER.exe...***** 
            TASKKILL /F /S %server_address% /IM redis-server.exe
        )
        ping -n %l1_fetch_wait_duration% 127.0.0.1
        goto end
    )
    set /a counter=%counter%+1
    CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING FOR TIME %counter%***** 
    
    if "%counter%" == "%l1_try_fetch_log_times%" (
        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER1] [%test_app_upper%] FAIL***** 
        GOTO end
    )
goto loop

:end
    CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER1] [%test_app_upper% %toollets%] END***** 




