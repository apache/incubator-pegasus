SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET cluster=%2
SET ifauto=%3

if "%1" EQU "simple_kv" (
    SET test_app_upper=simple_kv
    SET dll_name=dsn.replication.simple_kv.module.dll
)
    
if "%1" EQU "leveldb" (
    SET test_app_upper=LevelDb
    SET dll_name=dsn.apps.LevelDb.dll
)

if "%1" EQU "xlock" (
    SET test_app_upper=XLock
    SET dll_name=dsn.apps.XLock.dll
)

if "%1" EQU "kyotocabinet" (
    SET test_app_upper=KyotoCabinet
    SET dll_name=dsn.apps.KyotoCabinet.dll
)


if "%1" EQU "redis" (
    SET test_app_upper=redis
    SET dll_name=redis.dll
)

if "%test_app_upper%" EQU "" (
    CALL %bin_dir%\echoc.exe 4  no such app %1 for perf test, please check spelling
    GOTO:EOF
)


IF NOT EXIST "%bin_root%\dsn.core.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.core.dll not exist, please copy it from DSN_ROOT repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\%dll_name%" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\%dll_name% not exist, please copy it from bondex repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.svchost.exe" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.svchost.exe not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.meta_server.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.meta_server.dll not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.layer2.stateful.type1.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.layer2.stateful.type1.dll not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

< %exp_dir%cluster\%cluster%.txt (
    set /p meta_address=
    set /p replica1_address=
    set /p replica2_address=
    set /p replica3_address=
    set /p clientperf_address=
)

@mkdir "%exp_dir%log"
@mkdir "%exp_dir%log\layer2"
@mkdir "%exp_dir%log\layer2\%test_app_upper%"
SET log_dir=%exp_dir%log\layer2\%test_app_upper%

@mkdir "%exp_dir%layer2_test"
@rmdir /Q /S "%exp_dir%layer2_test\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\%test_app_upper%"
SET app_dir=%exp_dir%layer2_test\%test_app_upper%
@mkdir "%app_dir%\meta"
@mkdir "%app_dir%\replica"
@mkdir "%app_dir%\client.perf"

(
    ECHO meta
    ECHO replica
    ECHO client.perf
)  > %app_dir%\apps.txt
(
    ECHO %meta_address%
)  > %app_dir%\meta\machines.txt
(
    ECHO %replica1_address%
    ECHO %replica2_address%
    ECHO %replica3_address%
)  > %app_dir%\replica\machines.txt
(
    ECHO %clientperf_address%
)  > %app_dir%\client.perf\machines.txt

copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %app_dir%\meta
copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %app_dir%\replica
copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %app_dir%\client.perf

copy /Y %bin_root%\*.* %app_dir%\meta
copy /Y %bin_root%\*.* %app_dir%\replica
copy /Y %bin_root%\*.* %app_dir%\client.perf

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list meta -cargs meta_address=%meta_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\meta\start.cmd

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list replica@1 -cargs meta_address=%meta_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\replica\start.cmd

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    if "%test_app%" EQU "simple_kv" (
        ECHO .\dsn.svchost.exe config.ini -app_list client.perf.test -cargs meta_address=%meta_address%
    ) ELSE (
        ECHO .\dsn.svchost.exe config.ini -app_list client.perf.%test_app% -cargs meta_address=%meta_address%
    )
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\client.perf\start.cmd

CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER2.STATEFUL] [%test_app_upper%] BEGIN***** 

CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l2stateful_stop_and_cleanup_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)

CALL %bin_dir%"\deploy.cmd" deploy %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****DEPOLYING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l2stateful_deploy_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)

CALL %bin_dir%"\deploy.cmd" start %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STARTING...***** 

set /a counter=0
CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING LOG IN ROUND***** 
:loop
    ping -n %l2stateful_fetch_wait_duration% 127.0.0.1
    if exist \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.txt (
        xcopy  /F /Y /S \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.* %exp_dir%log\layer2\%test_app_upper%\
        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER2.STATEFUL] [%test_app_upper%] SUCCESS***** 
        CALL %bin_dir%\echoc.exe 3 *****LOG AND CONFIG SAVDED IN %exp_dir%log\layer2\%test_app_upper%\***** 
        CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
        CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 
        ::redis will invoke another process called redis-server.exe, so we need to call another kill.cmd to clean that process
        if "%test_app%" EQU "redis" (
            CALL %bin_dir%\echoc.exe 3 *****KILLING REDIS-SERVER.exe...***** 
            TASKKILL /F /S %replica1_address% /IM redis-server.exe
            TASKKILL /F /S %replica2_address% /IM redis-server.exe
            TASKKILL /F /S %replica3_address% /IM redis-server.exe

        )
        ping -n %l1_fetch_wait_duration% 127.0.0.1
        ::CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
        goto end
    )
    set /a counter=%counter%+1
    CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING FOR TIME %counter%***** 

    if "%counter%" == "%l2stateful_try_fetch_log_times%" (
        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER2.STATEFUL] [%test_app_upper%] FAIL***** 
        GOTO end
    )
goto loop

:end
    CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER2.STATEFUL] [%test_app_upper%] END*****




