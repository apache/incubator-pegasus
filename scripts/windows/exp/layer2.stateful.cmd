SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET meta_address=%2
SET replica1_address=%3
SET replica2_address=%4
SET replica3_address=%5
SET clientperf_address=%6

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

if "%1" EQU "rrdb" (
    SET test_app_upper=rrdb
    SET dll_name=dsn.apps.rrdb.dll
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

@mkdir "%exp_dir%layer2_test"
@rmdir /Q /S "%exp_dir%layer2_test\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\log"
@mkdir "%exp_dir%layer2_test\log\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\meta"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\replica"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\client.perf"

(
    ECHO meta
    ECHO replica
    ECHO client.perf
)  > %exp_dir%layer2_test\%test_app_upper%\apps.txt
(
    ECHO %meta_address%
)  > %exp_dir%layer2_test\%test_app_upper%\meta\machines.txt
(
    ECHO %replica1_address%
    ECHO %replica2_address%
    ECHO %replica3_address%
)  > %exp_dir%layer2_test\%test_app_upper%\replica\machines.txt
(
    ECHO %clientperf_address%
)  > %exp_dir%layer2_test\%test_app_upper%\client.perf\machines.txt

copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\meta
copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\replica
copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\client.perf

copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\meta
copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\replica
copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\client.perf

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
)  > %exp_dir%layer2_test\%test_app_upper%\meta\start.cmd

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
)  > %exp_dir%layer2_test\%test_app_upper%\replica\start.cmd

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
)  > %exp_dir%layer2_test\%test_app_upper%\client.perf\start.cmd
CALL %bin_dir%"\deploy.cmd" stop %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
CALL %bin_dir%"\deploy.cmd" cleanup %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
ECHO Wait for stop and cleanup finish, please continue after that
PAUSE
CALL %bin_dir%"\deploy.cmd" deploy %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
ECHO Wait for deployment finish, please continue after that
PAUSE

CALL %bin_dir%"\deploy.cmd" start %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
ECHO Starting all nodes...
 
:loop
    if exist \\%clientperf_address%\D$\v-chlou\client\data\client.perf.test\*.txt (
        xcopy  /F /Y /S \\%clientperf_address%\D$\v-chlou\client\data\client.perf.test\*.* %exp_dir%layer2_test\log\%test_app_upper%
        CALL %bin_dir%"\deploy.cmd" stop %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
        ::redis will invoke another process called redis-server.exe, so we need to call another kill.cmd to clean that process
        if %test_app% EQU "redis" (
            TASKKILL /F /S %replica1_address% /IM redis-server.exe
            TASKKILL /F /S %replica2_address% /IM redis-server.exe
            TASKKILL /F /S %replica3_address% /IM redis-server.exe

        )
        ::CALL %bin_dir%"\deploy.cmd" cleanup %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
        goto:EOF
    )
goto loop







