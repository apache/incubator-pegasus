SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET server_address=%2
SET clientperf_address=%3
if "%1" EQU "memcached" SET test_app_upper=MemCached
if "%1" EQU "thumbnail" SET test_app_upper=ThumbnailServe
if "%1" EQU "xlock" SET test_app_upper=XLock
if "%1" EQU "leveldb" SET test_app_upper=LevelDb

if "%test_app_upper%" EQU "" (
    CALL %bin_dir%\echoc.exe 4  no such app %1 for perf test, please check spelling
    GOTO:EOF
)


IF NOT EXIST "%bin_root%\dsn.core.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.core.dll not exist, please copy it from DSN_ROOT repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.apps.%test_app_upper%.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.apps.%test_app_upper%.dll not exist, please copy it from bondex repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.svchost.exe" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.svchost.exe not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

@mkdir "%exp_dir%layer1_test"
@rmdir /Q /S "%exp_dir%layer1_test\%test_app_upper%"
@mkdir "%exp_dir%layer1_test\%test_app_upper%"
@mkdir "%exp_dir%layer1_test\%test_app_upper%\log"
@mkdir "%exp_dir%layer1_test\%test_app_upper%\server"
@mkdir "%exp_dir%layer1_test\%test_app_upper%\client.perf"

(
    ECHO server
    ECHO client.perf
)  > %exp_dir%layer1_test\%test_app_upper%\apps.txt
(
    ECHO %server_address%
)  > %exp_dir%layer1_test\%test_app_upper%\server\machines.txt
(
    ECHO %clientperf_address%
)  > %exp_dir%layer1_test\%test_app_upper%\client.perf\machines.txt

copy /Y %exp_dir%\config.ini %exp_dir%layer1_test\%test_app_upper%

(
    ECHO[
    ECHO [apps.server]
    ECHO name = server
    ECHO type = server
    ECHO arguments = 
    ECHO ports = 33001
    ECHO run = true
    ECHO pools = THREAD_POOL_DEFAULT
    ECHO dmodule = dsn.apps.%test_app_upper%
    ECHO[   
    ECHO [apps.client.perf.%test_app%] 
    ECHO name = client.perf.%test_app% 
    ECHO type = client.perf.%test_app% 
    ECHO arguments = %server_address%:33001 
    ECHO count = 1
    ECHO run = true
    ECHO pools = THREAD_POOL_DEFAULT
    ECHO delay_seconds = 1
    ECHO dmodule = dsn.apps.%test_app_upper%
    ECHO[ 
    ECHO [apps.client.perf.test]
    ECHO type = client.perf.%test_app% 
    ECHO exit_after_test = true
)  >> %exp_dir%layer1_test\%test_app_upper%\config.ini

copy /Y %exp_dir%layer1_test\%test_app_upper%\config.ini %exp_dir%layer1_test\%test_app_upper%\server
copy /Y %exp_dir%layer1_test\%test_app_upper%\config.ini %exp_dir%layer1_test\%test_app_upper%\client.perf

copy /Y %bin_root%\*.* %exp_dir%layer1_test\%test_app_upper%\server
copy /Y %bin_root%\*.* %exp_dir%layer1_test\%test_app_upper%\client.perf

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list server
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %exp_dir%layer1_test\%test_app_upper%\server\start.cmd

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list client.perf.%test_app%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %exp_dir%layer1_test\%test_app_upper%\client.perf\start.cmd

CALL %bin_dir%"\deploy.cmd" deploy %exp_dir%layer1_test\%test_app_upper% d:\v-chlou
ECHO Wait for deployment finish, please continue after that
PAUSE

CALL %bin_dir%"\deploy.cmd" start %exp_dir%layer1_test\%test_app_upper% d:\v-chlou
:loop
    if exist \\%clientperf_address%\D$\v-chlou\client\data\client.perf.%test_app%\*.txt (
        xcopy  /F /Y /S \\%clientperf_address%\D$\v-chlou\client\data\client.perf.%test_app%\*.* %exp_dir%layer1_test\%test_app_upper%\log
        CALL %bin_dir%"\deploy.cmd" stop %exp_dir%layer1_test\%test_app_upper% d:\v-chlou
        CALL %bin_dir%"\deploy.cmd" cleanup %exp_dir%layer1_test\%test_app_upper% d:\v-chlou
        goto:EOF
    )
goto loop






