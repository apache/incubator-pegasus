SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET meta_address=%2
SET daemon1_address=%3
SET daemon2_address=%4
SET daemon3_address=%5
SET clientperf_address=%6

if "%1" EQU "memcached" SET test_app_upper=MemCached
if "%1" EQU "thumbnail" SET test_app_upper=ThumbnailServe

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

IF NOT EXIST "%bin_root%\dsn.meta_server.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.meta_server.dll not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

IF NOT EXIST "%bin_root%\dsn.layer2.stateless.dll" (
    CALL %bin_dir%\echoc.exe 4  %bin_root%\dsn.layer2.stateless.dll not exist, please copy it from rdsn builder repo
    GOTO:EOF
)

@mkdir "%exp_dir%layer2_test"
@rmdir /Q /S "%exp_dir%layer2_test\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\log"
@mkdir "%exp_dir%layer2_test\log\%test_app_upper%"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\meta"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\meta\packages"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\daemon"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\client.perf"
@mkdir "%exp_dir%layer2_test\%test_app_upper%\%test_app%"

(
    ECHO meta
    ECHO daemon
    ECHO client.perf
)  > %exp_dir%layer2_test\%test_app_upper%\apps.txt
(
    ECHO %meta_address%
)  > %exp_dir%layer2_test\%test_app_upper%\meta\machines.txt
(
    ECHO %daemon1_address%
    ECHO %daemon2_address%
    ECHO %daemon3_address%
)  > %exp_dir%layer2_test\%test_app_upper%\daemon\machines.txt
(
    ECHO %clientperf_address%
)  > %exp_dir%layer2_test\%test_app_upper%\client.perf\machines.txt

copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%

copy /Y %exp_dir%layer2_test\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\%test_app%
(
    ECHO [apps.server]
    ECHO name = server
    ECHO type = server
    ECHO arguments = 
    ECHO ports = %%port%%
    ECHO run = true
    ECHO pools = THREAD_POOL_DEFAULT
    ECHO dmodule = dsn.apps.%test_app_upper%
)  >> %exp_dir%layer2_test\%test_app_upper%\%test_app%\config.ini

(
ECHO echo port = %%port%% ^>^> run.txt
ECHO echo package_dir = %%package_dir%%^>^> run.txt
ECHO[ 
ECHO CALL %%package_dir%%\dsn.svchost.exe %%package_dir%%\config.ini -cargs meta_address=%meta_address%;port=%%port%% -app_list server  ^>^> run.txt
)  > %exp_dir%layer2_test\%test_app_upper%\%test_app%\run.cmd

copy /Y %exp_dir%layer2_test\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\meta
copy /Y %exp_dir%layer2_test\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\daemon
copy /Y %exp_dir%layer2_test\%test_app_upper%\config.ini %exp_dir%layer2_test\%test_app_upper%\client.perf

copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\%test_app%
copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\meta
copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\daemon
copy /Y %bin_root%\*.* %exp_dir%layer2_test\%test_app_upper%\client.perf

CALL %bin_dir%\7z.exe a -y %exp_dir%layer2_test\%test_app_upper%\meta\packages\%test_app%.7z %exp_dir%layer2_test\%test_app_upper%\%test_app%\

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
    ECHO     .\dsn.svchost.exe config.ini -app_list daemon -cargs meta_address=%meta_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %exp_dir%layer2_test\%test_app_upper%\daemon\start.cmd

(
    ECHO SET ldir=%%~dp0
    ECHO cd /d ldir
    ECHO set i=0
    ECHO :loop
    ECHO     set /a i=%%i%%+1
    ECHO     echo run %%i%%th ... ^>^> ./running.txt
    ECHO     .\dsn.svchost.exe config.ini -app_list client.perf.%test_app% -cargs meta_address=%meta_address%
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
    if exist \\%clientperf_address%\D$\v-chlou\client\data\client.perf.%test_app%\*.txt (
        xcopy  /F /Y /S \\%clientperf_address%\D$\v-chlou\client\data\client.perf.%test_app%\*.* %exp_dir%layer2_test\log\%test_app_upper%
        CALL %bin_dir%"\deploy.cmd" stop %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
        CALL %bin_dir%"\deploy.cmd" cleanup %exp_dir%layer2_test\%test_app_upper% d:\v-chlou
        goto:EOF
    )
goto loop







