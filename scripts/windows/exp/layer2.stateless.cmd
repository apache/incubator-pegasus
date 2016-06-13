SET exp_dir=%~dp0
SET bin_root=%~dp0bin
SET test_app=%1
SET cluster=%2
SET ifauto=%3

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

IF NOT EXIST "%exp_dir%cluster\%cluster%.txt" (
    CALL %bin_dir%\echoc.exe 4  %exp_dir%cluster\%cluster%.txt not exist, please see the detailed instructions in the cluster folder
    GOTO:EOF
)

< %exp_dir%cluster\%cluster%.txt (
    set /p meta_address=
    set /p daemon1_address=
    set /p daemon2_address=
    set /p daemon3_address=
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
@mkdir "%app_dir%\meta\packages"
@mkdir "%app_dir%\daemon"
@mkdir "%app_dir%\client.perf"
@mkdir "%app_dir%\%test_app%"

(
    ECHO meta
    ECHO daemon
    ECHO client.perf
)  > %app_dir%\apps.txt
(
    ECHO %meta_address%
)  > %app_dir%\meta\machines.txt
(
    ECHO %daemon1_address%
    ECHO %daemon2_address%
    ECHO %daemon3_address%
)  > %app_dir%\daemon\machines.txt
(
    ECHO %clientperf_address%
)  > %app_dir%\client.perf\machines.txt

copy /Y %exp_dir%config\layer2\%test_app_upper%\config.ini %app_dir%

copy /Y %app_dir%\config.ini %app_dir%\%test_app%
(
    ECHO [apps.server]
    ECHO name = server
    ECHO type = server
    ECHO arguments = 
    ECHO ports = %%port%%
    ECHO run = true
    ECHO pools = THREAD_POOL_DEFAULT
    ECHO dmodule = dsn.apps.%test_app_upper%
)  >> %app_dir%\%test_app%\config.ini

(
ECHO echo port = %%port%% ^>^> run.txt
ECHO echo package_dir = %%package_dir%%^>^> run.txt
ECHO[ 
ECHO CALL %%package_dir%%\dsn.svchost.exe %%package_dir%%\config.ini -cargs meta_address=%meta_address%;port=%%port%% -app_list server  ^>^> run.txt
)  > %app_dir%\%test_app%\run.cmd

copy /Y %app_dir%\config.ini %app_dir%\meta
copy /Y %app_dir%\config.ini %app_dir%\daemon
copy /Y %app_dir%\config.ini %app_dir%\client.perf

copy /Y %bin_root%\*.* %app_dir%\%test_app%
copy /Y %bin_root%\*.* %app_dir%\meta
copy /Y %bin_root%\*.* %app_dir%\daemon
copy /Y %bin_root%\*.* %app_dir%\client.perf

CALL %bin_dir%\7z.exe a -y %app_dir%\meta\packages\%test_app%.7z %app_dir%\%test_app%\

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
    ECHO     .\dsn.svchost.exe config.ini -app_list daemon -cargs meta_address=%meta_address%
    ECHO     ping -n 16 127.0.0.1 ^>nul
    ECHO goto loop
)  > %app_dir%\daemon\start.cmd

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
)  > %app_dir%\client.perf\start.cmd

CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER2.STATELESS] [%test_app_upper%] BEGIN***** 

CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l2stateless_stop_and_cleanup_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)
CALL %bin_dir%"\deploy.cmd" deploy %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****DEPOLYING...***** 

if "%ifauto%" EQU "auto" (
    ping -n %l2stateless_deploy_wait_duration% 127.0.0.1
) else (
    CALL %bin_dir%\echoc.exe 3 *****PRESS ENTER AFTER DONE***** 
    PAUSE
)
CALL %bin_dir%"\deploy.cmd" start %app_dir% %local_path%
CALL %bin_dir%\echoc.exe 3 *****STARTING...***** 
 
set /a counter=0
CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING LOG IN ROUND***** 
:loop
    ping -n %l2stateless_fetch_wait_duration% 127.0.0.1
    if exist \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.txt (
        xcopy  /F /Y /S \\%clientperf_address%\%remote_path%\client\data\client.perf.%test_app%\*.* %exp_dir%log\layer2\%test_app_upper%\
        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER2.STATELESS] [%test_app_upper%] SUCCESS***** 
        CALL %bin_dir%\echoc.exe 3 *****LOG AND CONFIG SAVDED IN %exp_dir%log\layer2\%test_app_upper%\***** 
        CALL %bin_dir%"\deploy.cmd" stop %app_dir% %local_path%
        CALL %bin_dir%\echoc.exe 3 *****STOPING AND CLEANUPING...***** 
        ::CALL %bin_dir%"\deploy.cmd" cleanup %app_dir% %local_path%
        goto end
    )
    set /a counter=%counter%+1
    CALL %bin_dir%\echoc.exe 3 *****TRY FETCHING FOR TIME %counter%***** 

    if "%counter%" == "%l2stateless_try_fetch_log_times%" (
        CALL %bin_dir%\echoc.exe 2 *****TEST [LAYER2.STATELESS] [%test_app_upper%] FAIL***** 
        GOTO end
    )
goto loop

:end
    CALL %bin_dir%\echoc.exe 3 *****TEST [LAYER2.STATELESS] [%test_app_upper%] END***** 







