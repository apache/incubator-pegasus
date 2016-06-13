@ECHO OFF
SET TOP_DIR="%~dp0\..\..\.."
SET exp_dir=%~dp0
SET bin_dir=%TOP_DIR%"\scripts\windows"
SET old_dsn_root=%DSN_ROOT%

if "%1" EQU "" GOTO usage


IF NOT EXIST "%exp_dir%\setting.ini" (
    CALL %bin_dir%\echoc.exe 4  %exp_dir%\setting.ini not exist, please check
    GOTO:EOF
)

for /f "delims=" %%x in (setting.ini) do (set "%%x")

:main
    CALL :%1 %1 %2 %3 %4 %5 %6 %7 %8 %9

    IF ERRORLEVEL 1 (
        CALL %bin_dir%\echoc.exe 4 unknown test '%1'
        CALL :usage
        GOTO exit
    )
    GOTO:EOF

:usage
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd <test_type> <app> <cluster>"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer1 leveldb|memcached|thumbnail|xlock|kyotocabinet <cluster>"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer2.stateless memcached|thumbnail <cluster>"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer2.stateful simple_kv|leveldb|xlock|redis <cluster>"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd toollets <cluster>"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd all <cluster>"
    GOTO:EOF

:layer1
:layer2.stateless
:layer2.stateful
    CALL %exp_dir%%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF

:toollets
    CALL %bin_dir%\echoc.exe 12 *****TEST [TOOLLETS_TEST] BEGIN***** 

    for %%x in (leveldb memcached thumbnail xlock kyotocabinet redis) do (
    ::for %%x in (redis) do (
        for %%y in (bare tracer profiler fault_injector all) do (
            CALL :layer1 layer1 %%x %2 auto %%y
        )
    )
    CALL %bin_dir%\echoc.exe 12 *****TEST [TOOLLETS_TEST] END***** 

    GOTO toollets

:all
    CALL %bin_dir%\echoc.exe 12 *****TEST [ALL_TEST] BEGIN***** 

    ::for %%x in (leveldb memcached thumbnail xlock kyotocabinet redis) do (
    ::    CALL :layer1 layer1 %%x %2 auto
    ::)
    ::for %%x in (memcached thumbnail) do (
    ::    CALL :layer2.stateless layer2.stateless %%x %2 auto
    ::)
    for %%x in (leveldb xlock kyotocabinet redis) do (
        CALL :layer2.stateful layer2.stateful %%x %2 auto
    )

    CALL %bin_dir%\echoc.exe 12 *****TEST [ALL_TEST] END***** 

    GOTO all
:exit

