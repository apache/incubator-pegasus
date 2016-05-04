@ECHO OFF
SET TOP_DIR="%~dp0\..\..\.."
SET exp_dir=%~dp0
SET bin_dir=%TOP_DIR%"\scripts\windows"
SET old_dsn_root=%DSN_ROOT%
if "%1" EQU "" GOTO usage

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
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer2.stateful simple_kv|leveldb|xlock|rrdb|redis <cluster>"
    GOTO:EOF

:layer1
:layer2.stateless
:layer2.stateful
    CALL %exp_dir%%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF

:exit

