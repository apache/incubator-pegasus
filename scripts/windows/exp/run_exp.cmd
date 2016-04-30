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
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer1 leveldb|memcached|thumbnail|xlock server_address(eg.srgsi-11) client.perf_address(eg.srgsi-12)"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer2.stateless memcached|thumbnail server_address(eg.srgsi-11) daemon1_address(eg.srgsi-12) daemon2_address(eg.srgsi-13) daemon3_address(eg.srgsi-14) client.perf_address(eg.srgsi-15)"
    CALL %bin_dir%\echoc.exe 4  "Usage: run_exp.cmd layer2.stateful simple_kv|leveldb|xlock|rrdb|redis server_address(eg.srgsi-11) daemon1_address(eg.srgsi-12) daemon2_address(eg.srgsi-13) daemon3_address(eg.srgsi-14) client.perf_address(eg.srgsi-15)"
    GOTO:EOF

:layer1
:layer2.stateless
:layer2.stateful
    CALL %exp_dir%%1.cmd %2 %3 %4 %5 %6 %7 %8 %9
    GOTO:EOF

:exit

