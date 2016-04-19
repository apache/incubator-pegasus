SET target_dir=%~f1
SET droot=%DSN_ROOT:/=\%\lib
SET bin_dir=%~dp0
SET build_dir=%~f2
SET build_type=%3

IF "%droot%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 DSN_ROOT not defined
    GOTO error
)

IF NOT EXIST "%target_dir%" (
    CALL %bin_dir%\echoc.exe 4  %target_dir% does not exist
    GOTO error
)

copy /Y %build_dir%\bin\%build_type%\dsn.core.* %target_dir%\
copy /Y %build_dir%\bin\%build_type%\dsn.meta_server.* %target_dir%\
CALL :copy_dll zookeeper_mt %target_dir%%
CALL :copy_dll dsn.dev.python_helper %target_dir%%
GOTO exit

:copy_dll
    IF NOT EXIST "%droot%\%1.dll" (
        CALL %bin_dir%\echoc.exe 4  %droot%\%1.dll not exist, please install rDSN/rDSN.Python properly
        GOTO:EOF
    )
    copy /Y %droot%\%1.* %target_dir%\
    GOTO:EOF

:error    
    ECHO deploy.simple_kv.cmd build_dir build_type(Debug^|Release^|RelWithDebInfo^|MinSizeRel)

:exit


