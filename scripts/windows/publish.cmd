SET app_name=%1
SET build_dir=%~f2
SET build_type=%3
SET bin_dir=%~dp0
SET monitor_url=%4

IF "%app_name%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 please specify app_name
    GOTO error
)

IF NOT EXIST "%bin_dir%\publish.%app_name%.cmd" (
    CALL %bin_dir%\echoc.exe 4 please create '%bin_dir%\publish.%app_name%.cmd' before publish 
    GOTO error
)

IF NOT EXIST "%build_dir%" (
    CALL %bin_dir%\echoc.exe 4 build dir '%build_dir%' is not specified or does not exist
    GOTO error
)

SET bt_valid=0
IF "%build_type%" EQU "Debug" SET bt_valid=1
IF "%build_type%" EQU "debug" SET bt_valid=1 
IF "%build_type%" EQU "Release"  SET bt_valid=1
IF "%build_type%" EQU "release"  SET bt_valid=1
IF "%build_type%" EQU "RelWithDebInfo" SET bt_valid=1
IF "%build_type%" EQU "relwithdebinfo" SET bt_valid=1
IF "%build_type%" EQU "MinSizeRel" SET bt_valid=1
IF "%build_type%" EQU "minsizerel"  SET bt_valid=1

IF "%bt_valid%" EQU "0" (
    CALL %bin_dir%\echoc.exe 4 invalid build_type '%build_type%'
    GOTO error
)


CALL %bin_dir%\publish.%app_name%.cmd %build_dir% %build_type% %monitor_url%
GOTO exit

:error    
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd publish app_name build_dir build_type(Debug|Release|RelWithDebInfo|MinSizeRel) [monitor_package_url]"

:exit


