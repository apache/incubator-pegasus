SET cmd=%1
SET build_dir=%~f2
SET build_type=%3
SET webstudio_url=%4
SET bin_dir=%~dp0

:start_build
CALL :build_app meta
CALL :build_app replica
CALL :build_app client
CALL :build_app client.perf.test

(
    ECHO meta
    ECHO replica
    ECHO client
    ECHO client.perf.test
)  > .\skv\apps.txt

GOTO exit

:build_app
    set app=%1
    @MKDIR .\skv\%app%

    IF "%webstudio_url%" NEQ "" (
        SET webstudio_str=;webstudio
        COPY /Y %webstudio_url% .\skv\%app%\WebStudio.7z
    )
    
    CALL %bin_dir%\copy_dsn_shared.cmd .\skv\%app% %build_dir% %build_type%
    COPY /Y %build_dir%\bin\dsn.replication.simple_kv\%build_type%\dsn.replication.simple_kv.* .\skv\%app%
    if "%cmd%" NEQ "republish" (
        COPY /Y %build_dir%\bin\dsn.replication.simple_kv\config.ini .\skv\%app%
    )

    SET has_webstudio=
    FOR /F "tokens=* USEBACKQ" %%F IN (`findstr apps.webstudio .\skv\%app%\config.ini`) DO (
        SET has_webstudio=%%F
    )
    
    IF "%webstudio_url%" NEQ "" IF "%has_webstudio%" EQU "" (
        ECHO rewrite config.ini for starting embedded webstudio ...
        (
            ECHO[
            ECHO [apps.webstudio]
            ECHO type = webstudio
            ECHO arguments = 8088
            ECHO pools = THREAD_POOL_DEFAULT
            ECHO dmodule = dsn.dev.python_helper
            ECHO dmodule_bridge_arguments = rDSN.WebStudio\rDSN.WebStudio.py
        ) >> .\skv\%app%\config.ini
    )
 
    ECHO write start.cmd ...
    (
        ECHO SET ldir=%%~dp0
        ECHO cd /d ldir
        ECHO IF NOT EXIST "rDSN.WebStudio" (
        ECHO    CALL .\7z.exe x -y WebStudio.7z 
        ECHO    XCOPY /Y /E /I WebStudio\* .\
        ECHO    rmdir /s /q WebStudio
        ECHO ^)
        ECHO set i=0
        ECHO :loop
        ECHO     set /a i=%%i%%+1
        ECHO     echo run %%i%%th ... ^>^> ./running.txt
        ECHO     .\dsn.replication.simple_kv.exe config.ini -app_list %app%@1%webstudio_str%
        ECHO     ping -n 16 127.0.0.1 ^>nul
        ECHO goto loop
    )  > .\skv\%app%\start.cmd

    GOTO:EOF

:error
    CALL %bin_dir%\echoc.exe 4  Usage: run publish^|republish build_dir build_type(Debug^|Release^|RelWithDebInfo^|MinSizeRel) [webstudio_package_url]

:exit
