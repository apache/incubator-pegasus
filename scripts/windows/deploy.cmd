SET cmd=%1
SET deploy_name=%2
SET src_dir=%~f3
SET ldst_dir=%4
SET rdst_dir=%ldst_dir::=$%
SET machine_list=%5

IF "%src_dir%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 source directory not specified
    CALL :usage
    GOTO exit
)

IF NOT EXIST "%src_dir%" (
    CALL %bin_dir%\echoc.exe 4 cannot find source directory '%src_dir%'
    CALL :usage
    GOTO exit
)

IF NOT EXIST "%src_dir%\start.cmd" (
    CALL %bin_dir%\echoc.exe 4 please create start.cmd in source directory '%src_dir%'
    GOTO exit
)

IF "%machine_list%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 machine list file not specified
    CALL :usage
    GOTO exit
)

IF NOT EXIST "%machine_list%" (
    CALL %bin_dir%\echoc.exe 4 cannot find machine_list file '%machine_list%'
    CALL :usage
    GOTO exit
)

FOR /F %%i IN (%machine_list%) DO ECHO %cmd% %%i ... && CALL :%cmd% %%i %6 %7 %8 %9    

IF ERRORLEVEL 1 (
    CALL %bin_dir%\echoc.exe 4 unknow command '%cmd%'
    CALL :usage
    GOTO exit
)

GOTO exit

:usage
    ECHO run.cmd deploy^|start^|stop^|cleanup deploy-name source-dir target-dir machine-list
    ECHO  source-dir is a directory which contains a start.cmd and other resource files/dirs
    GOTO:EOF

REM  
REM |-source-dir|target-dir
REM   - start.cmd
REM   - other dependent files or dirs
REM 

:deploy
    set machine=%1
    set rdst=\\%machine%\%rdst_dir%
    @mkdir %rdst%
    CALL %bin_dir%\7z.exe a send.7z %src_dir%
    mkdir send
    move send.7z send
    COPY /Y %bin_dir%\7z.exe send
    COPY /Y %bin_dir%\7z.dll send
    (
        ECHO cd /d %%~dp0
        ECHO CALL .\7z.exe x -y send.7z
        ECHO del send.7z
        ECHO del 7z.exe
        ECHO del 7z.dll
        ECHO del unzip.cmd
    )  > .\send\unzip.cmd
    xcopy /F /Y /S send %rdst%\..
    SCHTASKS /CREATE /S %machine% /RU SYSTEM /SC ONLOGON /TN unzip /TR "%ldst_dir%\..\unzip.cmd" /V1 /F
    @SCHTASKS /RUN /S %1 /TN unzip
    SCHTASKS /Delete /S %1 /TN unzip /F
    SCHTASKS /CREATE /S %machine% /RU SYSTEM /SC ONLOGON /TN %deploy_name% /TR "%ldst_dir%\..\start.cmd" /V1 /F
    GOTO:EOF

:start
    @SCHTASKS /RUN /S %1 /TN %deploy_name%
    GOTO:EOF
    
:stop
    @SCHTASKS /END /S %1 /TN %deploy_name%
    GOTO:EOF

:cleanup
    SCHTASKS /Delete /S %1 /TN %deploy_name% /F
    set rdst=\\%1\%rdst_dir%
    @rmdir /Q /S %rdst%
    GOTO:EOF
    
:exit

