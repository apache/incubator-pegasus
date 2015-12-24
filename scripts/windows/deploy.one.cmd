SET cmd=%1
SET src_dir=%2
SET ldst_dir=%3
SET deploy_name=%4
SET rdst_dir=%ldst_dir::=$%
SET machine=%5
TITLE %cmd% %deploy_name% @ %machine%

ECHO %cmd% %machine% ... && CALL :%cmd% %machine%

IF ERRORLEVEL 1 (
    CALL %bin_dir%\echoc.exe 4 unknow command '%cmd%'
    CALL :usage
    GOTO exit
)

GOTO exit

:usage
    ECHO run.cmd deploy^|start^|stop^|cleanup^|quick-cleanup source-dir target-dir
    ECHO  source-dir is a directory which contains a start.cmd, machines.txt, and other resource files/dirs
    GOTO:EOF

REM  
REM |-source-dir|target-dir
REM   - start.cmd
REM   - machines.txt
REM   - other dependent files or dirs
REM 

:deploy
    set machine=%1
    set rdst=\\%machine%\%rdst_dir%
    @mkdir %rdst%    
    xcopy /F /Y /S %src_dir% %rdst%
    COPY /Y %bin_dir%\7z.exe %rdst%
    COPY /Y %bin_dir%\7z.dll %rdst%
    SCHTASKS /CREATE /S %machine% /RU SYSTEM /SC ONLOGON /TN %deploy_name% /TR "%ldst_dir%\start.cmd" /V1 /F
    GOTO:EOF

:start
    @SCHTASKS /RUN /S %1 /TN %deploy_name%
    GOTO:EOF
    
:stop
    @SCHTASKS /END /S %1 /TN %deploy_name%
    GOTO:EOF

:quick-cleanup
    SCHTASKS /Delete /S %1 /TN %deploy_name% /F
    set rdst=\\%1\%rdst_dir%
    @rmdir /Q /S %rdst%\data
    GOTO:EO

:cleanup
    SCHTASKS /Delete /S %1 /TN %deploy_name% /F
    set rdst=\\%1\%rdst_dir%
    @rmdir /Q /S %rdst%
    GOTO:EOF
    
:exit

ECHO auto exit after 20 seconds ...
ping -n 21 127.0.0.1 >nul

