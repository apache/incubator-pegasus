@ECHO OFF
SET MOCK_ROOT=%~f1
SET CMAKE_PATH=%~f2
SET DSN_ROOT=%MOCK_ROOT%\rdsn
SET PATH=%CMAKE_PATH%;%DSN_ROOT%\lib;%DSN_ROOT%\bin;%PATH%
CALL  %bin_dir%\echoc.exe 2  %build_type%\dsn.idl.tests.exe %MOCK_ROOT%
CALL  %build_type%\dsn.idl.tests.exe %DSN_ROOT% %MOCK_ROOT%