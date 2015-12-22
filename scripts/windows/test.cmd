SET bin_dir=%~dp0
SET TOP_DIR=%bin_dir%\..\..\
SET build_type=%1
SET build_dir=%~f2

IF "%build_type%" EQU "" SET build_type=Debug

IF "%build_dir%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 please specify build_dir
    GOTO error
)

IF NOT EXIST "%build_dir%" (
    CALL %bin_dir%\echoc.exe 4 %build_dir% does not exist
    GOTO error
)

pushd %build_dir%

CALL  %bin_dir%\echoc.exe 2 run the test cases here ...

cd bin\dsn.core.tests

CALL  %bin_dir%\echoc.exe 2  %build_type%\dsn.core.tests.exe config-test.ini
%build_type%\dsn.core.tests.exe config-test.ini
@CALL clear.cmd

CALL  %bin_dir%\echoc.exe 2  %build_type%\dsn.core.tests.exe config-test-2.ini
%build_type%\dsn.core.tests.exe config-test-2.ini
@CALL clear.cmd

CALL  %bin_dir%\echoc.exe 2  %build_type%\dsn.core.tests.exe config-test-sim.ini
%build_type%\dsn.core.tests.exe config-test-sim.ini
@CALL clear.cmd

cd ..\dsn.tests

::CALL  %bin_dir%\echoc.exe 2  %build_type%\dsn.tests.exe config-test.ini
::%build_type%\dsn.tests.exe config-test.ini
::@CALL clear.cmd

popd

goto exit

:error
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd test build_type(Debug|Release|RelWithDebInfo|MinSizeRel) build_dir"

:exit
