SET bin_dir=%~dp0
SET TOP_DIR=%bin_dir%\..\..\
SET build_type=%1
SET build_dir=%~f2
SET install=%3

CALL %bin_dir%\pre-require.cmd

IF "%build_type%" EQU "" SET build_type=Debug

IF "%build_dir%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 please specify build_dir
    GOTO error
)

IF NOT "%VS140COMNTOOLS%"=="" (
    SET cmake_target=Visual Studio 14 2015 Win64
    SET boost_lib=lib64-msvc-14.0
    SET boost_package_name=boost_1_59_0_vc14_amd64.7z
    SET boost_dir_name=boost_1_59_0
)    

IF "%cmake_target%"=="" (
    ECHO "error: Visusal studio 2013 or 2015 is not installed, please fix and try later"
    GOTO error
)

IF NOT EXIST "%build_dir%" mkdir %build_dir%

pushd %build_dir%

echo CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\%boost_dir_name%" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\%boost_dir_name%\%boost_lib%" -DDSN_GIT_SOURCE="github" -G "%cmake_target%"
CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\%boost_dir_name%" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\%boost_dir_name%\%boost_lib%" -G "%cmake_target%"

msbuild dsn.sln /p:Configuration=%build_type%
IF "%install%" NEQ "" (
    msbuild INSTALL.vcxproj /p:Configuration=%build_type%
)

popd
goto exit

:error
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd build build_type(Debug|Release|RelWithDebInfo|MinSizeRel) build_dir [install]"

:exit
