SET bin_dir=%~dp0
SET TOP_DIR=%bin_dir%\..\..\
SET build_type=%1
SET build_dir=%~f2

IF "%build_type%" EQU "" SET build_type=Debug

IF "%TOP_DIR%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 please specify TOP_DIR
    GOTO error
)

IF "%build_dir%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 please specify build_dir
    GOTO error
)

IF NOT "%VS140COMNTOOLS%"=="" (
    CALL "%VS140COMNTOOLS%\..\..\VC\vcvarsall.bat" amd64
    SET cmake_target=Visual Studio 14 2015 Win64
    SET boost_lib=lib64-msvc-14.0
    SET boost_package_name=boost_1_59_0_vc14_amd64.7z
    SET boost_dir_name=boost_1_59_0
) ELSE (
    IF NOT "%VS120COMNTOOLS%"=="" (
        CALL "%VS120COMNTOOLS%\..\..\VC\vcvarsall.bat" amd64
        SET cmake_target=Visual Studio 12 2013 Win64
        SET boost_lib=lib64-msvc-12.0
        SET boost_package_name=boost_1_57_0_vc12_amd64.7z
        SET boost_dir_name=boost_1_57_0
    )
)

IF "%cmake_target%"=="" (
    ECHO "error: Visusal studio 2013 or 2015 is not installed, please fix and try later"
    GOTO error
)

IF NOT EXIST "%bin_dir%\7z.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.dll?raw=true
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.exe?raw=true
    @move 7z.dll %bin_dir%
    @move 7z.exe %bin_dir%
)
IF NOT EXIST "%TOP_DIR%\ext\%boost_dir_name%" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/%boost_package_name%?raw=true
    CALL %bin_dir%\7z.exe x %boost_package_name% -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%TOP_DIR%\ext\cmake-3.2.2" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/cmake-3.2.2.7z?raw=true
    CALL %bin_dir%\7z.exe x cmake-3.2.2.7z -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%build_dir%" mkdir %build_dir%

cd /d %build_dir%

echo CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\%boost_dir_name%" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\%boost_dir_name%\%boost_lib%" -G "%cmake_target%"
CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\%boost_dir_name%" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\%boost_dir_name%\%boost_lib%" -G "%cmake_target%"

msbuild dsn.sln /p:Configuration=%build_type%
goto exit

:error
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd build build_type(Debug|Release|RelWithDebInfo|MinSizeRel) build_dir"

:exit
