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

IF NOT EXIST "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" (
    CALL %bin_dir%\echoc.exe 4 Visusal studio 2013 is not installed, please fix and try later
    GOTO exit
)

CALL "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" amd64

IF NOT EXIST "%bin_dir%\7z.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.dll?raw=true
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.exe?raw=true
    @mv 7z.dll %bin_dir%\7z.dll
    @mv 7z.exe %bin_dir%\7z.exe
)

IF NOT EXIST "%TOP_DIR%\ext\boost_1_57_0" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/boost_1_57_0.7z?raw=true
    CALL %bin_dir%\7z.exe x boost_1_57_0.7z -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%TOP_DIR%\ext\cmake-3.2.2" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/cmake-3.2.2.7z?raw=true
    CALL %bin_dir%\7z.exe x cmake-3.2.2.7z -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%build_dir%" mkdir %build_dir%

cd /d %build_dir%

echo CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\boost_1_57_0" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\boost_1_57_0\lib64-msvc-12.0" -G "Visual Studio 12 2013 Win64"
CALL %TOP_DIR%\ext\cmake-3.2.2\bin\cmake.exe .. -DCMAKE_BUILD_TYPE="%build_type%" -DBOOST_INCLUDEDIR="%TOP_DIR%\ext\boost_1_57_0" -DBOOST_LIBRARYDIR="%TOP_DIR%\ext\boost_1_57_0\lib64-msvc-12.0" -G "Visual Studio 12 2013 Win64"

msbuild dsn.sln /p:Configuration=%build_type%
goto exit

:error
    CALL %bin_dir%\echoc.exe 4  "Usage: run.cmd build build_type(Debug|Release|RelWithDebInfo|MinSizeRel) build_dir"
    
:exit

