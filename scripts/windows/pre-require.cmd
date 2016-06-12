SET bin_dir=%~dp0
SET TOP_DIR=%bin_dir%\..\..\
SET has_cl=false

cl >nul 2>&1 && SET has_cl=true

IF "%has_cl%" NEQ "true" (
    IF NOT "%VS140COMNTOOLS%"=="" (
        CALL "%VS140COMNTOOLS%\..\..\VC\vcvarsall.bat" amd64
    )  
)

IF NOT "%VS140COMNTOOLS%"=="" (
    SET cmake_target=Visual Studio 14 2015 Win64
    SET boost_lib=lib64-msvc-14.0
    SET boost_package_name=boost_1_59_0_vc14_amd64.7z
    SET boost_dir_name=boost_1_59_0
)    

IF "%cmake_target%"=="" (
    ECHO "error: Visusal studio 2015 is not installed, please fix and try later"
    GOTO error
)

IF NOT EXIST "%bin_dir%\sed.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/sed.exe?raw=true
    @move sed.exe %bin_dir%
)

IF NOT EXIST "%TOP_DIR%\ext\%boost_dir_name%" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/%boost_package_name%?raw=true
    CALL %bin_dir%\7z.exe x %boost_package_name% -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%TOP_DIR%\ext\cmake-3.2.2" (
    CALL %bin_dir%\wget.exe --no-check-certificate http://github.com/imzhenyu/packages/blob/master/windows/cmake-3.2.2.7z?raw=true
    CALL %bin_dir%\7z.exe x cmake-3.2.2.7z -y -o"%TOP_DIR%\ext\"
)

IF NOT EXIST "%TOP_DIR%\bin\Windows\thrift.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/thrift/raw/master/pre-built/windows8.1/thrift.exe
    @move thrift.exe %TOP_DIR%\bin\Windows\
)


IF NOT EXIST "%TOP_DIR%\bin\Windows\7z.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.dll?raw=true
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/7z.exe?raw=true
    @copy /Y 7z.dll %TOP_DIR%\bin\Windows\
    @copy /Y 7z.exe %TOP_DIR%\bin\Windows\
    @move 7z.dll %bin_dir%
    @move 7z.exe %bin_dir%
)

IF NOT EXIST "%TOP_DIR%\bin\Windows\php.exe" (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/php5.dll?raw=true
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/php.exe?raw=true
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/imzhenyu/packages/raw/master/windows/php.ini?raw=true
    @move php5.dll %TOP_DIR%\bin\Windows\
    @move php.exe %TOP_DIR%\bin\Windows\
    @move php.ini %TOP_DIR%\bin\Windows\
)
