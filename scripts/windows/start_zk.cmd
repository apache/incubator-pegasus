SET bin_dir=%~dp0
SET INSTALL_DIR=%~f1
SET PORT=%2
SET zk=zookeeper-3.4.6

IF "%INSTALL_DIR%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 INSTALL_DIR not specified
    CALL :usage
    GOTO exit
)

IF "%PORT%" EQU "" (
    CALL %bin_dir%\echoc.exe 4 PORT not specified
    CALL :usage
    GOTO exit
)


CALL %bin_dir%\pre-require.cmd

IF NOT EXIST %INSTALL_DIR% mkdir %INSTALL_DIR%


pushd %INSTALL_DIR%

IF NOT EXIST %INSTALL_DIR%\%zk% (
    CALL %bin_dir%\wget.exe --no-check-certificate https://github.com/shengofsun/packages/raw/master/%zk%.tar.gz?raw=true
    IF NOT EXIST %zk%.tar.gz (
        CALL %bin_dir%\echoc.exe 4 download zookeeper package failed from  https://github.com/shengofsun/packages/raw/master/%zk%.tar.gz?raw=true
        popd
        GOTO exit
    )    
    CALL %bin_dir%\7z.exe x %zk%.tar.gz -y -o"%INSTALL_DIR%"
    CALL %bin_dir%\7z.exe x %zk%.tar -y -o"%INSTALL_DIR%"
)

SET ZOOKEEPER_HOME=%INSTALL_DIR%\%zk%
SET ZOOKEEPER_PORT=%PORT%

copy /Y %ZOOKEEPER_HOME%\conf\zoo_sample.cfg %ZOOKEEPER_HOME%\conf\zoo.cfg
CALL %bin_dir%\sed.exe -i "s@dataDir=/tmp/zookeeper@dataDir=%ZOOKEEPER_HOME%\data@" %ZOOKEEPER_HOME%\conf\zoo.cfg
CALL %bin_dir%\sed.exe -i "s@clientPort=2181@clientPort=%ZOOKEEPER_PORT%@" %ZOOKEEPER_HOME%\conf\zoo.cfg

@mkdir %ZOOKEEPER_HOME%\data
CALL start cmd.exe /k "title zk-%PORT%&& %ZOOKEEPER_HOME%\bin\zkServer.cmd"

popd

GOTO exit


:usage
    ECHO run.cmd start_zk INSTALL_DIR PORT
    GOTO:EOF
    
:exit
