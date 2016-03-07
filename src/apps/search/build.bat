set cdir=%~dp0
for /f %%f in ('dir /b %cdir%') do (
    cd /d %cdir%
    echo %%f    
    @rmdir /Q /S %cdir%\%%~nf
    dsn.cg.bat %cdir%\%%f csharp %cdir%\%%~nf json single
    cd /d %cdir%\%%~nf
    @mkdir build
    cd build
    cmake ..
    
    
    cd /d %cdir%
    Tron.exe gcs thrift dsn %cdir%\%%f
)

