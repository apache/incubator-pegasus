set cdir=%~dp0
cd /d %cdir%
for /f %%f in ('dir *.thrift /b') do (
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

