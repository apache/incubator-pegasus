set cdir=%~dp0
cd /d %cdir%
set gcsargument=
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
    set gcsargument=%gcsargument% %cdir%\%%f
)
echo done
echo %gcsargument%
Tron.exe gcs %gcsargument%

