@echo OFF
mkdir test
copy /Y *.ini .\test
copy /Y *.exe .\test
copy /Y *.pdb .\test
copy /Y *.cmd .\test
copy /Y *.dll .\test

set replica_port=34801
set meta_port=34601
	
SETLOCAL ENABLEDELAYEDEXPANSION

for /l %%x in (1, 1, 20) do call :test %%x
goto :eof

:test
	set x=%~n1
	echo start test instance %x%
	@mkdir test-%x%
	cd test-%x%
	set /a replica_port=!replica_port!+3
	set /a meta_port=!meta_port!+3
	set lcmd=../test/dsn.replication.simple_kv.exe ../test/vconfig.ini -cargs meta_port=!meta_port!;replica_port=!replica_port!
	echo %lcmd%
	start "test-%x%" /LOW %lcmd%
	cd ..

:eof
