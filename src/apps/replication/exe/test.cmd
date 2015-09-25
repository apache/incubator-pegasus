echo OFF
mkdir test
copy /Y *.ini .\test
copy /Y *.exe .\test
copy /Y *.pdb .\test
copy /Y *.cmd .\test
copy /Y *.dll .\test
	
for /l %%x in (1, 1, 40) do (
	echo start test instance %%x 
	mkdir test-%%x
	cd test-%%x	
	start "test-%%x" /LOW ../test/dsn.replication.simple_kv.exe ../test/config.ini
	cd ..
)
