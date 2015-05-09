echo OFF
for /l %%x in (1, 1, 10) do (
	echo start test instance %%x 
	mkdir test-%%x
	copy config.ini .\test-%%x
	cd test-%%x	
	start ..\dsn.replication.simple_kv.exe
	cd ..
)
