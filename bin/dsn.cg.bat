@ECHO OFF
SET CODEGEN_ROOT=%~dp0
CALL php -f %CODEGEN_ROOT%\dsn.generate_code.php %1 %2 %3 %4 %5 %6 %7 %8 %9 
:EOF
