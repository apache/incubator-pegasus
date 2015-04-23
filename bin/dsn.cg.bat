@ECHO OFF
FOR /f %%i IN ("%0") DO SET CODEGEN_ROOT=%%~dpi
CALL php -f %CODEGEN_ROOT%\dsn.generate_code.php %1 %2 %3 %4 %5 %6 %7 %8 %9 
:EOF
