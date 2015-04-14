@ECHO OFF
FOR /f %%i IN ("%0") DO SET CODEGEN_ROOT=%%~dpi
CALL php -f %CODEGEN_ROOT%\dsn.generate_code.php %1 %2
:EOF
