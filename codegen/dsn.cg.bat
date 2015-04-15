@ECHO OFF
FOR /f %%i IN ("%0") DO SET CODEGEN_ROOT=%%~dpi
CALL php -f %CODEGEN_ROOT%\generate_code.php %1 %2
:EOF
