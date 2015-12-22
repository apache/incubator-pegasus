cd /d %~dp0
set i=0

:loop
    set /a i=%i%+1
    echo i'm running %i% >> ./1.txt
    ping -n 6 127.0.0.1 >nul
goto loop

