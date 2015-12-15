#include <windows.h>
#pragma comment(lib, "user32.lib")

int main(int argc, char* argv[])
{
    DWORD_PTR dwReturnValue;
    SendMessageTimeoutA(HWND_BROADCAST, WM_SETTINGCHANGE, 0, (LPARAM)("Environment"), SMTO_ABORTIFHUNG, 5000, &dwReturnValue);
    return 0;
}
