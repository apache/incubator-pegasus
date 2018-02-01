/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool_api.h>
#include "coredump.h"

#ifdef _WIN32

#include <Windows.h>
#include <DbgHelp.h>
#include <cstdlib>
#include <ctime>
#include <PsApi.h>
#pragma comment(lib, "PsApi.lib")

namespace dsn {
namespace utils {

static std::string s_dump_dir;
static char s_app_name[256] = "unknown";

static LONG WINAPI TopLevelFilter(struct _EXCEPTION_POINTERS *pExceptionInfo);

void coredump::init(const char *dump_dir)
{
    s_dump_dir = dump_dir;

    ::GetModuleBaseNameA(::GetCurrentProcess(), ::GetModuleHandleA(NULL), s_app_name, 256);

    ::SetUnhandledExceptionFilter(TopLevelFilter);
}

void coredump::write() { TopLevelFilter(0); }

typedef BOOL(WINAPI *MINIDUMPWRITEDUMP)(HANDLE hProcess,
                                        DWORD dwPid,
                                        HANDLE fh,
                                        MINIDUMP_TYPE DumpType,
                                        CONST PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
                                        CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
                                        CONST PMINIDUMP_CALLBACK_INFORMATION CallbackParam);

static LONG WINAPI TopLevelFilter(struct _EXCEPTION_POINTERS *pExceptionInfo)
{
    LONG retval = EXCEPTION_CONTINUE_SEARCH;
    HWND hParent = NULL; // find a better value for your app

    // firstly see if dbghelp.dll is around and has the function we need
    // look next to the EXE first, as the one in System32 might be old
    // (e.g. Windows 2000)
    HMODULE hDll = NULL;

    if (hDll == NULL) {
        // load any version we can
        hDll = ::LoadLibraryA("DBGHELP.DLL");
    }

    LPCSTR szResult = "core dump success";
    char szDumpPath[512];
    char szScratch[512];

    dfatal("fatal exception, core dump started ...");

    if (hDll) {
        MINIDUMPWRITEDUMP pDump = (MINIDUMPWRITEDUMP)::GetProcAddress(hDll, "MiniDumpWriteDump");
        if (pDump) {
            sprintf(szDumpPath,
                    "%s\\%s_%d_%" PRId64 ".dmp",
                    s_dump_dir.c_str(),
                    s_app_name,
                    ::GetCurrentProcessId(),
                    (int64_t)time(NULL));

            // create the file
            HANDLE fh = ::CreateFileA(szDumpPath,
                                      GENERIC_WRITE,
                                      FILE_SHARE_WRITE,
                                      NULL,
                                      CREATE_ALWAYS,
                                      FILE_ATTRIBUTE_NORMAL,
                                      NULL);

            if (fh != INVALID_HANDLE_VALUE) {
                _MINIDUMP_EXCEPTION_INFORMATION ExInfo;

                ExInfo.ThreadId = ::GetCurrentThreadId();
                ExInfo.ExceptionPointers = pExceptionInfo;
                ExInfo.ClientPointers = NULL;

                // write the dump
                BOOL bOK = pDump(GetCurrentProcess(),
                                 GetCurrentProcessId(),
                                 fh,
                                 MiniDumpWithFullMemory,
                                 &ExInfo,
                                 NULL,
                                 NULL);
                if (bOK) {
                    sprintf(szScratch, "saved dump file to '%s'", szDumpPath);
                    szResult = szScratch;
                    retval = EXCEPTION_EXECUTE_HANDLER;
                } else {
                    sprintf(szScratch,
                            "failed to save dump file to '%s' (error %d)",
                            szDumpPath,
                            GetLastError());
                    szResult = szScratch;
                }
                ::CloseHandle(fh);
            } else {
                sprintf(szScratch,
                        "failed to create dump file '%s' (error %d)",
                        szDumpPath,
                        GetLastError());
                szResult = szScratch;
            }
        } else {
            szResult = "DBGHELP.DLL too old";
        }
    } else {
        szResult = "DBGHELP.DLL not found";
    }

    if (szResult) {
        derror("%s", szResult);
        printf(szResult);
    }

    ::dsn::tools::sys_exit.execute(SYS_EXIT_EXCEPTION);
    return retval;
}
}
}

#endif // _WIN32
