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
 *     self-execution code for dynamic linked libraries
 *
 * Revision history:
 *     Aug., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */



//
// developers define the following global function somewhere
//
//     MODULE_INIT_BEGIN(module_name)
//          ...
//     MODULE_INIT_END
//    
// and include this cpp file only once in a module
//     # include <dsn/utility/module_init.cpp.h>
//
// then it is done.
//


# if defined(__GNUC__) || defined(_WIN32)
# else
# error "dsn init on shared lib loading is not supported on this platform yet"
# endif

extern void dsn_module_init();

# if defined(__GNUC__)
# define MODULE_INIT_BEGIN(x) __attribute__((constructor)) void dsn_module_init_##x() {
# else
# define MODULE_INIT_BEGIN(x) void dsn_module_init() {
# endif

# define MODULE_INIT_END }

# ifdef _WIN32
# include <Windows.h>

#ifdef _MANAGED
#pragma managed(push, off)
#endif

bool APIENTRY DllMain(HMODULE hModule,
    DWORD  ul_reason_for_call,
    void* lpReserved
    )
{
    switch (ul_reason_for_call)
    {
    case DLL_PROCESS_ATTACH:
        dsn_module_init();
        break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
        break;
    case DLL_PROCESS_DETACH:
        break;
    }
    return TRUE;
}

#ifdef _MANAGED
#pragma managed(pop)
#endif

# endif

