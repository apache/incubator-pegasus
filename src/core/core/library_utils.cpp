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

# include <dsn/utility/utils.h>
# include <dsn/utility/singleton.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <random>

# if defined(__linux__)
# include <sys/syscall.h>
# include <dlfcn.h> 
# elif defined(__FreeBSD__)
# include <sys/thr.h>
# include <dlfcn.h> 
# elif defined(__APPLE__)
# include <pthread.h>
# include <dlfcn.h> 
# endif

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "dsn.utils"

namespace dsn {
    namespace utils {


        dsn_handle_t load_dynamic_library(const char* module)
        {
            std::string module_name(module);
# if defined(_WIN32)
            module_name += ".dll";
            auto hmod = ::LoadLibraryA(module_name.c_str());
            if (hmod == NULL)
            {
                derror("load dynamic library '%s' failed, err = %d", module_name.c_str(), ::GetLastError());
            }            
# elif defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
            module_name = "lib" + module_name + ".so";
            auto hmod = dlopen(module_name.c_str(), RTLD_LAZY|RTLD_GLOBAL);
            if (nullptr == hmod)
            {
                derror("load dynamic library '%s' failed, err = %s", module_name.c_str(), dlerror());
            }
# else
# error not implemented yet
# endif 
            return (dsn_handle_t)(hmod);
        }

        dsn_handle_t load_symbol(dsn_handle_t hmodule, const char* symbol)
        {
# if defined(_WIN32)
            return (dsn_handle_t)::GetProcAddress((HMODULE)hmodule, (LPCSTR)symbol);
# elif defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
            return (dsn_handle_t)dlsym((void*)hmodule, symbol);
# else
# error not implemented yet
# endif 
        }

        void unload_dynamic_library(dsn_handle_t hmodule)
        {
# if defined(_WIN32)
            ::CloseHandle((HMODULE)hmodule);
# elif defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
            dlclose((void*)hmodule);
# else
# error not implemented yet
# endif
        }
    }
}

