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
# include <dsn/cpp/utils.h>
# include <dsn/internal/singleton.h>
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


        bool load_dynamic_library(const char* module)
        {
            std::string module_name(module);
# if defined(_WIN32)
            module_name += ".dll";
            if (::LoadLibraryA(module_name.c_str()) != NULL)
            {
                derror("load dynamic library '%s' failed, err = %d", module_name.c_str(), ::GetLastError());
                return false;
            }
            else
                return true;
# elif defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
            module_name += ".dll";
            if (nullptr == dlopen(module_name.c_str(), RTLD_LAZY))
            {
                derror("load dynamic library '%s' failed, err = %s", module_name.c_str(), dlerror());
                return false;
            }
            else
                return true;
# else
# error not implemented yet
# endif 
        }
    }
}

