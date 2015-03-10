/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# pragma once

#if defined(_WIN32)

# include <Windows.h>
__pragma(warning(disable:4127))

#define __thread __declspec(thread)
#define __selectany __declspec(selectany) extern 
typedef void* handle_t;

#elif defined(__linux__)

#define __selectany __attribute__((weak)) extern 
typedef int handle_t;

#elif defined(__MACH__)

#define __selectany __attribute__((weak)) extern 
typedef int handle_t;

#else

#error "unsupported platform"
#endif

# ifndef INFINITE
# define INFINITE 0xFFFFFFFFUL
# endif

# ifndef FIELD_OFFSET
# define FIELD_OFFSET(s, field)  ((size_t)&((s *)(0))->field)
# endif

# ifndef CONTAINING_RECORD 
# define CONTAINING_RECORD(address, type, field) \
            ((type *)((PCHAR)(address)-(void*)(&((type *)0)->field)))
# endif

# ifndef MAX_COMPUTERNAME_LENGTH
# define MAX_COMPUTERNAME_LENGTH 32
# endif

# ifndef __in_param
# define __in_param
# endif

# ifndef __out_param
# define __out_param
# endif

# ifndef __inout_param
# define __inout_param
# endif

// stl headers
# include <string>
# include <memory>
# include <map>
# include <vector>
# include <list>
# include <algorithm>

// common c headers
# include <cassert>
# include <cstring>
# include <cstdlib>
# include <fcntl.h> // for file open flags

// common utilities
# include <atomic>
# include <boost/shared_ptr.hpp>
# include <boost/intrusive_ptr.hpp>

// common types
namespace dsn
{   
    class ref_object
    {
    public:
        ref_object() { ref_counter = 0; }
        virtual ~ref_object() {}
        std::atomic<long> ref_counter;

        void add_ref()
        {
            ++ref_counter;
        }

        void release_ref()
        {
            if (--ref_counter == 0)
                delete this;
        }
    };

#define DEFINE_REF_OBJECT(T) \
    static void intrusive_ptr_add_ref(T* obj) \
    { \
        ++obj->ref_counter; \
    } \
    static void intrusive_ptr_release(T* obj) \
    { \
        if (--obj->ref_counter == 0) \
            delete obj; \
    } 

    class task;
    class message;
    class rpc_client_session;
    class rpc_server_session;

    typedef ::boost::intrusive_ptr<task> task_ptr;
    typedef ::boost::intrusive_ptr<message> message_ptr;
    typedef ::boost::intrusive_ptr<rpc_client_session> rpc_client_session_ptr;
    typedef ::boost::intrusive_ptr<rpc_server_session> rpc_server_session_ptr;

    #define TOOL_TYPE_MAIN 0
    #define TOOL_TYPE_ASPECT 1

    #define PROVIDER_TYPE_MAIN 0
    #define PROVIDER_TYPE_ASPECT 1
    
} // end namespace dsn
