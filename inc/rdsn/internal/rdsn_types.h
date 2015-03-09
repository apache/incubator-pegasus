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

# ifndef __in
# define __in
# endif

# ifndef __out
# define __out
# endif

# ifndef __inout
# define __inout
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
#include <fcntl.h> // for file open flags

// common utilities
# include <atomic>
# include <boost/shared_ptr.hpp>
# include <boost/intrusive_ptr.hpp>

// common types
namespace rdsn
{   
    class ref_object
    {
    public:
        ref_object() { ref_counter = 0; }
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
    
} // end namespace rdsn