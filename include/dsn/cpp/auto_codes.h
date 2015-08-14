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
# pragma once

# include <dsn/service_api_c.h>
# include <dsn/ports.h>
# include <dsn/cpp/autoref_ptr.h>
# include <memory>

namespace dsn 
{
    class message_ptr : public ::std::shared_ptr<char>
    {
    public:
        message_ptr() : ::std::shared_ptr<char>(nullptr, release)
        {}

        message_ptr(dsn_message_t msg) : ::std::shared_ptr<char>((char*)msg, release)
        {}

        dsn_message_t get_msg()
        {
            return (dsn_message_t)get();
        }

    private:
        static void release(char* msg)
        {
            if (nullptr != msg)
            {
                dsn_msg_release_ref((dsn_message_t)msg);
            }
        }
    };

    class task_code
    {
    public:
        task_code(const char* name, dsn_task_type_t tt, dsn_task_priority_t pri, dsn_threadpool_code_t pool)
        {
            _internal_code = dsn_task_code_register(name, tt, pri, pool);
        }

        task_code()
        {
            _internal_code = 0;
        }

        task_code(const task_code& r)
        {
            _internal_code = r._internal_code;
        }

        const char* to_string() const
        {
            return dsn_task_code_to_string(_internal_code);
        }

        task_code& operator=(const task_code& source)
        {
            _internal_code = source._internal_code;
            return *this;
        }

        bool operator == (const task_code& r)
        {
            return _internal_code == r._internal_code;
        }

        bool operator != (const task_code& r)
        {
            return !(*this == r);
        }

        operator dsn_task_code_t() const
        {
            return _internal_code;
        }

    private:
        dsn_task_code_t _internal_code;
    };

    // task code with explicit name
    #define DEFINE_NAMED_TASK_CODE(x, name, pri, pool) __selectany const ::dsn::task_code x(#name, TASK_TYPE_COMPUTE, pri, pool);
    #define DEFINE_NAMED_TASK_CODE_AIO(x, name, pri, pool) __selectany const ::dsn::task_code x(#name, TASK_TYPE_AIO, pri, pool);
    #define DEFINE_NAMED_TASK_CODE_RPC(x, name, pri, pool) \
        __selectany const ::dsn::task_code x(#name, TASK_TYPE_RPC_REQUEST, pri, pool); \
        __selectany const ::dsn::task_code x##_ACK(#name"_ACK", TASK_TYPE_RPC_RESPONSE, pri, pool);

    // auto name version
    #define DEFINE_TASK_CODE(x, pri, pool) DEFINE_NAMED_TASK_CODE(x, x, pri, pool)
    #define DEFINE_TASK_CODE_AIO(x, pri, pool) DEFINE_NAMED_TASK_CODE_AIO(x, x, pri, pool)
    #define DEFINE_TASK_CODE_RPC(x, pri, pool) DEFINE_NAMED_TASK_CODE_RPC(x, x, pri, pool)

    class threadpool_code
    {
    public:
        threadpool_code(const char* name)
        {
            _internal_code = dsn_threadpool_code_register(name);
        }

        threadpool_code()
        {
            _internal_code = 0;
        }

        threadpool_code(const threadpool_code& r)
        {
            _internal_code = r._internal_code;
        }

        const char* to_string() const
        {
            return dsn_task_code_to_string(_internal_code);
        }

        threadpool_code& operator=(const threadpool_code& source)
        {
            _internal_code = source._internal_code;
            return *this;
        }

        bool operator == (const threadpool_code& r)
        {
            return _internal_code == r._internal_code;
        }

        bool operator != (const threadpool_code& r)
        {
            return !(*this == r);
        }

        operator dsn_threadpool_code_t() const
        {
            return _internal_code;
        }

    private:
        dsn_threadpool_code_t _internal_code;
    };

    #define DEFINE_THREAD_POOL_CODE(x) __selectany const ::dsn::threadpool_code x(#x);
    
    DEFINE_THREAD_POOL_CODE(THREAD_POOL_INVALID)
    DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEFAULT)
    // define default task code
    DEFINE_TASK_CODE(TASK_CODE_INVALID, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

    class error_code
    {
    public:
        error_code(const char* name)
        {
            _internal_code = dsn_error_register(name);

            dassert (name, "name for an error code cannot be empty");
    # ifdef _DEBUG
            _used = true;
    # endif
        }

        error_code()
        {
            _internal_code = 0;

    # ifdef _DEBUG
            _used = true;
    # endif
        }

        error_code(dsn_error_t err)
        {
            _internal_code = err;

# ifdef _DEBUG
            _used = false;
# endif
        }

        error_code(const error_code& err) 
        {
            _internal_code = err._internal_code;
    # ifdef _DEBUG
            _used = false;
            err._used = true;
    # endif
        }

        const char* to_string() const
        {
    # ifdef _DEBUG
            _used = true;
    # endif
            return dsn_error_to_string(_internal_code);
        }

        error_code& operator=(const error_code& source)
        {
            _internal_code = source._internal_code;
    # ifdef _DEBUG
            _used = false;
            source._used = true;
    # endif
            return *this;
        }

        bool operator == (const error_code& r)
        {
    # ifdef _DEBUG
            _used = true;
            r._used = true;
    # endif
            return _internal_code == r._internal_code;
        }

        bool operator != (const error_code& r)
        {
            return !(*this == r);
        }
       
    
    # ifdef _DEBUG
        ~error_code()
        {
            // in cases where error code is std::bind-ed as task callbacks,
            // and when tasks are cancelled, it is difficult to end track them on cancel
            // therefore we change derror to dwarn
            if (!_used)
            {
                dlog(LOG_LEVEL_WARNING, "error-code", "error code is not handled");
            }
        }
    # endif
    
        dsn_error_t get() const
        {
    # ifdef _DEBUG
            _used = true;
    # endif
            return _internal_code;
        }

        operator dsn_error_t() const
        {
    # ifdef _DEBUG
            _used = true;
    # endif
            return _internal_code;
        }

        void end_tracking() const
        {
    # ifdef _DEBUG
            _used = true;
    # endif
        }

    private:
    # ifdef _DEBUG
        mutable bool _used;
    # endif
        dsn_error_t _internal_code;
    };

    #define DEFINE_ERR_CODE(x) __selectany const dsn::error_code x(#x);

    DEFINE_ERR_CODE(ERR_OK)
    DEFINE_ERR_CODE(ERR_SERVICE_NOT_FOUND)
    DEFINE_ERR_CODE(ERR_SERVICE_ALREADY_RUNNING)
    DEFINE_ERR_CODE(ERR_IO_PENDING)
    DEFINE_ERR_CODE(ERR_TIMEOUT)
    DEFINE_ERR_CODE(ERR_SERVICE_NOT_ACTIVE)
    DEFINE_ERR_CODE(ERR_BUSY)
    DEFINE_ERR_CODE(ERR_NETWORK_INIT_FALED)
    DEFINE_ERR_CODE(ERR_TALK_TO_OTHERS)
    DEFINE_ERR_CODE(ERR_OBJECT_NOT_FOUND)
    DEFINE_ERR_CODE(ERR_HANDLER_NOT_FOUND)
    DEFINE_ERR_CODE(ERR_LEARN_FILE_FALED)
    DEFINE_ERR_CODE(ERR_GET_LEARN_STATE_FALED)
    DEFINE_ERR_CODE(ERR_INVALID_VERSION)
    DEFINE_ERR_CODE(ERR_INVALID_PARAMETERS)
    DEFINE_ERR_CODE(ERR_CAPACITY_EXCEEDED)
    DEFINE_ERR_CODE(ERR_INVALID_STATE)
    DEFINE_ERR_CODE(ERR_INACTIVE_STATE)
    DEFINE_ERR_CODE(ERR_NOT_ENOUGH_MEMBER)
    DEFINE_ERR_CODE(ERR_FILE_OPERATION_FAILED)
    DEFINE_ERR_CODE(ERR_HANDLE_EOF)
    DEFINE_ERR_CODE(ERR_WRONG_CHECKSUM)
    DEFINE_ERR_CODE(ERR_INVALID_DATA)
    DEFINE_ERR_CODE(ERR_VERSION_OUTDATED)
    DEFINE_ERR_CODE(ERR_PATH_NOT_FOUND)
    DEFINE_ERR_CODE(ERR_PATH_ALREADY_EXIST)
    DEFINE_ERR_CODE(ERR_ADDRESS_ALREADY_USED)
    DEFINE_ERR_CODE(ERR_STATE_FREEZED)
    DEFINE_ERR_CODE(ERR_LOCAL_APP_FAILURE)

} // end namespace

