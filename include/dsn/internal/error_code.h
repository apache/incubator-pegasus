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

#include <dsn/internal/logging.h>
#include <dsn/internal/customizable_id.h>

namespace dsn {

struct error_code : public dsn::utils::customized_id<error_code>
{
    error_code(const char* name) : dsn::utils::customized_id<error_code>(name)
    {
        dassert (name, "name for an error code cannot be empty");
# ifdef _DEBUG
        _used = true;
# endif
    }

    error_code() : dsn::utils::customized_id<error_code>(0)
    {
# ifdef _DEBUG
        _used = true;
# endif
    }

    error_code(const error_code& err) : dsn::utils::customized_id<error_code>(err)
    {
# ifdef _DEBUG
        _used = false;
        err._used = true;
# endif
    }

    error_code& operator=(const error_code& source)
    {
        _internal_code = source;
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
        dassert (_used, "error code is not handled");
    }

    const char* to_string() const
    {
        _used = true;
        return dsn::utils::customized_id<error_code>::to_string();
    }
# endif
    
    void set(int err) 
    { 
        _internal_code = err; 
# ifdef _DEBUG
        _used = false; 
# endif
    }

    int  get() const
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
    operator int() const
    {
# ifdef _DEBUG
        _used = true;
# endif
        return dsn::utils::customized_id<error_code>::operator int();
    }

private:
# ifdef _DEBUG
    mutable bool _used;
# endif
};

#define DEFINE_ERR_CODE(x) __selectany const ::dsn::error_code x(#x);

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

