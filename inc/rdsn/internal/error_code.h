# pragma once

#include <rdsn/internal/logging.h>
#include <rdsn/internal/customizable_id.h>

namespace rdsn {

struct error_code : public rdsn::utils::customized_id<error_code>
{
    error_code(const char* name) : rdsn::utils::customized_id<error_code>(name)
    {
        _used = false;
    }

    error_code() : rdsn::utils::customized_id<error_code>(0)
    {
        _used = true;
    }

    error_code(const error_code& err) : rdsn::utils::customized_id<error_code>(err)
    {
        _used = false;
    }

    error_code& operator=(const error_code& source)
    {
        _internal_code = source.get();
        _used = false;
        return *this;
    }


    /*error_code(int err)
    {
    _error = err;
    _used = false;
    }*/

    ~error_code()
    {
        //assert (_used, "error code is not handled");
    }

    int get() const { _used = true; return operator int(); }

    void set(int err) { _internal_code = err; _used = false; }

private:
    mutable bool _used;
};

#define DEFINE_ERR_CODE(x) __selectany const ::rdsn::error_code x(#x);

DEFINE_ERR_CODE(ERR_SUCCESS)
DEFINE_ERR_CODE(ERR_SERVICE_NOT_FOUND)
DEFINE_ERR_CODE(ERR_SERVICE_ALREADY_RUNNING)
DEFINE_ERR_CODE(ERR_IO_PENDING)
DEFINE_ERR_CODE(ERR_TIMEOUT)
DEFINE_ERR_CODE(ERR_SERVICE_NOT_ACTIVE)
DEFINE_ERR_CODE(ERR_BUSY)
DEFINE_ERR_CODE(ERR_TALK_TO_OTHERS)
DEFINE_ERR_CODE(ERR_OBJECT_NOT_FOUND)
DEFINE_ERR_CODE(ERR_LEARN_FILE_FALED)
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

} // end namespace

