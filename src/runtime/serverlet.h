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
#pragma once

#include "runtime/service_app.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/rpc/serialization.h"

namespace dsn {
/*!
@addtogroup rpc-server
@{
*/

//
// for TRequest/TResponse, we assume that the following routines are defined:
//    marshall(binary_writer& writer, const T& val);
//    unmarshall(binary_reader& reader, /*out*/ T& val);
// either in the namespace of ::dsn or T
// developers may write these helper functions by their own, or use tools
// such as protocol-buffer, thrift, or bond to generate these functions automatically
// for their TRequest and TResponse
//

template <typename TResponse>
class rpc_replier
{
public:
    rpc_replier(dsn::message_ex *response) { _response = response; }
    rpc_replier(rpc_replier &&r)
    {
        _response = r._response;
        r._response = nullptr;
    }
    rpc_replier &operator=(rpc_replier &&r)
    {
        release();
        _response = r._response;
        r._response = nullptr;
        return *this;
    }

    ~rpc_replier() { release(); }

    rpc_replier(const rpc_replier &r) = delete;
    rpc_replier(rpc_replier &r) = delete;
    rpc_replier &operator=(const rpc_replier &r) = delete;
    rpc_replier &operator=(rpc_replier &r) = delete;

    void operator()(const TResponse &resp)
    {
        if (_response != nullptr) {
            ::dsn::marshall(_response, resp);
            dsn_rpc_reply(_response);
            _response = nullptr;
        }
    }

    bool is_empty() const { return _response == nullptr; }

    // response message, may be nullptr
    dsn::message_ex *response_message() const { return _response; }

    // the address where send response to
    rpc_address to_address() const
    {
        return _response != nullptr ? _response->to_address : rpc_address::s_invalid_address;
    }

private:
    void release()
    {
        if (_response != nullptr) {
            _response->add_ref();
            _response->release_ref();
            _response = nullptr;
        }
    }
    dsn::message_ex *_response;
};

template <typename T> // where T : serverlet<T>
class serverlet
{
public:
    explicit serverlet(const char *nm);
    virtual ~serverlet();

protected:
    template <typename TRequest>
    bool register_rpc_handler(task_code rpc_code,
                              const char *extra_name,
                              void (T::*handler)(const TRequest &));

    template <typename TRequest, typename TResponse>
    bool register_rpc_handler(task_code rpc_code,
                              const char *extra_name,
                              void (T::*handler)(const TRequest &, TResponse &));

    template <typename TRpcHolder>
    bool register_rpc_handler_with_rpc_holder(dsn::task_code rpc_code,
                                              const char *extra_name,
                                              void (T::*handler)(TRpcHolder));

    template <typename TRequest, typename TResponse>
    bool register_async_rpc_handler(task_code rpc_code,
                                    const char *extra_name,
                                    void (T::*handler)(const TRequest &, rpc_replier<TResponse> &));

    bool register_rpc_handler(task_code rpc_code,
                              const char *extra_name,
                              void (T::*handler)(dsn::message_ex *));

    bool unregister_rpc_handler(task_code rpc_code);

    template <typename TResponse>
    void reply(dsn::message_ex *request, const TResponse &resp);

public:
    const std::string &name() const { return _name; }

private:
    std::string _name;
};

// ------------- inline implementation ----------------
template <typename T>
inline serverlet<T>::serverlet(const char *nm) : _name(nm)
{
}

template <typename T>
inline serverlet<T>::~serverlet()
{
}

template <typename T>
template <typename TRequest>
inline bool serverlet<T>::register_rpc_handler(task_code rpc_code,
                                               const char *extra_name,
                                               void (T::*handler)(const TRequest &))
{
    rpc_request_handler cb = [this, handler](dsn::message_ex *request) {
        TRequest req;
        ::dsn::unmarshall(request, req);
        (((T *)this)->*(handler))(req);
    };

    return dsn_rpc_register_handler(rpc_code, extra_name, cb);
}

template <typename T>
template <typename TRequest, typename TResponse>
inline bool serverlet<T>::register_rpc_handler(task_code rpc_code,
                                               const char *extra_name,
                                               void (T::*handler)(const TRequest &, TResponse &))
{
    rpc_request_handler cb = [this, handler](dsn::message_ex *request) {
        TRequest req;
        ::dsn::unmarshall(request, req);

        TResponse resp;
        (((T *)this)->*(handler))(req, resp);
        rpc_replier<TResponse> replier(request->create_response());
        replier(resp);
    };
    return dsn_rpc_register_handler(rpc_code, extra_name, cb);
}

template <typename T>
template <typename TRpcHolder>
inline bool serverlet<T>::register_rpc_handler_with_rpc_holder(dsn::task_code rpc_code,
                                                               const char *extra_name,
                                                               void (T::*handler)(TRpcHolder))
{
    rpc_request_handler cb = [this, handler](dsn::message_ex *request) {
        (((T *)this)->*(handler))(TRpcHolder::auto_reply(request));
    };

    return dsn_rpc_register_handler(rpc_code, extra_name, cb);
}

template <typename T>
template <typename TRequest, typename TResponse>
inline bool serverlet<T>::register_async_rpc_handler(task_code rpc_code,
                                                     const char *extra_name,
                                                     void (T::*handler)(const TRequest &,
                                                                        rpc_replier<TResponse> &))
{
    rpc_request_handler cb = [this, handler](dsn::message_ex *request) {
        TRequest req;
        ::dsn::unmarshall(request, req);
        rpc_replier<TResponse> replier(request->create_response());
        (((T *)this)->*(handler))(req, replier);
    };
    return dsn_rpc_register_handler(rpc_code, extra_name, cb);
}

template <typename T>
inline bool serverlet<T>::register_rpc_handler(task_code rpc_code,
                                               const char *extra_name,
                                               void (T::*handler)(dsn::message_ex *))
{
    rpc_request_handler cb = [this, handler](dsn::message_ex *request) {
        (((T *)this)->*(handler))(request);
    };

    return dsn_rpc_register_handler(rpc_code, extra_name, cb);
}

template <typename T>
inline bool serverlet<T>::unregister_rpc_handler(task_code rpc_code)
{
    return dsn_rpc_unregiser_handler(rpc_code);
}

template <typename T>
template <typename TResponse>
inline void serverlet<T>::reply(dsn::message_ex *request, const TResponse &resp)
{
    auto msg = request->create_response();
    ::dsn::marshall(msg, resp);
    dsn_rpc_reply(msg);
}
/*@}*/
} // end namespace
