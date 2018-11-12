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

#include <dsn/c/api_common.h>
#include <dsn/c/api_layer1.h>
#include <dsn/service_api_cpp.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/tool-api/task_tracker.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/chrono_literals.h>

namespace dsn {

using literals::chrono_literals::operator"" _ms;

//
// rpc_holder is mainly designed for RAII of dsn::message_ex*.
// Since the request message will be automatically released after the rpc ends,
// it will become inaccessible when you use it in an async call (probably via tasking::enqueue).
// So in rpc_holder we hold another reference of the message, preventing it to be deleted.
//
// rpc_holder also provides a simple approach for rpc mocking.
// For example:
//
//   typedef rpc_holder<write_request, write_response> write_rpc;
//
//   void write() {
//       ....
//       auto request = make_unique<write_request>();
//       request->data = "abc";
//       request->timestamp = 12;
//       write_rpc rpc(std::move(request), RPC_WRITE);
//       rpc.call(rpc_address("10.57.223.31", 12321), nullptr, on_write_rpc_reply);
//       ...
//   }
//
//   RPC_MOCKING(write_rpc) {
//       write();
//       ASSERT_EQ(1, write_rpc::mail_box().size());
//   }
//
// Here in the instance of `RPC_MOCKING`, we call a function `write`, in which a write_rpc
// was sent to "10.57.223.31:12321". However, since we are in the mock mode, every `write_request`
// message will be dropped into `write_rpc::mail_box` without going through network.
//

template <typename TRequest, typename TResponse>
class rpc_holder
{
public:
    using request_type = TRequest;
    using response_type = TResponse;

public:
    explicit rpc_holder(dsn::message_ex *req = nullptr)
    {
        if (req != nullptr) {
            _i = std::make_shared<internal>(req);
        }
    }

    rpc_holder(std::unique_ptr<TRequest> req,
               dsn::task_code code,
               std::chrono::milliseconds timeout = 0_ms,
               uint64_t partition_hash = 0)
        : _i(new internal(req, code, timeout, partition_hash))
    {
    }

    // copyable and movable
    // Copying an rpc_holder doesn't produce a deep copy, the new instance will
    // reference the same rpc internal data. So, just feel free to copy :)
    rpc_holder(const rpc_holder &) = default;
    rpc_holder(rpc_holder &&) noexcept = default;
    rpc_holder &operator=(const rpc_holder &) = default;
    rpc_holder &operator=(rpc_holder &&) noexcept = default;

    bool is_initialized() const { return bool(_i); }

    const TRequest &request() const
    {
        dassert(_i, "rpc_holder is uninitialized");
        return *(_i->thrift_request);
    }

    TRequest *mutable_request() const
    {
        dassert(_i, "rpc_holder is uninitialized");
        return _i->thrift_request.get();
    }

    TResponse &response() const
    {
        dassert(_i, "rpc_holder is uninitialized");
        return _i->thrift_response;
    }

    dsn::message_ex *dsn_request() const
    {
        dassert(_i, "rpc_holder is uninitialized");
        return _i->dsn_request;
    }

    // the remote address where reveice request from and send response to.
    rpc_address remote_address() const { return dsn_request()->header->from_address; }

    // TCallback = void(dsn::error_code)
    // NOTE that the `error_code` is not the error carried by response. Users should
    // check the responded error themselves.
    template <typename TCallback>
    task_ptr call(::dsn::rpc_address server,
                  dsn::task_tracker *tracker,
                  TCallback &&callback,
                  int reply_thread_hash = 0)
    {
        // ensures that TCallback receives exactly one argument, which must be a dsn::error_code.
        static_assert(function_traits<TCallback>::arity == 1,
                      "TCallback must receive exactly one argument");
        static_assert(std::is_same<typename function_traits<TCallback>::template arg_t<0>,
                                   dsn::error_code>::value,
                      "the first argument of TCallback must be dsn::error_code");

        if (dsn_unlikely(_mail_box != nullptr)) {
            _mail_box->emplace_back(*this);
            return nullptr;
        }

        rpc_response_task_ptr t = rpc::create_rpc_response_task(
            dsn_request(),
            tracker,
            [ cb_fwd = std::forward<TCallback>(callback),
              rpc = *this ](error_code err, dsn::message_ex * req, dsn::message_ex * resp) mutable {
                if (err == ERR_OK) {
                    ::dsn::unmarshall(resp, rpc.response());
                }
                cb_fwd(err);
            },
            reply_thread_hash);
        dsn_rpc_call(server, t);
        return t;
    }

    // Returns an rpc_holder that will reply the request after its lifetime ends.
    // By default rpc_holder never replies.
    // SEE: serverlet<T>::register_rpc_handler_with_rpc_holder
    static inline rpc_holder auto_reply(dsn::message_ex *req)
    {
        rpc_holder rpc(req);
        rpc._i->auto_reply = true;
        return rpc;
    }

    // Only use this function when testing.
    // In mock mode, all messages will be dropped into mail_box without going through network,
    // and response callbacks will never be called.
    // This function is not thread-safe.
    using mail_box_t = std::vector<rpc_holder<TRequest, TResponse>>;
    using mail_box_u_ptr = std::unique_ptr<mail_box_t>;
    static void enable_mocking()
    {
        dassert(_mail_box == nullptr, "remember to call clear_mocking_env after testing");
        _mail_box = make_unique<mail_box_t>();
    }

    // Only use this function when testing.
    // Remember to call it after test finishes, or it may effect the results of other tests.
    // This function is not thread-safe.
    static void clear_mocking_env() { _mail_box.reset(nullptr); }

    static mail_box_t &mail_box()
    {
        dassert(_mail_box != nullptr, "call this function only when you are in mock mode");
        return *_mail_box.get();
    }

    friend bool operator<(const rpc_holder &lhs, const rpc_holder &rhs) { return lhs._i < rhs._i; }

private:
    friend class rpc_holder_test;

    struct internal
    {
        explicit internal(dsn::message_ex *req)
            : dsn_request(req), thrift_request(make_unique<TRequest>()), auto_reply(false)
        {
            // we must hold one reference for the request, or rdsn will delete it after
            // the rpc call ends.
            dsn_request->add_ref();
            dsn::unmarshall(req, *thrift_request);
        }

        internal(std::unique_ptr<TRequest> &req,
                 dsn::task_code code,
                 std::chrono::milliseconds timeout,
                 uint64_t partition_hash)
            : thrift_request(std::move(req)), auto_reply(false)
        {
            dassert(thrift_request != nullptr, "req should not be null");

            // leave thread_hash to 0
            dsn_request = dsn::message_ex::create_request(
                code, static_cast<int>(timeout.count()), 0, partition_hash);
            dsn_request->add_ref();
            dsn::marshall(dsn_request, *thrift_request);
        }

        void reply()
        {
            if (dsn_unlikely(_mail_box != nullptr)) {
                rpc_holder<TRequest, TResponse> rpc(std::move(thrift_request),
                                                    dsn_request->rpc_code());
                rpc.response() = std::move(thrift_response);
                _mail_box->emplace_back(std::move(rpc));
                return;
            }

            dsn::message_ex *dsn_response = dsn_request->create_response();
            ::dsn::marshall(dsn_response, thrift_response);
            dsn_rpc_reply(dsn_response);
        }

        ~internal()
        {
            if (auto_reply) {
                reply();
            }
            dsn_request->release_ref();
        }

        dsn::message_ex *dsn_request;
        std::unique_ptr<TRequest> thrift_request;
        TResponse thrift_response;

        bool auto_reply;
    };

    std::shared_ptr<internal> _i;

    static mail_box_u_ptr _mail_box;
};

// ======== type traits ========

// check if a given type is rpc_holder.
// is_rpc_holder<T>::value = true indicates that type T is an rpc_holder.

template <typename T>
struct is_rpc_holder : public std::false_type
{
};

template <typename TRequest, typename TResponse>
struct is_rpc_holder<rpc_holder<TRequest, TResponse>> : public std::true_type
{
};

// ======== utilities ========

namespace rpc {

// call an RPC specified by rpc_holder.
// TCallback = void(dsn::error_code)

template <typename TCallback, typename TRpcHolder>
task_ptr call(::dsn::rpc_address server,
              TRpcHolder rpc,
              dsn::task_tracker *tracker,
              TCallback &&callback,
              int reply_thread_hash = 0)
{
    static_assert(is_rpc_holder<TRpcHolder>::value, "TRpcHolder must be an rpc_holder");
    return rpc.call(server, tracker, std::forward<TCallback &&>(callback), reply_thread_hash);
}

} // namespace rpc

// ======== rpc mock ========

template <typename TRequest, typename TResponse>
typename rpc_holder<TRequest, TResponse>::mail_box_u_ptr rpc_holder<TRequest, TResponse>::_mail_box;

template <typename TRpcHolder>
struct rpc_mock_wrapper
{
    rpc_mock_wrapper() { TRpcHolder::enable_mocking(); }
    ~rpc_mock_wrapper() { TRpcHolder::clear_mocking_env(); }
    int counter = 0;
};

#define RPC_MOCKING(__rpc_type__)                                                                  \
    for (::dsn::rpc_mock_wrapper<__rpc_type__> __rpc_type__##_mocking__;                           \
         __rpc_type__##_mocking__.counter != 1;                                                    \
         __rpc_type__##_mocking__.counter++)

} // namespace dsn
