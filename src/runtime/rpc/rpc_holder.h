// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_tracker.h"
#include "utils/smart_pointers.h"
#include "utils/chrono_literals.h"
#include "client/partition_resolver.h"

namespace dsn {

using literals::chrono_literals::operator"" _ms;

//
// rpc_holder is mainly designed for RAII of message_ex*.
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
    explicit rpc_holder(message_ex *req = nullptr)
    {
        if (req != nullptr) {
            _i = std::make_shared<internal>(req);
        }
    }

    rpc_holder(std::unique_ptr<TRequest> req,
               task_code code,
               std::chrono::milliseconds timeout = 0_ms,
               uint64_t partition_hash = 0,
               int thread_hash = 0)
        : _i(new internal(req, code, timeout, partition_hash, thread_hash))
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
        CHECK(_i, "rpc_holder is uninitialized");
        return *(_i->thrift_request);
    }

    TRequest *mutable_request() const
    {
        CHECK(_i, "rpc_holder is uninitialized");
        return _i->thrift_request.get();
    }

    TResponse &response() const
    {
        CHECK(_i, "rpc_holder is uninitialized");
        return _i->thrift_response;
    }

    dsn::error_code &error() const
    {
        CHECK(_i, "rpc_holder is uninitialized");
        return _i->rpc_error;
    }

    message_ex *dsn_request() const
    {
        CHECK(_i, "rpc_holder is uninitialized");
        return _i->dsn_request;
    }

    // the remote address where reveice request from and send response to.
    rpc_address remote_address() const { return dsn_request()->header->from_address; }

    // TCallback = void(error_code)
    // NOTE that the `error_code` is not the error carried by response. Users should
    // check the responded error themselves.
    template <typename TCallback>
    task_ptr call(const rpc_address &server,
                  task_tracker *tracker,
                  TCallback &&callback,
                  int reply_thread_hash = 0)
    {
        // ensures that TCallback receives exactly one argument, which must be a error_code.
        static_assert(function_traits<TCallback>::arity == 1,
                      "TCallback must receive exactly one argument");
        static_assert(
            std::is_same<typename function_traits<TCallback>::template arg_t<0>, error_code>::value,
            "the first argument of TCallback must be error_code");

        if (dsn_unlikely(_mail_box != nullptr)) {
            _mail_box->emplace_back(*this);
            return nullptr;
        }

        rpc_response_task_ptr t = rpc::create_rpc_response_task(
            dsn_request(),
            tracker,
            [ cb_fwd = std::forward<TCallback>(callback),
              rpc = *this ](error_code err, message_ex * req, message_ex * resp) mutable {
                if (err == ERR_OK) {
                    unmarshall(resp, rpc.response());
                }
                cb_fwd(err);
            },
            reply_thread_hash);
        dsn_rpc_call(server, t);
        return t;
    }

    template <typename TCallback>
    task_ptr call(replication::partition_resolver_ptr &resolver,
                  task_tracker *tracker,
                  TCallback &&callback,
                  int reply_thread_hash = 0)
    {
        static_assert(function_traits<TCallback>::arity == 1,
                      "TCallback must receive exactly one argument");
        static_assert(
            std::is_same<typename function_traits<TCallback>::template arg_t<0>, error_code>::value,
            "the first argument of TCallback must be error_code");

        if (dsn_unlikely(_mail_box != nullptr)) {
            _mail_box->emplace_back(*this);
            return nullptr;
        }

        rpc_response_task_ptr t = rpc::create_rpc_response_task(
            dsn_request(),
            tracker,
            [ cb_fwd = std::forward<TCallback>(callback),
              rpc = *this ](error_code err, message_ex * req, message_ex * resp) mutable {
                if (err == ERR_OK) {
                    unmarshall(resp, rpc.response());
                }
                cb_fwd(err);
            },
            reply_thread_hash);
        resolver->call_task(t);
        return t;
    }

    void forward(const rpc_address &addr)
    {
        _i->auto_reply = false;
        if (dsn_unlikely(_forward_mail_box != nullptr)) {
            dsn_request()->header->from_address = addr;
            _forward_mail_box->emplace_back(*this);
            return;
        }

        dsn_rpc_forward(dsn_request(), addr);
    }

    // Returns an rpc_holder that will reply the request after its lifetime ends.
    // By default rpc_holder never replies.
    // SEE: serverlet<T>::register_rpc_handler_with_rpc_holder
    static inline rpc_holder auto_reply(message_ex *req)
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
        CHECK(_mail_box == nullptr && _forward_mail_box == nullptr,
              "remember to call clear_mocking_env after testing");
        _mail_box = make_unique<mail_box_t>();
        _forward_mail_box = make_unique<mail_box_t>();
    }

    // Only use this function when testing.
    // Remember to call it after test finishes, or it may effect the results of other tests.
    // This function is not thread-safe.
    static void clear_mocking_env()
    {
        _mail_box.reset(nullptr);
        _forward_mail_box.reset(nullptr);
    }

    static mail_box_t &mail_box()
    {
        CHECK(_mail_box, "call this function only when you are in mock mode");
        return *_mail_box.get();
    }

    static mail_box_t &forward_mail_box()
    {
        CHECK(_forward_mail_box, "call this function only when you are in mock mode");
        return *_forward_mail_box.get();
    }

    friend bool operator<(const rpc_holder &lhs, const rpc_holder &rhs) { return lhs._i < rhs._i; }

private:
    friend class rpc_holder_test;

    struct internal
    {
        explicit internal(message_ex *req)
            : dsn_request(req), thrift_request(make_unique<TRequest>()), auto_reply(false)
        {
            // we must hold one reference for the request, or rdsn will delete it after
            // the rpc call ends.
            dsn_request->add_ref();
            unmarshall(req, *thrift_request);
        }

        internal(std::unique_ptr<TRequest> &req,
                 task_code code,
                 std::chrono::milliseconds timeout,
                 uint64_t partition_hash,
                 int thread_hash)
            : thrift_request(std::move(req)), auto_reply(false)
        {
            CHECK(thrift_request, "req should not be null");

            dsn_request = message_ex::create_request(
                code, static_cast<int>(timeout.count()), thread_hash, partition_hash);
            dsn_request->add_ref();
            marshall(dsn_request, *thrift_request);
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

            message_ex *dsn_response = dsn_request->create_response();
            marshall(dsn_response, thrift_response);
            dsn_rpc_reply(dsn_response, rpc_error);
        }

        ~internal()
        {
            if (auto_reply) {
                reply();
            }
            dsn_request->release_ref();
        }

        message_ex *dsn_request;
        std::unique_ptr<TRequest> thrift_request;
        TResponse thrift_response;
        dsn::error_code rpc_error = dsn::ERR_OK;

        bool auto_reply;
    };

    std::shared_ptr<internal> _i;

    static mail_box_u_ptr _mail_box;
    static mail_box_u_ptr _forward_mail_box;
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
// TCallback = void(error_code)

template <typename TCallback, typename TRpcHolder>
task_ptr call(rpc_address server,
              TRpcHolder rpc,
              task_tracker *tracker,
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
template <typename TRequest, typename TResponse>
typename rpc_holder<TRequest, TResponse>::mail_box_u_ptr
    rpc_holder<TRequest, TResponse>::_forward_mail_box;

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
