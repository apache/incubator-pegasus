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

#include <vector>
#include <unordered_map>
#include <functional>

#include "replica/storage/simple_kv/simple_kv.code.definition.h"
#include "rrdb/rrdb.code.definition.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "task/task_code.h"
#include "common/gpid.h"
#include "rpc/serialization.h"
#include "rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "rpc/rpc_address.h"

namespace dsn {
namespace replication {
template <typename T>
class storage_serverlet
{
protected:
    typedef std::function<int(T *, dsn::message_ex *req)> rpc_handler;
    static std::unordered_map<std::string, rpc_handler> s_handlers;
    static std::vector<rpc_handler> s_vhandlers;

    template <typename TReq, typename TResp>
    static bool
    register_async_rpc_handler(dsn::task_code rpc_code,
                               const char *name,
                               void (*handler)(T *svc, const TReq &req, rpc_replier<TResp> &resp))
    {
        // Only allowed to register simple.kv rpc handler.
        CHECK(dsn::replication::application::RPC_SIMPLE_KV_SIMPLE_KV_READ == rpc_code ||
                  dsn::replication::application::RPC_SIMPLE_KV_SIMPLE_KV_WRITE == rpc_code ||
                  dsn::replication::application::RPC_SIMPLE_KV_SIMPLE_KV_APPEND == rpc_code,
              "Not allowed to register with rpc_code {}",
              rpc_code);
        rpc_handler h = [handler](T *p, dsn::message_ex *r) {
            TReq req;
            ::dsn::unmarshall(r, req);
            rpc_replier<TResp> replier(r->create_response());
            handler(p, req, replier);
            // For simple.kv, always return 0 which means success.
            return 0;
        };

        return register_async_rpc_handler(rpc_code, name, h);
    }

    template <typename TRpcHolder>
    static bool register_rpc_handler_with_rpc_holder(dsn::task_code rpc_code,
                                                     const char *name,
                                                     void (*handler)(T *svc, TRpcHolder))
    {
        rpc_handler h = [handler](T *p, dsn::message_ex *request) {
            auto rh = TRpcHolder::auto_reply(request);
            handler(p, rh);
            return rh.response().error;
        };

        return register_async_rpc_handler(rpc_code, name, h);
    }

    template <typename TReq>
    static bool register_async_rpc_handler(dsn::task_code rpc_code,
                                           const char *name,
                                           void (*handler)(T *svc, const TReq &req))
    {
        // Only allowed to register RPC_RRDB_RRDB_CLEAR_SCANNER handler.
        CHECK_EQ_MSG(dsn::apps::RPC_RRDB_RRDB_CLEAR_SCANNER,
                     rpc_code,
                     "Not allowed to register with rpc_code {}",
                     rpc_code);
        rpc_handler h = [handler](T *p, dsn::message_ex *r) {
            TReq req;
            ::dsn::unmarshall(r, req);
            handler(p, req);
            // For RPC_RRDB_RRDB_CLEAR_SCANNER, always return 0 which means success.
            return 0;
        };

        return register_async_rpc_handler(rpc_code, name, h);
    }

    static bool register_async_rpc_handler(dsn::task_code rpc_code, const char *name, rpc_handler h)
    {
        CHECK(s_handlers.emplace(rpc_code.to_string(), h).second,
              "handler {} has already been registered",
              rpc_code);
        CHECK(s_handlers.emplace(name, h).second, "handler {} has already been registered", name);

        s_vhandlers.resize(rpc_code + 1);
        CHECK(s_vhandlers[rpc_code] == nullptr,
              "handler {}({}) has already been registered",
              rpc_code,
              rpc_code.code());
        s_vhandlers[rpc_code] = h;
        return true;
    }

    static const rpc_handler *find_handler(dsn::task_code rpc_code)
    {
        if (rpc_code < s_vhandlers.size() && s_vhandlers[rpc_code] != nullptr) {
            return &s_vhandlers[rpc_code];
        }

        auto iter = s_handlers.find(rpc_code.to_string());
        if (iter != s_handlers.end()) {
            return &(iter->second);
        }

        return nullptr;
    }

    // The return type is generated by storage engine, e.g. rocksdb::Status::Code, 0 always mean OK.
    int handle_request(dsn::message_ex *request)
    {
        dsn::task_code t = request->rpc_code();
        const rpc_handler *ptr = find_handler(t);
        if (ptr != nullptr) {
            return (*ptr)(static_cast<T *>(this), request);
        } else {
            LOG_WARNING("recv message with unhandled rpc name {} from {}, trace_id = {:#018x} ",
                        t,
                        request->header->from_address,
                        request->header->trace_id);
            dsn_rpc_reply(request->create_response(), ::dsn::ERR_HANDLER_NOT_FOUND);
            // TODO(yingchun): return a non-zero value
            return 0;
        }
    }
};

template <typename T>
std::unordered_map<std::string, typename storage_serverlet<T>::rpc_handler>
    storage_serverlet<T>::s_handlers;

template <typename T>
std::vector<typename storage_serverlet<T>::rpc_handler> storage_serverlet<T>::s_vhandlers;
} // namespace replication
} // namespace dsn
