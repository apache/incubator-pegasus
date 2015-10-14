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

#include <dsn/cpp/serverlet.h>

namespace dsn 
{
    namespace rpc 
    {
        template<typename TRequest, typename TResponse>
        dsn::task_ptr call_typed_replicated(
            // servers
            ::dsn::rpc_address first_server,
            const std::vector<::dsn::rpc_address>& servers,
            // request
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,

            // callback
            clientlet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, ::dsn::rpc_address)> callback,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            );

        dsn::task_ptr call_replicated(
            ::dsn::rpc_address first_server,
            const std::vector<::dsn::rpc_address>& servers,
            dsn_message_t request,

            // reply
            clientlet* svc,
            rpc_reply_handler callback,
            int reply_hash = 0
            );
        // ----------------  inline implementation -------------------

        namespace internal_use_only
        {
            template<typename TRequest, typename TResponse>
            inline void rpc_replicated_callback(
                error_code code,
                dsn_message_t request,
                dsn_message_t response,
                std::shared_ptr<TRequest>& req,
                std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, ::dsn::rpc_address)> callback
                )
            {
                ::dsn::rpc_address srv;
                std::shared_ptr<TResponse> resp(nullptr);
                if (code == ERR_OK)
                {
                    srv = dsn_msg_from_address(response);
                    resp.reset(new TResponse);
                    ::unmarshall(response, *resp);
                }
                callback(code, req, resp, srv);
            }
        }

        template<typename TRequest, typename TResponse>
        inline dsn::task_ptr call_typed_replicated(
            // servers
            ::dsn::rpc_address first_server,
            const std::vector<::dsn::rpc_address>& servers,
            // request
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,

            // callback
            clientlet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, ::dsn::rpc_address)> callback,
            int request_hash,
            int timeout_milliseconds,
            int reply_hash
            )
        {
            dsn_message_t request = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            ::marshall(request, *req);

            return call_replicated(
                first_server,
                servers,
                request,
                owner,
                std::bind(&internal_use_only::rpc_replicated_callback,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                req,
                callback
                ),
                reply_hash
                );
        }
    } // end rpc
} // end namespace dsn
