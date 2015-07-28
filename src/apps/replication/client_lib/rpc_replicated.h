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

namespace dsn {
    namespace service {
            namespace rpc {

            template<typename TRequest, typename TResponse>
            dsn::service::cpp_task_ptr call_typed_replicated(
                // servers
                const dsn_address_t& first_server,
                const std::vector<dsn_address_t>& servers,
                // request
                dsn_task_code_t code,
                std::shared_ptr<TRequest>& req,

                // callback
                servicelet* owner,
                std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, const dsn_address_t&)> callback,
                int request_hash = 0,
                int timeout_milliseconds = 0,
                int reply_hash = 0
                );

            dsn::service::cpp_task_ptr call_replicated(
                const dsn_address_t& first_server,
                const std::vector<dsn_address_t>& servers,
                dsn_message_t request,

                // reply
                servicelet* svc,
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
                    std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, const dsn_address_t&)> callback
                    )
                {
                    dsn_address_t srv = dsn_address_invalid;
                    std::shared_ptr<TResponse> resp(nullptr);
                    if (code == ERR_OK)
                    {
                        dsn_msg_from_address(response, &srv);
                        resp.reset(new TResponse);
                        unmarshall(response->reader(), *resp);
                    }
                    callback(code, req, resp, srv);
                }
            }

            template<typename TRequest, typename TResponse>
            inline dsn::service::cpp_task_ptr call_typed_replicated(
                // servers
                const dsn_address_t& first_server,
                const std::vector<dsn_address_t>& servers,
                // request
                dsn_task_code_t code,
                std::shared_ptr<TRequest>& req,

                // callback
                servicelet* owner,
                std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&, const dsn_address_t&)> callback,
                int request_hash,
                int timeout_milliseconds,
                int reply_hash
                )
            {
                dsn_message_t request = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
                marshall(request->writer(), *req);

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
    } // end service
} // end namespace dsn
