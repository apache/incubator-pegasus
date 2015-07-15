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
# include "replication_common.h"
# include "rpc_replicated.h"

using namespace dsn::replication;

namespace dsn {
    namespace service {
        namespace rpc {

            namespace rpc_replicated_impl {

                struct params
                {
                    std::vector<end_point> servers;
                    rpc_response_task_ptr response_task;
                    rpc_reply_handler callback;
                };

                static end_point get_next_server(const end_point& currentServer, const std::vector<end_point>& servers)
                {
                    if (currentServer == end_point::INVALID)
                    {
                        return servers[env::random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
                    }
                    else
                    {
                        auto it = std::find(servers.begin(), servers.end(), currentServer);
                        if (it != servers.end())
                        {
                            ++it;
                            return it == servers.end() ? *servers.begin() : *it;
                        }
                        else
                        {
                            return servers[env::random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
                        }
                    }
                }

                static void internal_rpc_reply_callback(error_code err, message_ptr& request, message_ptr& response, params* ps)
                {
                    //printf ("%s\n", __FUNCTION__);

                    end_point next_server;
                    if (nullptr != response)
                    {
                        err.end_tracking();

                        meta_response_header header;
                        unmarshall(response->reader(), header);

                        if (header.err == ERR_SERVICE_NOT_ACTIVE || header.err == ERR_BUSY)
                        {

                        }
                        else if (header.err == ERR_TALK_TO_OTHERS)
                        {
                            next_server = header.primary_address;
                            err = ERR_OK;
                        }
                        else
                        {
                            if (nullptr != ps->callback)
                            {
                                (ps->callback)(err, request, response);
                            }
                            delete ps;
                            return;
                        }
                    }

                    if (err != ERR_OK)
                    {
                        if (nullptr != ps->callback)
                        {
                            (ps->callback)(err, request, response);
                        }
                        delete ps;
                        return;
                    }

                    rpc::call(
                        next_server,
                        request,
                        ps->response_task
                        );
                }


            } // end namespace rpc_replicated_impl 

            rpc_response_task_ptr call_replicated(
                const end_point& first_server,
                const std::vector<end_point>& servers,
                message_ptr& request,

                // reply
                servicelet* svc,
                rpc_reply_handler callback,
                int reply_hash
                )
            {
                end_point first = first_server;
                if (first == end_point::INVALID)
                {
                    first = rpc_replicated_impl::get_next_server(first_server, servers);
                }

                rpc_replicated_impl::params *ps = new rpc_replicated_impl::params;
                ps->servers = servers;
                ps->callback = callback;

                std::function<void(error_code, message_ptr&, message_ptr&)> cb = std::bind(
                    &rpc_replicated_impl::internal_rpc_reply_callback,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3,
                    ps
                    );

                ps->response_task = new internal_use_only::service_rpc_response_task4(
                    svc,
                    cb,
                    request,
                    reply_hash
                    );

                rpc::call(
                    first,
                    request,
                    ps->response_task
                    );

                return ps->response_task;
            }
        }
    }
} // end namespace
