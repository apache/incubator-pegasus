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

namespace dsn 
{
    namespace rpc
    {

        namespace rpc_replicated_impl {

            struct params
            {
                std::vector<::dsn::rpc_address> servers;
                rpc_reply_handler callback;

                // internal callback contexts
                std::function<void(error_code, dsn_message_t, dsn_message_t)> internal_cb;
                clientlet* svc;
                int         reply_hash;
            };

            static ::dsn::rpc_address get_next_server(::dsn::rpc_address currentServer, const std::vector<::dsn::rpc_address>& servers)
            {
                if (currentServer.is_invalid())
                {
                    return servers[dsn_random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
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
                        return servers[dsn_random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
                    }
                }
            }

            static void internal_rpc_reply_callback(error_code err, dsn_message_t request, dsn_message_t response, params* ps)
            {
                meta_response_header header;
                if (nullptr != response)
                {
                    err.end_tracking();
                    ::unmarshall(response, header);

                    if (header.err == ERR_FORWARD_TO_OTHERS)
                    {
                        dsn_msg_add_ref(request); // add for another round of rpc::call
                        rpc::call(header.primary_address, request, ps->svc, ps->internal_cb, ps->reply_hash);
                        return;
                    }

                    err = header.err;
                }

                if (nullptr != ps->callback)
                {
                    (ps->callback)(err, request, response);
                }
                delete ps;
            }


        } // end namespace rpc_replicated_impl 

        dsn::task_ptr call_replicated(
            ::dsn::rpc_address first_server,
            const std::vector<::dsn::rpc_address>& servers,
            dsn_message_t request,

            // reply
            clientlet* svc,
            rpc_reply_handler callback,
            int reply_hash
            )
        {
            dassert(false, "this api is not obsolete, pls use normal rpc::call with rpc_group_address instead");

            ::dsn::rpc_address first = first_server;
            if (first.is_invalid())
            {
                first = rpc_replicated_impl::get_next_server(first_server, servers);
            }

            rpc_replicated_impl::params *ps = new rpc_replicated_impl::params;
            ps->servers = servers;
            ps->callback = callback;

            ps->internal_cb = std::bind(
                &rpc_replicated_impl::internal_rpc_reply_callback,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                ps
                );
            ps->svc = svc;
            ps->reply_hash = reply_hash;

            return rpc::call(first, request, ps->svc, ps->internal_cb, ps->reply_hash);
        }
    }
} // end namespace
