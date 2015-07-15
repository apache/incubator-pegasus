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

#include "replica.h"
#include "mutation.h"
#include <dsn/internal/factory_store.h>
#include <boost/filesystem.hpp>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.2pc"

namespace dsn { namespace replication {

void register_replica_provider(replica_app_factory f, const char* name)
{
    ::dsn::utils::factory_store<replication_app_base>::register_factory(name, f, PROVIDER_TYPE_MAIN);
}

replication_app_base::replication_app_base(replica* replica, configuration_ptr& config)
{
    _physical_error = 0;
    _dir_data = replica->dir() + "/data";
    _dir_learn = replica->dir() + "/learn";

    _replica = replica;
    _last_committed_decree = _last_durable_decree = 0;

    if (!boost::filesystem::exists(_dir_data))
        boost::filesystem::create_directory(_dir_data);

    if (!boost::filesystem::exists(_dir_learn))
        boost::filesystem::create_directory(_dir_learn);
}

error_code replication_app_base::write_internal(mutation_ptr& mu, bool ack_client)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");
    
    if (mu->rpc_code != RPC_REPLICATION_WRITE_EMPTY)
    {
        auto& msg = mu->client_request;
        dispatch_rpc_call(
            mu->rpc_code,
            msg,
            ack_client
            );
    }
    else
    {
        on_empty_write();
    }

    if (_physical_error != 0)
    {
        derror("physical error %d occurs in replication local app %s", _physical_error, data_dir().c_str());
    }

    return _physical_error == 0 ? ERR_OK : ERR_LOCAL_APP_FAILURE;
}

void replication_app_base::dispatch_rpc_call(int code, message_ptr& request, bool ack_client)
{
    auto it = _handlers.find(code);
    if (it != _handlers.end())
    {
        if (ack_client)
        {
            message_ptr response = request->create_response();

            int err = 0; // replication layer error
            marshall(response->writer(), err);

            it->second(request, response);
        }
        else
        {
            message_ptr response(nullptr);
            it->second(request, response);
        }
    }
    else
    {
        dassert(false, "cannot find handler for rpc code %d in %s", code, data_dir().c_str());
    }
}

}} // end namespace
