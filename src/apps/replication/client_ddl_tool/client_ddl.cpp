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

/*
 * Description:
 *     ddl client implementation
 *
 * Revision history:
 *     2015-12-30, xiaotz, first version
 */

#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication.types.h>
#include <dsn/dist/replication/client_ddl.h>
#include <iostream>

using namespace dsn::replication;

namespace dsn{ namespace client{

client_ddl::client_ddl(std::vector<dsn::rpc_address> meta_servers)
{
    _meta_servers.assign_group(dsn_group_build("meta.servers"));
    for (auto& m : meta_servers)
        dsn_group_add(_meta_servers.group_handle(), m.c_addr());
}

dsn::error_code client_ddl::create_app(const std::string& app_name, const std::string& app_type, int partition_count, int replica_count)
{
    if((partition_count < 0) || ((partition_count & (partition_count -1)) != 0))
        return ERR_INVALID_PARAMETERS;

    if(replica_count < 3)
        return ERR_INVALID_PARAMETERS;

    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) client_ddl::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    if(app_type.empty() || !std::all_of(app_type.cbegin(),app_type.cend(),(bool (*)(int)) client_ddl::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_create_app_request> req(new configuration_create_app_request());
    req->app_name = app_name;
    req->options.partition_count = partition_count;
    req->options.replica_count = replica_count;
    req->options.success_if_exist = true;
    req->options.app_type = app_type;

    auto resp_task = request_meta<configuration_create_app_request, configuration_create_app_response>(
            RPC_CM_CREATE_APP,
            req,
            nullptr,
            nullptr
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_create_app_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    std::cout << "create app " << app_name << " succeed, waiting for app ready." << std::endl;

    int sleep_sec = 2;
    while(true)
    {
        std::shared_ptr<configuration_query_by_index_request> query_req(new configuration_query_by_index_request());
        query_req->app_name = app_name;
        query_req->if_query_all_partitions = true;

        auto query_task = request_meta<configuration_query_by_index_request, configuration_query_by_index_response>(
                    RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                    query_req,
                    nullptr,
                    nullptr
                    );
        query_task->wait();
        if (query_task->error() != dsn::ERR_OK)
        {
            return dsn::ERR_IO_PENDING;
        }

        dsn::replication::configuration_query_by_index_response query_resp;
        ::unmarshall(query_task->response(), query_resp);
        if(query_resp.err != dsn::ERR_OK)
        {
            return resp.err;
        }
        bool ready = true;
        dassert(partition_count == query_resp.partition_count, "partition count not equal");
        for(int i =0; i < partition_count; i++)
        {
            partition_configuration pc = query_resp.partitions[i];
            if(pc.primary.is_invalid() || ((pc.secondaries.size() * 2 + 2) < replica_count))
            {
                ready = false;
                break;
            }
        }

        if(ready)
            break;
        std::cout << app_name << " not ready yet, still waiting." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
    }
    std::cout << app_name << " is ready now!" << std::endl;
    return dsn::ERR_OK;
}

dsn::error_code client_ddl::drop_app(const std::string& app_name)
{
    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) client_ddl::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_drop_app_request> req(new configuration_drop_app_request());
    req->app_name = app_name;
    req->options.success_if_not_exist = true;

    auto resp_task = request_meta<configuration_drop_app_request, configuration_drop_app_response>(
            RPC_CM_DROP_APP,
            req,
            nullptr,
            nullptr
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_drop_app_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }
    return dsn::ERR_OK;
}

}} // namespace
