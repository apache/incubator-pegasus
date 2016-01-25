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
#include <dsn/dist/replication/replication_other_types.h>
#include "client_ddl.h"
#include <iostream>
#include <fstream>
#include <iomanip>

namespace dsn{ namespace replication{

client_ddl::client_ddl(const std::vector<dsn::rpc_address>& meta_servers)
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

    auto resp_task = request_meta<configuration_create_app_request>(
            RPC_CM_CREATE_APP,
            req
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

        auto query_task = request_meta<configuration_query_by_index_request>(
                    RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                    query_req
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

    auto resp_task = request_meta<configuration_drop_app_request>(
            RPC_CM_DROP_APP,
            req
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

dsn::error_code client_ddl::list_apps(const dsn::replication::app_status status, const std::string& file_name)
{
    std::shared_ptr<configuration_list_apps_request> req(new configuration_list_apps_request());
    req->status = status;

    auto resp_task = request_meta<configuration_list_apps_request>(
            RPC_CM_LIST_APPS,
            req
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_list_apps_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    // print configuration_list_apps_response
    std::streambuf * buf;
    std::ofstream of;

    if(!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    out << std::setw(10) << std::left << "app_id"
        << std::setw(20) << std::left << "status"
        << std::setw(20) << std::left << "app_name"
        << std::setw(20) << std::left << "app_type"
        << std::setw(10) << std::left << "partition_count"
        << std::endl;
    for(int i = 0; i < resp.infos.size(); i++)
    {
        dsn::replication::app_info info = resp.infos[i];
        out << std::setw(10) << std::left << info.app_id
            << std::setw(20) << std::left << enum_to_string(info.status)
            << std::setw(20) << std::left << info.app_name
            << std::setw(20) << std::left << info.app_type
            << std::setw(10) << std::left << info.partition_count
            << std::endl;
    }
    out << std::endl << std::flush;
    return dsn::ERR_OK;
}

dsn::error_code client_ddl::list_app(const std::string& app_name, bool detailed, const std::string& file_name)
{
    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) client_ddl::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_query_by_index_request> req(new configuration_query_by_index_request());
    req->app_name = app_name;

    auto resp_task = request_meta<configuration_query_by_index_request>(
            RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
            req
    );

    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_query_by_index_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    // print configuration_query_by_index_response
    std::streambuf * buf;
    std::ofstream of;

    if(!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    out << "app name:" << app_name << std::endl
        << "app id:" << resp.app_id << std::endl
        << "app partition count:" << resp.partition_count << std::endl;
    if(detailed)
    {
        out << "details:" << std::endl
            << std::setw(10) << std::left << "pidx"
            << std::setw(10) << std::left << "ballot"
            << std::setw(15) << std::left << "commmitdecree"
            << std::setw(25) << std::left << "primary"
            << std::setw(40) << std::left << "secondaries"
            << std::endl;
        for(int i = 0; i < resp.partitions.size(); i++)
        {
            const dsn::replication::partition_configuration& p = resp.partitions[i];
            out << std::setw(10) << std::left << p.gpid.pidx
                << std::setw(10) << std::left << p.ballot
                << std::setw(15) << std::left << p.last_committed_decree
                << std::setw(25) << std::left << p.primary.to_std_string()
                << std::left<< p.secondaries.size() << ":[";
            for(int j = 0; j < p.secondaries.size(); j++)
            {
                if(j!= 0)
                    out << ",";
                out << p.secondaries[j].to_std_string();
            }
            out << "]" << std::endl;
        }
    }
    out << std::endl;
    return dsn::ERR_OK;
}

bool client_ddl::valid_app_char(int c)
{
    return (bool)std::isalnum(c) || c == '_' || c == '.';
}

void client_ddl::end_meta_request(task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp)
{
    if(err == dsn::ERR_TIMEOUT && retry_times < 5)
    {
        rpc_address leader = dsn_group_get_leader(_meta_servers.group_handle());
        rpc_address next = dsn_group_next(_meta_servers.group_handle(), leader.c_addr());
        dsn_group_set_leader(_meta_servers.group_handle(), next.c_addr());

        rpc::call(
            _meta_servers,
            request,
            this,
            [=, callback_capture = std::move(callback)](error_code err, dsn_message_t request, dsn_message_t response)
            {
                end_meta_request(std::move(callback_capture), retry_times + 1, err, request, response);
            },
            0
         );
    }
    else
        callback->enqueue_rpc_response(err, resp);
}

}} // namespace
