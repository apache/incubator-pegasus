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
 *     replication ddl client implementation
 *
 * Revision history:
 *     2015-12-30, xiaotz, first version
 */

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <iostream>
#include <fstream>
#include <iomanip>

namespace dsn{ namespace replication{

replication_ddl_client::replication_ddl_client(const dsn::rpc_address& meta_server)
    : _meta_server(meta_server)
{
}

replication_ddl_client::replication_ddl_client(const std::vector<dsn::rpc_address>& meta_servers)
{
    _meta_server.assign_group(dsn_group_build("meta-server"));
    for (auto& m : meta_servers)
        dsn_group_add(_meta_server.group_handle(), m.c_addr());
}

dsn::error_code replication_ddl_client::create_app(const std::string& app_name, const std::string& app_type, int partition_count, int replica_count, const std::map<std::string, std::string>& envs, bool is_stateless)
{
    if(partition_count < 1)
    {
        std::cout << "create app " << app_name << " failed: partition_count should >= 1" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if(replica_count < 3)
    {
        std::cout << "create app " << app_name << " failed: replica_count should >= 3" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) replication_ddl_client::valid_app_char))
    {
        std::cout << "create app " << app_name << " failed: invalid app_name" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if(app_type.empty() || !std::all_of(app_type.cbegin(),app_type.cend(),(bool (*)(int)) replication_ddl_client::valid_app_char))
    {
        std::cout << "create app " << app_name << " failed: invalid app_type" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    std::shared_ptr<configuration_create_app_request> req(new configuration_create_app_request());
    req->app_name = app_name;
    req->options.partition_count = partition_count;
    req->options.replica_count = replica_count;
    req->options.success_if_exist = true;
    req->options.app_type = app_type;
    req->options.envs = envs;
    req->options.is_stateful = !is_stateless;

    auto resp_task = request_meta<configuration_create_app_request>(
            RPC_CM_CREATE_APP,
            req
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        std::cout << "create app " << app_name << " failed: [create] call server error: " << resp_task->error().to_string() << std::endl;
        return resp_task->error();
    }

    dsn::replication::configuration_create_app_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        std::cout << "create app " << app_name << " failed: [create] received server error: " << resp.err.to_string() << std::endl;
        return resp.err;
    }

    std::cout << "create app " << app_name << " succeed, waiting for app ready" << std::endl;

    int sleep_sec = 2;
    while(true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));

        std::shared_ptr<configuration_query_by_index_request> query_req(new configuration_query_by_index_request());
        query_req->app_name = app_name;

        auto query_task = request_meta<configuration_query_by_index_request>(
                    RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                    query_req
                    );
        query_task->wait();
        if (query_task->error() == ERR_INVALID_STATE)
        {
            std::cout << app_name << " not ready yet, still waiting..." << std::endl;
            continue;
        }

        if (query_task->error() != dsn::ERR_OK)
        {
            std::cout << "create app " << app_name << " failed: [query] call server error: " << query_task->error().to_string() << std::endl;
            return query_task->error();
        }

        dsn::configuration_query_by_index_response query_resp;
        ::dsn::unmarshall(query_task->response(), query_resp);
        if(query_resp.err != dsn::ERR_OK)
        {
            std::cout << "create app " << app_name << " failed: [query] received server error: " << query_resp.err.to_string() << std::endl;
            return query_resp.err;
        }
        dassert(partition_count == query_resp.partition_count, "partition count not equal");
        int ready_count = 0;
        for(int i = 0; i < partition_count; i++)
        {
            const partition_configuration& pc = query_resp.partitions[i];
            if (!pc.primary.is_invalid() && (pc.secondaries.size() >= replica_count / 2))
            {
                ready_count++;
            }
        }
        std::cout << app_name << " not ready yet, still waiting... ("
                  << ready_count << "/" << partition_count << ")" << std::endl;
        if(ready_count == partition_count)
        {
            break;
        }
    }
    std::cout << app_name << " is ready now!" << std::endl;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::drop_app(const std::string& app_name)
{
    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) replication_ddl_client::valid_app_char))
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
    ::dsn::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_apps(const dsn::app_status::type status, const std::string& file_name)
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
    ::dsn::unmarshall(resp_task->response(), resp);
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
        << std::setw(20) << std::left << "partition_count"
        << std::setw(20) << std::left << "is_stateful"
        << std::endl;
    for(int i = 0; i < resp.infos.size(); i++)
    {
        dsn::app_info info = resp.infos[i];
        std::string status_str = enum_to_string(info.status);
        status_str = status_str.substr(status_str.find("AS_") + 3);
        out << std::setw(10) << std::left << info.app_id
            << std::setw(20) << std::left << status_str
            << std::setw(20) << std::left << info.app_name
            << std::setw(20) << std::left << info.app_type
            << std::setw(20) << std::left << info.partition_count
            << std::setw(20) << std::left << (info.is_stateful ? "true" : "false")
            << std::endl;
    }
    out << std::endl << std::flush;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_nodes(const dsn::replication::node_status::type status, const std::string& file_name)
{
    std::shared_ptr<configuration_list_nodes_request> req(new configuration_list_nodes_request());
    req->status = status;

    auto resp_task = request_meta<configuration_list_nodes_request>(
            RPC_CM_LIST_NODES,
            req
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_list_nodes_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    // print configuration_list_nodes_response
    std::streambuf * buf;
    std::ofstream of;

    if(!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    out << std::setw(25) << std::left << "address"
        << std::setw(20) << std::left << "status"
        << std::endl;
    for(int i = 0; i < resp.infos.size(); i++)
    {
        dsn::replication::node_info info = resp.infos[i];
        std::string status_str = enum_to_string(info.status);
        status_str = status_str.substr(status_str.find("NS_") + 3);
        out << std::setw(25) << std::left << info.address.to_string()
            << std::setw(20) << std::left << status_str
            << std::endl;
    }
    out << std::endl << std::flush;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::cluster_info(const std::string& file_name)
{
    std::shared_ptr<configuration_cluster_info_request> req(new configuration_cluster_info_request());

    auto resp_task = request_meta<configuration_cluster_info_request>(
            RPC_CM_CLUSTER_INFO,
            req
    );
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    configuration_cluster_info_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    // print configuration_cluster_info_response
    std::streambuf * buf;
    std::ofstream of;

    if(!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    size_t width = 0;
    for(int i = 0; i < resp.keys.size(); i++)
    {
        if (resp.keys[i].size() > width)
            width = resp.keys[i].size();
    }

    for(int i = 0; i < resp.keys.size(); i++)
    {
        out << std::setw(width) << std::left << resp.keys[i]
            << " : " << resp.values[i] << std::endl;
    }
    out << std::endl << std::flush;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_app(const std::string& app_name, bool detailed, const std::string& file_name)
{
    int32_t app_id;
    int32_t partition_count;
    std::vector<partition_configuration> partitions;
    dsn::error_code err = list_app(app_name, app_id, partition_count, partitions);
    if(err != dsn::ERR_OK)
    {
        return err;
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

    int width = strlen("partition_count");
    out << std::setw(width) << std::left << "app_name" << " : " << app_name << std::endl;
    out << std::setw(width) << std::left << "app_id" << " : " << app_id << std::endl;
    out << std::setw(width) << std::left << "partition_count" << " : " << partition_count << std::endl;
    if(detailed)
    {
        std::map<rpc_address, std::pair<int, int> > node_stat;
        out << std::setw(width) << std::left << "details" << " : " << std::endl;
        out << std::setw(10) << std::left << "pidx"
            << std::setw(10) << std::left << "ballot"
            << std::setw(20) << std::left << "replica_count"
            << std::setw(25) << std::left << "primary"
            << std::setw(40) << std::left << "secondaries"
            << std::endl;
        for(int i = 0; i < partitions.size(); i++)
        {
            const dsn::partition_configuration& p = partitions[i];
            int replica_count = 0;
            if (!p.primary.is_invalid())
            {
                replica_count++;
                node_stat[p.primary].first++;
            }
            replica_count += p.secondaries.size();
            std::stringstream oss;
            oss << replica_count << "/" << p.max_replica_count;
            out << std::setw(10) << std::left << p.pid.get_partition_index()
                << std::setw(10) << std::left << p.ballot
                << std::setw(20) << std::left << oss.str()
                << std::setw(25) << std::left << p.primary.to_std_string()
                << std::left<< p.secondaries.size() << ":[";
            for(int j = 0; j < p.secondaries.size(); j++)
            {
                if(j!= 0)
                    out << ",";
                out << p.secondaries[j].to_std_string();
                node_stat[p.secondaries[j]].second++;
            }
            out << "]" << std::endl;
        }
        out << std::endl;
        out << std::setw(25) << std::left << "node"
            << std::setw(10) << std::left << "primary"
            << std::setw(10) << std::left << "secondary"
            << std::setw(10) << std::left << "total"
            << std::endl;
        for (auto& kv : node_stat)
        {
            out << std::setw(25) << std::left << kv.first.to_string()
                << std::setw(10) << std::left << kv.second.first
                << std::setw(10) << std::left << kv.second.second
                << std::setw(10) << std::left << (kv.second.first + kv.second.second)
                << std::endl;
        }
    }
    out << std::endl;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_app(const std::string& app_name,
                                                 int32_t& app_id, int32_t& partition_count,
                                                 std::vector<partition_configuration>& partitions)
{
    if(app_name.empty() || !std::all_of(app_name.cbegin(),app_name.cend(),(bool (*)(int)) replication_ddl_client::valid_app_char))
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

    dsn::configuration_query_by_index_response resp;
    dsn::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    app_id = resp.app_id;
    partition_count = resp.partition_count;
    partitions = resp.partitions;

    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::control_meta_balancer_migration(bool start)
{
    std::shared_ptr<control_balancer_migration_request> req(new control_balancer_migration_request());
    req->enable_migration = start;

    auto response_task = request_meta<control_balancer_migration_request>(
        RPC_CM_CONTROL_BALANCER_MIGRATION,
        req);
    response_task->wait();
    if ( response_task->error() != dsn::ERR_OK)
        return response_task->error();
    dsn::replication::control_balancer_migration_response resp;
    ::dsn::unmarshall(response_task->response(), resp);
    return resp.err;
}

dsn::error_code replication_ddl_client::send_balancer_proposal(const balancer_proposal_request &request)
{
    std::shared_ptr<balancer_proposal_request> req(new balancer_proposal_request(request));

    auto response_task = request_meta<balancer_proposal_request>(
        RPC_CM_BALANCER_PROPOSAL,
        req);
    response_task->wait();
    if ( response_task->error() != dsn::ERR_OK)
        return response_task->error();
    dsn::replication::balancer_proposal_response resp;
    ::dsn::unmarshall(response_task->response(), resp);
    return resp.err;
}

bool replication_ddl_client::valid_app_char(int c)
{
    return (bool)std::isalnum(c) || c == '_' || c == '.';
}

void replication_ddl_client::end_meta_request(task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp)
{
    if(err != dsn::ERR_OK && retry_times < 2)
    {
        rpc::call(
            _meta_server,
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
    {
        callback->enqueue_rpc_response(err, resp);
    }
}

}} // namespace
