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
#include <boost/lexical_cast.hpp>
#include <dsn/dist/error_code.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <iostream>
#include <fstream>
#include <iomanip>

#include <boost/lexical_cast.hpp>

#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

namespace dsn {
namespace replication {

std::string replication_ddl_client::hostname_from_ip(uint32_t ip)
{
    struct sockaddr_in addr_in;
    addr_in.sin_family = AF_INET;
    addr_in.sin_port = 0;
    addr_in.sin_addr.s_addr = ip;

    char hostname[256];
    int err = getnameinfo((struct sockaddr *)(&addr_in),
                          sizeof(struct sockaddr),
                          hostname,
                          sizeof(hostname),
                          nullptr,
                          0,
                          NI_NAMEREQD);
    if (err != 0) {
        struct in_addr net_addr;
        net_addr.s_addr = ip;
        char ip_str[256];
        inet_ntop(AF_INET, &net_addr, ip_str, sizeof(ip_str));

        if (err == EAI_SYSTEM) {
            dwarn("got error %s when try to resolve %s", strerror(errno), ip_str);
        } else {
            dwarn("return error(%s) when try to resolve %s", gai_strerror(err), ip_str);
        }
        return std::string("UNRESOLVABLE");
    } else {
        return std::string(hostname);
    }
}

std::string replication_ddl_client::hostname_from_ip(const char *ip)
{
    uint32_t ip_addr;
    inet_pton(AF_INET, ip, &ip_addr);
    return hostname_from_ip(ip_addr);
}

std::string replication_ddl_client::hostname_from_ip_port(const char *ip_port)
{
    dsn::rpc_address addr;
    if (!addr.from_string_ipv4(ip_port)) {
        dwarn("invalid ip_port(%s)", ip_port);
        return std::string("UNRESOLVABLE");
    }
    return hostname(addr);
}

std::string replication_ddl_client::hostname(const rpc_address &address)
{
    if (address.type() != HOST_TYPE_IPV4) {
        return std::string("invalid");
    }
    return hostname_from_ip(htonl(address.ip())) + ":" +
           boost::lexical_cast<std::string>(address.port());
}

std::string replication_ddl_client::list_hostname_from_ip(const char *ip_list)
{
    std::vector<std::string> splitted_ip;
    dsn::utils::split_args(ip_list, splitted_ip, ',');

    if (splitted_ip.empty()) {
        dwarn("invalid ip_list(%s)", ip_list);
        return std::string("UNRESOLVABLE");
    }

    std::stringstream result;
    result << hostname_from_ip(splitted_ip[0].c_str());
    for (int i = 1; i < splitted_ip.size(); ++i) {
        result << "," << hostname_from_ip(splitted_ip[i].c_str());
    }
    return result.str();
}

std::string replication_ddl_client::list_hostname_from_ip_port(const char *ip_port_list)
{
    std::vector<std::string> splitted_ip_port;
    dsn::utils::split_args(ip_port_list, splitted_ip_port, ',');
    if (splitted_ip_port.empty()) {
        dwarn("invalid ip_port_list(%s)", ip_port_list);
        return std::string("UNRESOLVABLE");
    }

    std::stringstream result;
    result << hostname_from_ip_port(splitted_ip_port[0].c_str());
    for (int i = 1; i < splitted_ip_port.size(); ++i) {
        result << "," << hostname_from_ip_port(splitted_ip_port[i].c_str());
    }
    return result.str();
}

replication_ddl_client::replication_ddl_client(const std::vector<dsn::rpc_address> &meta_servers)
{
    _meta_server.assign_group(dsn_group_build("meta-servers"));
    for (auto &m : meta_servers) {
        dsn_group_add(_meta_server.group_handle(), m.c_addr());
    }
}

replication_ddl_client::~replication_ddl_client()
{
    dsn_group_destroy(_meta_server.group_handle());
}

dsn::error_code replication_ddl_client::wait_app_ready(const std::string &app_name,
                                                       int partition_count,
                                                       int max_replica_count)
{
    int sleep_sec = 2;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));

        std::shared_ptr<configuration_query_by_index_request> query_req(
            new configuration_query_by_index_request());
        query_req->app_name = app_name;

        auto query_task = request_meta<configuration_query_by_index_request>(
            RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, query_req);
        query_task->wait();
        if (query_task->error() == ERR_INVALID_STATE) {
            std::cout << app_name << " not ready yet, still waiting..." << std::endl;
            continue;
        }

        if (query_task->error() != dsn::ERR_OK) {
            std::cout << "create app " << app_name
                      << " failed: [query] call server error: " << query_task->error().to_string()
                      << std::endl;
            return query_task->error();
        }

        dsn::configuration_query_by_index_response query_resp;
        ::dsn::unmarshall(query_task->response(), query_resp);
        if (query_resp.err != dsn::ERR_OK) {
            std::cout << "create app " << app_name
                      << " failed: [query] received server error: " << query_resp.err.to_string()
                      << std::endl;
            return query_resp.err;
        }
        dassert(partition_count == query_resp.partition_count, "partition count not equal");
        int ready_count = 0;
        for (int i = 0; i < partition_count; i++) {
            const partition_configuration &pc = query_resp.partitions[i];
            if (!pc.primary.is_invalid() && (pc.secondaries.size() + 1 >= max_replica_count)) {
                ready_count++;
            }
        }
        if (ready_count == partition_count) {
            std::cout << app_name << " is ready now: (" << ready_count << "/" << partition_count
                      << ")" << std::endl;
            break;
        }
        std::cout << app_name << " not ready yet, still waiting... (" << ready_count << "/"
                  << partition_count << ")" << std::endl;
    }
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::create_app(const std::string &app_name,
                                                   const std::string &app_type,
                                                   int partition_count,
                                                   int replica_count,
                                                   const std::map<std::string, std::string> &envs,
                                                   bool is_stateless)
{
    if (partition_count < 1) {
        std::cout << "create app " << app_name << " failed: partition_count should >= 1"
                  << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if (replica_count < 3) {
        std::cout << "create app " << app_name << " failed: replica_count should >= 3" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if (app_name.empty() ||
        !std::all_of(app_name.cbegin(),
                     app_name.cend(),
                     (bool (*)(int))replication_ddl_client::valid_app_char)) {
        std::cout << "create app " << app_name << " failed: invalid app_name" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }

    if (app_type.empty() ||
        !std::all_of(app_type.cbegin(),
                     app_type.cend(),
                     (bool (*)(int))replication_ddl_client::valid_app_char)) {
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

    auto resp_task = request_meta<configuration_create_app_request>(RPC_CM_CREATE_APP, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        std::cout << "create app " << app_name
                  << " failed: [create] call server error: " << resp_task->error().to_string()
                  << std::endl;
        return resp_task->error();
    }

    dsn::replication::configuration_create_app_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        std::cout << "create app " << app_name
                  << " failed: [create] received server error: " << resp.err.to_string()
                  << std::endl;
        return resp.err;
    }

    std::cout << "create app " << app_name << " succeed, waiting for app ready" << std::endl;

    dsn::error_code error = wait_app_ready(app_name, partition_count, replica_count);
    if (error == dsn::ERR_OK)
        std::cout << app_name << " is ready now!" << std::endl;
    return error;
}

dsn::error_code replication_ddl_client::drop_app(const std::string &app_name, int reserve_seconds)
{
    if (app_name.empty() ||
        !std::all_of(app_name.cbegin(),
                     app_name.cend(),
                     (bool (*)(int))replication_ddl_client::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_drop_app_request> req(new configuration_drop_app_request());
    req->app_name = app_name;
    req->options.success_if_not_exist = true;
    req->options.__set_reserve_seconds(reserve_seconds);

    auto resp_task = request_meta<configuration_drop_app_request>(RPC_CM_DROP_APP, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::replication::configuration_drop_app_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::recall_app(int32_t app_id, const std::string &new_app_name)
{
    if (!std::all_of(new_app_name.cbegin(),
                     new_app_name.cend(),
                     (bool (*)(int))replication_ddl_client::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_recall_app_request> req =
        std::make_shared<configuration_recall_app_request>();
    req->app_id = app_id;
    req->new_app_name = new_app_name;

    auto resp_task = request_meta<configuration_recall_app_request>(RPC_CM_RECALL_APP, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK)
        return resp_task->error();

    dsn::replication::configuration_recall_app_response resp;
    dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK)
        return resp.err;
    std::cout << "recall app ok, id(" << resp.info.app_id << "), "
              << "name(" << resp.info.app_name << "), "
              << "partition_count(" << resp.info.partition_count << "), wait it ready" << std::endl;
    return wait_app_ready(
        resp.info.app_name, resp.info.partition_count, resp.info.max_replica_count);
}

dsn::error_code replication_ddl_client::list_apps(const dsn::app_status::type status,
                                                  std::vector<::dsn::app_info> &apps)
{
    std::shared_ptr<configuration_list_apps_request> req(new configuration_list_apps_request());
    req->status = status;

    auto resp_task = request_meta<configuration_list_apps_request>(RPC_CM_LIST_APPS, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::replication::configuration_list_apps_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    apps = resp.infos;

    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_apps(const dsn::app_status::type status,
                                                  bool show_all,
                                                  bool detailed,
                                                  const std::string &file_name)
{
    std::vector<::dsn::app_info> apps;
    auto r = list_apps(status, apps);
    if (r != dsn::ERR_OK) {
        return r;
    }

    // print configuration_list_apps_response
    std::streambuf *buf;
    std::ofstream of;

    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    size_t max_app_name_size = 20;
    for (int i = 0; i < apps.size(); i++) {
        dsn::app_info info = apps[i];
        max_app_name_size = std::max(max_app_name_size, info.app_name.size() + 2);
    }

    out << std::setw(10) << std::left << "app_id" << std::setw(20) << std::left << "status"
        << std::setw(max_app_name_size) << std::left << "app_name" << std::setw(20) << std::left
        << "app_type" << std::setw(20) << std::left << "partition_count" << std::setw(20)
        << std::left << "replica_count" << std::setw(20) << std::left << "is_stateful"
        << std::setw(20) << std::left << "settings" << std::setw(20) << std::left
        << "drop_expire_time" << std::endl;
    int available_app_count = 0;
    for (int i = 0; i < apps.size(); i++) {
        dsn::app_info info = apps[i];
        if (!show_all && info.status != app_status::AS_AVAILABLE) {
            continue;
        }
        std::string status_str = enum_to_string(info.status);
        status_str = status_str.substr(status_str.find("AS_") + 3);
        std::string settings = "{";
        for (auto kv : info.envs) {
            settings += (kv.first + ":" + kv.second + ",");
        }
        if (settings.length() > 1) {
            settings.back() = '}';
        } else {
            settings = "{}";
        }
        std::string drop_expire_time = "-";
        if (info.status == app_status::AS_AVAILABLE) {
            available_app_count++;
        } else if (info.status == app_status::AS_DROPPED && info.expire_second > 0) {
            char buf[20];
            dsn::utils::time_ms_to_date_time((uint64_t)info.expire_second * 1000, buf, 20);
            drop_expire_time = buf;
        }
        out << std::setw(10) << std::left << info.app_id << std::setw(20) << std::left << status_str
            << std::setw(max_app_name_size) << std::left << info.app_name << std::setw(20)
            << std::left << info.app_type << std::setw(20) << std::left << info.partition_count
            << std::setw(20) << std::left << info.max_replica_count << std::setw(20) << std::left
            << (info.is_stateful ? "true" : "false") << std::setw(20) << std::left << settings
            << std::setw(20) << std::left << drop_expire_time << std::endl;
    }
    out << std::endl << std::flush;

    if (detailed && available_app_count > 0) {
        out << "[App Healthy Info]" << std::endl;
        out << std::setw(10) << std::left << "app_id" << std::setw(max_app_name_size) << std::left
            << "app_name" << std::setw(20) << std::left << "partition_count" << std::setw(20)
            << std::left << "fully_healthy_num" << std::setw(20) << std::left
            << "partly_healthy_num" << std::setw(20) << std::left << "unhealthy_num"
            << std::setw(20) << std::left << "is_app_healthy" << std::endl;
        for (auto &info : apps) {
            if (info.status != app_status::AS_AVAILABLE) {
                continue;
            }
            int32_t app_id;
            int32_t partition_count;
            std::vector<partition_configuration> partitions;
            r = list_app(info.app_name, app_id, partition_count, partitions);
            if (r != dsn::ERR_OK) {
                derror("list app(%s) failed, err = %s", info.app_name.c_str(), r.to_string());
                return r;
            }
            dassert(info.app_id == app_id, "invalid app_id, %d VS %d", info.app_id, app_id);
            dassert(info.partition_count == partition_count,
                    "invalid partition_count, %d VS %d",
                    info.partition_count,
                    partition_count);
            int fully_healthy = 0;
            int partly_healthy = 0;
            for (int i = 0; i < partitions.size(); i++) {
                const dsn::partition_configuration &p = partitions[i];
                int replica_count = 0;
                if (!p.primary.is_invalid()) {
                    replica_count++;
                }
                replica_count += p.secondaries.size();
                if (!p.primary.is_invalid()) {
                    if (replica_count >= p.max_replica_count)
                        fully_healthy++;
                    else if (replica_count >= 2)
                        partly_healthy++;
                }
            }
            int unhealthy = info.partition_count - fully_healthy - partly_healthy;
            out << std::setw(10) << std::left << info.app_id << std::setw(max_app_name_size)
                << std::left << info.app_name << std::setw(20) << std::left << info.partition_count
                << std::setw(20) << std::left << fully_healthy << std::setw(20) << std::left
                << partly_healthy << std::setw(20) << std::left << unhealthy << std::setw(20)
                << std::left << (unhealthy > 0 ? "false" : "true") << std::endl;
        }
        out << std::endl << std::flush;
    }

    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_nodes(
    const dsn::replication::node_status::type status,
    std::map<dsn::rpc_address, dsn::replication::node_status::type> &nodes)
{
    std::shared_ptr<configuration_list_nodes_request> req(new configuration_list_nodes_request());
    req->status = status;

    auto resp_task = request_meta<configuration_list_nodes_request>(RPC_CM_LIST_NODES, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::replication::configuration_list_nodes_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    for (const dsn::replication::node_info &n : resp.infos) {
        nodes[n.address] = n.status;
    }

    return dsn::ERR_OK;
}

struct list_nodes_helper
{
    std::string node_name;
    std::string node_status;
    int primary_count;
    int secondary_count;
    list_nodes_helper(const std::string &n, const std::string &s)
        : node_name(n), node_status(s), primary_count(0), secondary_count(0)
    {
    }
};

dsn::error_code replication_ddl_client::list_nodes(const dsn::replication::node_status::type status,
                                                   bool detailed,
                                                   const std::string &file_name,
                                                   bool resolve_ip)
{
#define RESOLVE(value) (resolve_ip ? hostname_from_ip_port(value.c_str()) : value)
    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto r = list_nodes(status, nodes);
    if (r != dsn::ERR_OK) {
        return r;
    }

    std::map<dsn::rpc_address, list_nodes_helper> tmp_map;
    int node_name_width = 0;
    for (auto &kv : nodes) {
        std::string status_str = enum_to_string(kv.second);
        status_str = status_str.substr(status_str.find("NS_") + 3);
        auto result = tmp_map.emplace(
            kv.first, list_nodes_helper(RESOLVE(kv.first.to_std_string()), status_str));
        node_name_width = std::max(node_name_width, (int)result.first->second.node_name.size());
    }

    if (detailed) {
        std::vector<::dsn::app_info> apps;
        r = list_apps(dsn::app_status::AS_AVAILABLE, apps);
        if (r != dsn::ERR_OK) {
            return r;
        }

        for (auto &app : apps) {
            int32_t app_id;
            int32_t partition_count;
            std::vector<partition_configuration> partitions;
            r = list_app(app.app_name, app_id, partition_count, partitions);
            if (r != dsn::ERR_OK) {
                return r;
            }

            for (int i = 0; i < partitions.size(); i++) {
                const dsn::partition_configuration &p = partitions[i];
                if (!p.primary.is_invalid()) {
                    auto find = tmp_map.find(p.primary);
                    if (find != tmp_map.end()) {
                        find->second.primary_count++;
                    }
                }
                for (int j = 0; j < p.secondaries.size(); j++) {
                    auto find = tmp_map.find(p.secondaries[j]);
                    if (find != tmp_map.end()) {
                        find->second.secondary_count++;
                    }
                }
            }
        }
    }

    // print configuration_list_nodes_response
    std::streambuf *buf;
    std::ofstream of;

    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    if (detailed) {
        out << std::setw(node_name_width + 5) << std::left << "address" << std::setw(20)
            << std::left << "status" << std::setw(20) << std::left << "replica_count"
            << std::setw(20) << std::left << "primary_count" << std::setw(20) << std::left
            << "secondary_count" << std::endl;
        for (auto &kv : tmp_map) {
            out << std::setw(node_name_width + 5) << std::left << kv.second.node_name
                << std::setw(20) << std::left << kv.second.node_status << std::setw(20) << std::left
                << kv.second.primary_count + kv.second.secondary_count << std::setw(20) << std::left
                << kv.second.primary_count << std::setw(20) << std::left
                << kv.second.secondary_count << std::endl;
        }
    } else {
        out << std::setw(node_name_width + 5) << std::left << "address" << std::setw(20)
            << std::left << "status" << std::endl;
        for (auto &kv : tmp_map) {
            out << std::setw(node_name_width + 5) << std::left << kv.second.node_name
                << std::setw(20) << std::left << kv.second.node_status << std::endl;
        }
    }
    out << std::endl << std::flush;

    return dsn::ERR_OK;
#undef RESOLVE
}

dsn::error_code replication_ddl_client::cluster_info(const std::string &file_name, bool resolve_ip)
{
    std::shared_ptr<configuration_cluster_info_request> req(
        new configuration_cluster_info_request());

    auto resp_task = request_meta<configuration_cluster_info_request>(RPC_CM_CLUSTER_INFO, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    configuration_cluster_info_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    // print configuration_cluster_info_response
    std::streambuf *buf;
    std::ofstream of;

    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    size_t width = 0;
    for (int i = 0; i < resp.keys.size(); i++) {
        if (resp.keys[i].size() > width)
            width = resp.keys[i].size();
    }

    if (resolve_ip) {
        for (int i = 0; i < resp.keys.size(); ++i) {
            if (resp.keys[i] == "meta_servers") {
                resp.values[i] = list_hostname_from_ip_port(resp.values[i].c_str());
            } else if (resp.keys[i] == "primary_meta_server") {
                resp.values[i] = hostname_from_ip_port(resp.values[i].c_str());
            }
        }
    }

    for (int i = 0; i < resp.keys.size(); i++) {
        out << std::setw(width) << std::left << resp.keys[i] << " : " << resp.values[i]
            << std::endl;
    }
    out << std::endl << std::flush;
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_app(const std::string &app_name,
                                                 bool detailed,
                                                 const std::string &file_name,
                                                 bool resolve_ip)
{
#define RESOLVE(value) (resolve_ip ? hostname_from_ip_port(value.c_str()) : value)
    int32_t app_id = 0;
    int32_t partition_count = 0;
    int32_t max_replica_count = 0;
    std::vector<partition_configuration> partitions;
    dsn::error_code err = list_app(app_name, app_id, partition_count, partitions);
    if (err != dsn::ERR_OK) {
        return err;
    }
    if (!partitions.empty()) {
        max_replica_count = partitions[0].max_replica_count;
    }

    // print configuration_query_by_index_response
    std::streambuf *buf;
    std::ofstream of;

    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    int width = strlen("max_replica_count");
    out << std::setw(width) << std::left << "app_name"
        << " : " << app_name << std::endl;
    out << std::setw(width) << std::left << "app_id"
        << " : " << app_id << std::endl;
    out << std::setw(width) << std::left << "partition_count"
        << " : " << partition_count << std::endl;
    out << std::setw(width) << std::left << "max_replica_count"
        << " : " << max_replica_count << std::endl;
    if (detailed) {
        std::map<rpc_address, std::pair<int, int>> node_stat;
        out << std::setw(width) << std::left << "details"
            << " : " << std::endl;
        out << std::setw(10) << std::left << "pidx" << std::setw(10) << std::left << "ballot"
            << std::setw(20) << std::left << "replica_count" << std::setw(40) << std::left
            << "primary" << std::setw(40) << std::left << "secondaries" << std::endl;
        int total_prim_count = 0;
        int total_sec_count = 0;
        int total_fully_healthy_partition = 0;
        int total_partly_healthy_partition = 0;
        for (int i = 0; i < partitions.size(); i++) {
            const dsn::partition_configuration &p = partitions[i];
            int replica_count = 0;
            if (!p.primary.is_invalid()) {
                replica_count++;
                node_stat[p.primary].first++;
                total_prim_count++;
            }
            replica_count += p.secondaries.size();
            total_sec_count += p.secondaries.size();
            if (!p.primary.is_invalid()) {
                if (replica_count >= p.max_replica_count)
                    total_fully_healthy_partition++;
                else if (replica_count >= 2)
                    total_partly_healthy_partition++;
            }
            std::stringstream oss;
            oss << replica_count << "/" << p.max_replica_count;
            out << std::setw(10) << std::left << p.pid.get_partition_index() << std::setw(10)
                << std::left << p.ballot << std::setw(20) << std::left << oss.str() << std::setw(40)
                << std::left << (p.primary.is_invalid() ? "-" : RESOLVE(p.primary.to_std_string()))
                << std::left << "[";
            for (int j = 0; j < p.secondaries.size(); j++) {
                if (j != 0)
                    out << ",";
                out << RESOLVE(p.secondaries[j].to_std_string());
                node_stat[p.secondaries[j]].second++;
            }
            out << "]" << std::endl;
        }
        out << std::endl;
        out << std::setw(40) << std::left << "node" << std::setw(10) << std::left << "primary"
            << std::setw(10) << std::left << "secondary" << std::setw(10) << std::left << "total"
            << std::endl;
        for (auto &kv : node_stat) {
            out << std::setw(40) << std::left << RESOLVE(kv.first.to_std_string()) << std::setw(10)
                << std::left << kv.second.first << std::setw(10) << std::left << kv.second.second
                << std::setw(10) << std::left << (kv.second.first + kv.second.second) << std::endl;
        }
        out << std::setw(40) << std::left << "" << std::setw(10) << std::left << total_prim_count
            << std::setw(10) << std::left << total_sec_count << std::setw(10) << std::left
            << total_prim_count + total_sec_count << std::endl;
        out << std::endl;
        width = strlen("partly_healthy_partition_count");
        out << std::setw(width) << std::left << "fully_healthy_partition_count"
            << " : " << total_fully_healthy_partition << std::endl;
        out << std::setw(width) << std::left << "partly_healthy_partition_count"
            << " : " << total_partly_healthy_partition << std::endl;
        out << std::setw(width) << std::left << "unhealthy_partition_count"
            << " : "
            << (partition_count - total_fully_healthy_partition - total_partly_healthy_partition)
            << std::endl;
    }
    out << std::endl;
    return dsn::ERR_OK;
#undef RESOLVE
}

dsn::error_code replication_ddl_client::list_app(const std::string &app_name,
                                                 int32_t &app_id,
                                                 int32_t &partition_count,
                                                 std::vector<partition_configuration> &partitions)
{
    if (app_name.empty() ||
        !std::all_of(app_name.cbegin(),
                     app_name.cend(),
                     (bool (*)(int))replication_ddl_client::valid_app_char))
        return ERR_INVALID_PARAMETERS;

    std::shared_ptr<configuration_query_by_index_request> req(
        new configuration_query_by_index_request());
    req->app_name = app_name;

    auto resp_task = request_meta<configuration_query_by_index_request>(
        RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, req);

    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::configuration_query_by_index_response resp;
    dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    app_id = resp.app_id;
    partition_count = resp.partition_count;
    partitions = resp.partitions;

    return dsn::ERR_OK;
}

dsn::replication::configuration_meta_control_response
replication_ddl_client::control_meta_function_level(meta_function_level::type level)
{
    std::shared_ptr<configuration_meta_control_request> req =
        std::make_shared<configuration_meta_control_request>();
    req->level = level;

    auto response_task = request_meta<configuration_meta_control_request>(RPC_CM_CONTROL_META, req);
    response_task->wait();
    configuration_meta_control_response resp;
    if (response_task->error() != dsn::ERR_OK) {
        resp.err = response_task->error();
    } else {
        dsn::unmarshall(response_task->response(), resp);
    }
    return resp;
}

dsn::error_code
replication_ddl_client::send_balancer_proposal(const configuration_balancer_request &request)
{
    std::shared_ptr<configuration_balancer_request> req =
        std::make_shared<configuration_balancer_request>(request);

    auto response_task = request_meta<configuration_balancer_request>(RPC_CM_PROPOSE_BALANCER, req);
    response_task->wait();
    if (response_task->error() != dsn::ERR_OK)
        return response_task->error();
    dsn::replication::configuration_balancer_response resp;
    dsn::unmarshall(response_task->response(), resp);
    return resp.err;
}

dsn::error_code replication_ddl_client::do_recovery(const std::vector<rpc_address> &replica_nodes,
                                                    int wait_seconds,
                                                    bool skip_bad_nodes,
                                                    bool skip_lost_partitions,
                                                    const std::string &outfile)
{
    std::streambuf *buf;
    std::ofstream of;

    if (!outfile.empty()) {
        of.open(outfile);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    std::shared_ptr<configuration_recovery_request> req =
        std::make_shared<configuration_recovery_request>();
    req->recovery_set.clear();
    for (const dsn::rpc_address &node : replica_nodes) {
        if (std::find(req->recovery_set.begin(), req->recovery_set.end(), node) !=
            req->recovery_set.end()) {
            out << "duplicate replica node " << node.to_string() << ", just ingore it" << std::endl;
        } else {
            req->recovery_set.push_back(node);
        }
    }
    if (req->recovery_set.empty()) {
        out << "node set for recovery it empty" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }
    req->skip_bad_nodes = skip_bad_nodes;
    req->skip_lost_partitions = skip_lost_partitions;

    out << "Wait seconds: " << wait_seconds << std::endl;
    out << "Skip bad nodes: " << (skip_bad_nodes ? "true" : "false") << std::endl;
    out << "Skip lost partitions: " << (skip_lost_partitions ? "true" : "false") << std::endl;
    out << "Node list:" << std::endl;
    out << "=============================" << std::endl;
    for (auto &node : req->recovery_set) {
        out << node.to_string() << std::endl;
    }
    out << "=============================" << std::endl;

    auto response_task = request_meta<configuration_recovery_request>(
        RPC_CM_START_RECOVERY, req, wait_seconds * 1000);
    bool wait_done = false;
    for (int i = 0; i < wait_seconds; ++i) {
        wait_done = response_task->wait(1000);
        if (wait_done)
            break;
        else
            out << "Wait recovery for " << i << " seconds" << std::endl;
    }

    if (!wait_done || response_task->response() == NULL) {
        out << "Wait recovery failed, administrator should check the meta for progress"
            << std::endl;
        return dsn::ERR_TIMEOUT;
    } else {
        configuration_recovery_response resp;
        dsn::unmarshall(response_task->response(), resp);
        out << "Recover result: " << resp.err.to_string() << std::endl;
        if (!resp.hint_message.empty()) {
            out << "=============================" << std::endl;
            out << resp.hint_message;
            out << "=============================" << std::endl;
        }
        return resp.err;
    }
}

dsn::error_code replication_ddl_client::do_restore(const std::string &backup_provider_name,
                                                   const std::string &cluster_name,
                                                   const std::string &policy_name,
                                                   int64_t timestamp,
                                                   const std::string &old_app_name,
                                                   int32_t old_app_id,
                                                   const std::string &new_app_name,
                                                   bool skip_bad_partition)
{
    std::shared_ptr<configuration_restore_request> req =
        std::make_shared<configuration_restore_request>();

    req->cluster_name = cluster_name;
    req->policy_name = policy_name;
    req->app_name = old_app_name;
    req->app_id = old_app_id;
    req->new_app_name = new_app_name;
    req->backup_provider_name = backup_provider_name;
    req->time_stamp = timestamp;
    req->skip_bad_partition = skip_bad_partition;

    auto resp_task = request_meta<configuration_restore_request>(RPC_CM_START_RESTORE, req);
    bool finish = false;
    while (!finish) {
        std::cout << "sleep 1 second to wait complete..." << std::endl;
        finish = resp_task->wait(1000);
    }

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    } else {
        configuration_create_app_response resp;
        dsn::unmarshall(resp_task->response(), resp);
        if (resp.err == ERR_OBJECT_NOT_FOUND) {
            std::cout << "app metadata is damaged on cold backup media, restore app failed"
                      << std::endl;
            return ERR_OK;
        } else if (resp.err == ERR_OK) {
            std::cout << "\t"
                      << "new app_id = " << resp.appid << std::endl;
        }
        return resp.err;
    }
}

dsn::error_code replication_ddl_client::add_backup_policy(const std::string &policy_name,
                                                          const std::string &backup_provider_type,
                                                          const std::vector<int32_t> &app_ids,
                                                          int64_t backup_interval_seconds,
                                                          int32_t backup_history_cnt,
                                                          const std::string &start_time)
{
    std::shared_ptr<configuration_add_backup_policy_request> req =
        std::make_shared<configuration_add_backup_policy_request>();
    req->policy_name = policy_name;
    req->backup_provider_type = backup_provider_type;
    req->app_ids = app_ids;
    req->backup_interval_seconds = backup_interval_seconds;
    req->backup_history_count_to_keep = backup_history_cnt;
    req->start_time = start_time;
    auto resp_task =
        request_meta<configuration_add_backup_policy_request>(RPC_CM_ADD_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_add_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);

    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        std::cout << "add backup policy succeed, policy_name = " << policy_name << std::endl;
    }
    return ERR_OK;
}

dsn::error_code replication_ddl_client::disable_backup_policy(const std::string &policy_name)
{
    std::shared_ptr<configuration_modify_backup_policy_request> req =
        std::make_shared<configuration_modify_backup_policy_request>();
    req->policy_name = policy_name;
    req->__set_is_disable(true);

    auto resp_task =
        request_meta<configuration_modify_backup_policy_request>(RPC_CM_MODIFY_BACKUP_POLICY, req);

    resp_task->wait();
    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        std::cout << "disable policy result: " << resp.err.to_string() << std::endl;
        if (!resp.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << resp.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
        return resp.err;
    }
}

dsn::error_code replication_ddl_client::enable_backup_policy(const std::string &policy_name)
{
    std::shared_ptr<configuration_modify_backup_policy_request> req =
        std::make_shared<configuration_modify_backup_policy_request>();
    req->policy_name = policy_name;
    req->__set_is_disable(false);

    auto resp_task =
        request_meta<configuration_modify_backup_policy_request>(RPC_CM_MODIFY_BACKUP_POLICY, req);

    resp_task->wait();
    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != ERR_OK) {
        return resp.err;
    } else if (resp.err == ERR_BUSY) {
        std::cout << "policy is under backup, please try disable later" << std::endl;
        return ERR_OK;
    } else {
        std::cout << "enable policy result: " << resp.err.to_string() << std::endl;
        if (!resp.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << resp.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
        return resp.err;
    }
}

// help functions

template <typename T>
// make sure T support cout << T;
std::string print_set(const std::set<T> &set)
{
    std::stringstream ss;
    ss << "{";
    auto begin = set.begin();
    auto end = set.end();
    for (auto it = begin; it != end; it++) {
        if (it != begin) {
            ss << ", ";
        }
        ss << *it;
    }
    ss << "}";
    return ss.str();
}

static void print_policy_entry(const policy_entry &entry)
{
    int width = strlen("backup_provider_type");

    std::cout << "    " << std::setw(width) << std::left << "name"
              << " : " << entry.policy_name << std::endl
              << "    " << std::setw(width) << std::left << "backup_provider_type"
              << " : " << entry.backup_provider_type << std::endl
              << "    " << std::setw(width) << std::left << "backup_interval"
              << " : " << entry.backup_interval_seconds << "s" << std::endl
              << "    " << std::setw(width) << std::left << "app_ids"
              << " : " << print_set(entry.app_ids) << std::endl;

    std::string status = (entry.is_disable) ? std::string("disabled") : std::string("enabled");
    std::cout << "    " << std::setw(width) << std::left << "start_time"
              << " : " << entry.start_time << std::endl
              << "    " << std::setw(width) << std::left << "status"
              << " : " << status << std::endl
              << "    " << std::setw(width) << std::left << "backup_history_count"
              << " : " << entry.backup_history_count_to_keep << std::endl;
}

static void print_backup_entry(const backup_entry &bentry)
{
    int width = strlen("start_time");

    char start_time[30] = {'\0'};
    char end_time[30] = {'\0'};
    ::dsn::utils::time_ms_to_date_time(bentry.start_time_ms, start_time, 30);
    if (bentry.end_time_ms == 0) {
        end_time[0] = '-';
        end_time[1] = '\0';
    } else {
        ::dsn::utils::time_ms_to_date_time(bentry.end_time_ms, end_time, 30);
    }

    std::cout << "    " << std::setw(width) << std::left << "id"
              << " : " << bentry.backup_id << std::endl
              << "    " << std::setw(width) << std::left << "start_time"
              << " : " << start_time << std::endl
              << "    " << std::setw(width) << std::left << "end_time"
              << " : " << end_time << std::endl
              << "    " << std::setw(width) << std::left << "app_ids"
              << " : " << print_set(bentry.app_ids) << std::endl;
}

dsn::error_code replication_ddl_client::ls_backup_policy()
{
    std::shared_ptr<configuration_query_backup_policy_request> req =
        std::make_shared<configuration_query_backup_policy_request>();
    req->policy_names.clear();
    req->backup_info_count = 0;

    auto resp_task =
        request_meta<configuration_query_backup_policy_request>(RPC_CM_QUERY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }
    configuration_query_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);

    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        for (int32_t idx = 0; idx < resp.policys.size(); idx++) {
            std::cout << "[" << idx + 1 << "]" << std::endl;
            print_policy_entry(resp.policys[idx]);
            std::cout << std::endl;
        }
    }
    return ERR_OK;
}

dsn::error_code
replication_ddl_client::query_backup_policy(const std::vector<std::string> &policy_names,
                                            int backup_info_cnt)
{
    std::shared_ptr<configuration_query_backup_policy_request> req =
        std::make_shared<configuration_query_backup_policy_request>();
    req->policy_names = policy_names;
    req->backup_info_count = backup_info_cnt;

    auto resp_task =
        request_meta<configuration_query_backup_policy_request>(RPC_CM_QUERY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_query_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);

    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        for (int32_t idx = 0; idx < resp.policys.size(); idx++) {
            if (idx != 0) {
                std::cout << "************************" << std::endl;
            }
            const policy_entry &pentry = resp.policys[idx];
            std::cout << "policy_info:" << std::endl;
            print_policy_entry(pentry);
            std::cout << std::endl << "backup_infos:" << std::endl;
            const std::vector<backup_entry> &backup_infos = resp.backup_infos[idx];
            for (int idx = 0; idx < backup_infos.size(); idx++) {
                std::cout << "[" << (idx + 1) << "]" << std::endl;
                print_backup_entry(backup_infos[idx]);
            }
        }
    }
    return ERR_OK;
}

dsn::error_code
replication_ddl_client::update_backup_policy(const std::string &policy_name,
                                             const std::vector<int32_t> &add_appids,
                                             const std::vector<int32_t> &removal_appids,
                                             int64_t new_backup_interval_sec,
                                             int32_t backup_history_count_to_keep,
                                             const std::string &start_time)
{
    std::shared_ptr<configuration_modify_backup_policy_request> req =
        std::make_shared<configuration_modify_backup_policy_request>();
    req->policy_name = policy_name;
    if (!add_appids.empty()) {
        req->__set_add_appids(add_appids);
    }
    if (!removal_appids.empty()) {
        req->__set_removal_appids(removal_appids);
    }
    if (new_backup_interval_sec > 0) {
        req->__set_new_backup_interval_sec(new_backup_interval_sec);
    }

    if (backup_history_count_to_keep > 0) {
        req->__set_backup_history_count_to_keep(backup_history_count_to_keep);
    }

    if (!start_time.empty()) {
        req->__set_start_time(start_time);
    }
    auto resp_task =
        request_meta<configuration_modify_backup_policy_request>(RPC_CM_MODIFY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->response(), resp);
    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        std::cout << "Modify policy result: " << resp.err.to_string() << std::endl;
        if (!resp.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << resp.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
        return resp.err;
    }
}

dsn::error_code replication_ddl_client::query_restore(int32_t restore_app_id, bool detailed)
{
    if (restore_app_id <= 0) {
        return ERR_INVALID_PARAMETERS;
    }
    std::shared_ptr<configuration_query_restore_request> req =
        std::make_shared<configuration_query_restore_request>();
    req->restore_app_id = restore_app_id;

    auto resp_task =
        request_meta<configuration_query_restore_request>(RPC_CM_QUERY_RESTORE_STATUS, req);

    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_query_restore_response response;
    ::dsn::unmarshall(resp_task->response(), response);
    if (response.err == ERR_OK) {
        int overall_progress = 0;
        for (const auto &p : response.restore_progress) {
            overall_progress += p;
        }
        overall_progress = overall_progress / response.restore_progress.size();
        overall_progress = overall_progress / 10;

        if (detailed) {
            int width = strlen("restore_status");
            std::cout << std::setw(width) << std::left << "pid" << std::setw(width) << std::left
                      << "progress(%)" << std::setw(width) << std::left << "restore_status"
                      << std::endl;
            for (int idx = 0; idx < response.restore_status.size(); idx++) {
                std::string restore_status = std::string("unknown");
                if (response.restore_status[idx] == ::dsn::ERR_OK) {
                    restore_status = (response.restore_progress[idx] == 1000) ? "ok" : "ongoing";
                } else if (response.restore_status[idx] == ERR_IGNORE_BAD_DATA) {
                    restore_status = "skip";
                }
                int progress = response.restore_progress[idx] / 10;
                std::cout << std::setw(width) << std::left << idx << std::setw(width) << std::left
                          << progress << std::setw(width) << std::left << restore_status
                          << std::endl;
            }

            std::cout << std::endl
                      << "the overall progress of restore is " << overall_progress << "%"
                      << std::endl;

            std::cout << std::endl << "annotations:" << std::endl;
            std::cout << "    ok : mean restore complete" << std::endl;
            std::cout << "    ongoing : mean restore is under going" << std::endl;
            std::cout
                << "    skip : data on cold backup media is damaged, but skip the damaged partition"
                << std::endl;
            std::cout << "    unknown : invalid, should login server and check it" << std::endl;
        } else {
            std::cout << "the overall progress of restore is " << overall_progress << "%"
                      << std::endl;
        }
    } else if (response.err == ERR_APP_NOT_EXIST) {
        std::cout << "invalid restore_app_id(" << restore_app_id << ")" << std::endl;
    } else if (response.err == ERR_APP_DROPPED) {
        std::cout << "restore failed, because some partition's data is damaged on cold backup media"
                  << std::endl;
    }
    return ERR_OK;
}

bool replication_ddl_client::valid_app_char(int c)
{
    return (bool)std::isalnum(c) || c == '_' || c == '.' || c == ':';
}

void replication_ddl_client::end_meta_request(
    task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp)
{
    if (err != dsn::ERR_OK && retry_times < 2) {
        rpc::call(_meta_server, request, this, [
            =,
            callback_capture = std::move(callback)
        ](error_code err, dsn_message_t request, dsn_message_t response) {
            end_meta_request(std::move(callback_capture), retry_times + 1, err, request, response);
        });
    } else {
        callback->enqueue_rpc_response(err, resp);
    }
}
}
} // namespace
