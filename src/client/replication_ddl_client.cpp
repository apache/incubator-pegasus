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

#include "replication_ddl_client.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <type_traits>

#include "backup_types.h"
#include "common//duplication_common.h"
#include "common/backup_common.h"
#include "common/bulk_load_common.h"
#include "common/gpid.h"
#include "common/manual_compact.h"
#include "common/partition_split_common.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "meta/meta_rpc_types.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_address.h"
#include "runtime/api_layer1.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/output_utils.h"
#include "utils/time_utils.h"
#include "utils/utils.h"

DSN_DEFINE_uint32(ddl_client,
                  ddl_client_max_attempt_count,
                  3,
                  "The max count that attempt for failed requests to meta server.");
DSN_DEFINE_validator(ddl_client_max_attempt_count,
                     [](uint32_t value) -> bool { return value > 0; });

DSN_DEFINE_uint32(ddl_client,
                  ddl_client_retry_interval_ms,
                  10 * 1000,
                  "The retry interval after receiving error from meta server.");

namespace dsn {
namespace replication {

using tp_output_format = ::dsn::utils::table_printer::output_format;

error_s replication_ddl_client::validate_app_name(const std::string &app_name,
                                                  bool allow_empty_name)
{
    if ((app_name.empty() && !allow_empty_name) ||
        !std::all_of(app_name.cbegin(), app_name.cend(), [](const char c) {
            return static_cast<bool>(std::isalnum(c)) || c == '_' || c == '.' || c == ':';
        })) {
        return FMT_ERR(ERR_INVALID_PARAMETERS, "Invalid name: Only 0-9a-zA-Z.:_ are valid.");
    }

    return error_s::ok();
}

replication_ddl_client::replication_ddl_client(const std::vector<dsn::host_port> &meta_servers)
{
    _meta_server.assign_group("meta-servers");
    for (const auto &m : meta_servers) {
        if (!_meta_server.group_host_port()->add(m)) {
            LOG_WARNING("duplicate address {}", m);
        }
    }
}

replication_ddl_client::~replication_ddl_client() { _tracker.cancel_outstanding_tasks(); }

void replication_ddl_client::set_meta_servers_leader()
{
    auto req = std::make_shared<configuration_cluster_info_request>();

    auto resp_task = request_meta(RPC_CM_CLUSTER_INFO, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        LOG_ERROR("get cluster_info failed!");
        return;
    }

    configuration_cluster_info_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != dsn::ERR_OK) {
        LOG_ERROR("get cluster_info failed!");
        return;
    }

    for (int i = 0; i < resp.keys.size(); i++) {
        if (resp.keys[i] == "primary_meta_server") {
            auto hp = host_port::from_string(resp.values[i]);
            if (_meta_server.group_host_port()->contains(hp)) {
                _meta_server.group_host_port()->set_leader(hp);
            } else {
                LOG_ERROR("meta_servers not contains {}", hp);
            }
            break;
        }
    }
}

dsn::error_code replication_ddl_client::wait_app_ready(const std::string &app_name,
                                                       int partition_count,
                                                       int max_replica_count)
{
    do {
        uint32_t one_step_wait_sec = std::min(_max_wait_secs, 2U);
        std::this_thread::sleep_for(std::chrono::seconds(one_step_wait_sec));
        _max_wait_secs -= one_step_wait_sec;

        auto query_req = std::make_shared<query_cfg_request>();
        query_req->app_name = app_name;

        auto query_task = request_meta(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, query_req);
        query_task->wait();
        if (query_task->error() == ERR_INVALID_STATE) {
            std::cout << app_name << " not ready yet, still waiting..." << std::endl;
            continue;
        }

        if (query_task->error() != dsn::ERR_OK) {
            std::cout << "create app " << app_name
                      << " failed: [query] call server error: " << query_task->error() << std::endl;
            return query_task->error();
        }

        dsn::query_cfg_response query_resp;
        ::dsn::unmarshall(query_task->get_response(), query_resp);
        if (query_resp.err != dsn::ERR_OK) {
            std::cout << "create app " << app_name
                      << " failed: [query] received server error: " << query_resp.err << std::endl;
            return query_resp.err;
        }
        CHECK_EQ(partition_count, query_resp.partition_count);
        int ready_count = 0;
        for (int i = 0; i < partition_count; i++) {
            const auto &pc = query_resp.partitions[i];
            if (pc.hp_primary && (pc.hp_secondaries.size() + 1 >= max_replica_count)) {
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
    } while (_max_wait_secs > 0);

    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::create_app(const std::string &app_name,
                                                   const std::string &app_type,
                                                   int partition_count,
                                                   int replica_count,
                                                   const std::map<std::string, std::string> &envs,
                                                   bool is_stateless,
                                                   bool success_if_exist)
{
    if (partition_count < 1) {
        fmt::println(stderr, "create app {} failed: partition_count should >= 1", app_name);
        return ERR_INVALID_PARAMETERS;
    }

    if (replica_count < 1) {
        fmt::println(stderr, "create app {} failed: replica_count should >= 1", app_name);
        return ERR_INVALID_PARAMETERS;
    }

    RETURN_EC_NOT_OK_MSG(validate_app_name(app_name), "invalid app_name: '{}'", app_name);
    RETURN_EC_NOT_OK_MSG(validate_app_name(app_type), "invalid app_type: '{}'", app_type);

    auto req = std::make_shared<configuration_create_app_request>();
    req->app_name = app_name;
    req->options.partition_count = partition_count;
    req->options.replica_count = replica_count;
    req->options.success_if_exist = success_if_exist;
    req->options.app_type = app_type;
    req->options.envs = envs;
    req->options.is_stateful = !is_stateless;

    dsn::replication::configuration_create_app_response resp;
    RETURN_EC_NOT_OK_MSG(request_meta_and_wait_response(RPC_CM_CREATE_APP, req, resp),
                         "create app {} failed: [create] call server error",
                         app_name);

    if (resp.err != dsn::ERR_OK) {
        fmt::println(
            stderr, "{}: create app {} failed: [create] received server error", resp.err, app_name);
        return resp.err;
    }

    fmt::println("create app {} succeed, waiting for app ready", app_name);

    dsn::error_code error = wait_app_ready(app_name, partition_count, replica_count);
    if (error == dsn::ERR_OK) {
        fmt::println("{} is ready now!", app_name);
    }

    return error;
}

dsn::error_code replication_ddl_client::drop_app(const std::string &app_name, int reserve_seconds)
{
    RETURN_EC_NOT_OK_MSG(validate_app_name(app_name), "invalid app_name: '{}'", app_name);

    auto req = std::make_shared<configuration_drop_app_request>();
    req->app_name = app_name;
    req->options.success_if_not_exist = true;
    req->options.__set_reserve_seconds(reserve_seconds);

    dsn::replication::configuration_drop_app_response resp;
    RETURN_EC_NOT_OK(request_meta_and_wait_response(RPC_CM_DROP_APP, req, resp));

    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::recall_app(int32_t app_id, const std::string &new_app_name)
{
    RETURN_EC_NOT_OK_MSG(
        validate_app_name(new_app_name, true), "invalid new_app_name: '{}'", new_app_name);
    auto req = std::make_shared<configuration_recall_app_request>();
    req->app_id = app_id;
    req->new_app_name = new_app_name;

    dsn::replication::configuration_recall_app_response resp;
    RETURN_EC_NOT_OK(request_meta_and_wait_response(RPC_CM_RECALL_APP, req, resp));

    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    std::cout << "recall app ok, id(" << resp.info.app_id << "), "
              << "name(" << resp.info.app_name << "), "
              << "partition_count(" << resp.info.partition_count << "), wait it ready" << std::endl;
    return wait_app_ready(
        resp.info.app_name, resp.info.partition_count, resp.info.max_replica_count);
}

error_s replication_ddl_client::list_apps(dsn::app_status::type status,
                                          const std::string &app_name_pattern,
                                          utils::pattern_match_type::type match_type,
                                          std::vector<::dsn::app_info> &apps)
{
    auto req = std::make_shared<configuration_list_apps_request>();
    req->status = status;
    req->__set_app_name_pattern(app_name_pattern);
    req->__set_match_type(match_type);

    dsn::replication::configuration_list_apps_response resp;
    const auto &req_result = request_meta_and_wait_response(RPC_CM_LIST_APPS, req, resp);
    if (!req_result) {
        return req_result;
    }

    if (resp.err != dsn::ERR_OK) {
        return error_s::make(resp.err, resp.hint_message);
    }

    apps = std::move(resp.infos);

    return error_s::ok();
}

error_s replication_ddl_client::list_apps(dsn::app_status::type status,
                                          std::vector<::dsn::app_info> &apps)
{
    return list_apps(status, {}, utils::pattern_match_type::PMT_MATCH_ALL, apps);
}

error_s replication_ddl_client::list_apps(bool detailed,
                                          bool json,
                                          const std::string &output_file,
                                          dsn::app_status::type status,
                                          const std::string &app_name_pattern,
                                          utils::pattern_match_type::type match_type)
{
    std::vector<::dsn::app_info> apps;
    {
        const auto &result = list_apps(status, app_name_pattern, match_type, apps);
        if (!result) {
            return result;
        }
    }

    size_t max_app_name_size = 20;
    for (const auto &app : apps) {
        max_app_name_size = std::max(max_app_name_size, app.app_name.size() + 2);
    }

    dsn::utils::multi_table_printer multi_printer;
    dsn::utils::table_printer tp_general("general_info");
    tp_general.add_title("app_id");
    tp_general.add_column("status");
    tp_general.add_column("app_name");
    tp_general.add_column("app_type");
    tp_general.add_column("partition_count");
    tp_general.add_column("replica_count");
    tp_general.add_column("is_stateful");
    tp_general.add_column("create_time");
    tp_general.add_column("drop_time");
    tp_general.add_column("drop_expire");
    tp_general.add_column("envs_count");

    int available_app_count = 0;
    for (const auto &info : apps) {
        std::string status_str = enum_to_string(info.status);
        status_str = status_str.substr(status_str.find("AS_") + 3);
        std::string create_time = "-";
        if (info.create_second > 0) {
            char ts_buf[20] = {0};
            dsn::utils::time_ms_to_date_time((uint64_t)info.create_second * 1000, ts_buf, 20);
            ts_buf[10] = '_';
            create_time = ts_buf;
        }
        std::string drop_time = "-";
        std::string drop_expire_time = "-";
        if (info.status == app_status::AS_AVAILABLE) {
            available_app_count++;
        } else if (info.status == app_status::AS_DROPPED && info.expire_second > 0) {
            if (info.drop_second > 0) {
                char ts_buf[20] = {0};
                dsn::utils::time_ms_to_date_time((uint64_t)info.drop_second * 1000, ts_buf, 20);
                ts_buf[10] = '_';
                drop_time = ts_buf;
            }
            if (info.expire_second > 0) {
                char ts_buf[20] = {0};
                dsn::utils::time_ms_to_date_time((uint64_t)info.expire_second * 1000, ts_buf, 20);
                ts_buf[10] = '_';
                drop_expire_time = ts_buf;
            }
        }
        tp_general.add_row(info.app_id);
        tp_general.append_data(status_str);
        tp_general.append_data(info.app_name);
        tp_general.append_data(info.app_type);
        tp_general.append_data(info.partition_count);
        tp_general.append_data(info.max_replica_count);
        tp_general.append_data(info.is_stateful);
        tp_general.append_data(create_time);
        tp_general.append_data(drop_time);
        tp_general.append_data(drop_expire_time);
        tp_general.append_data(info.envs.size());
    }
    multi_printer.add(std::move(tp_general));

    int total_fully_healthy_app_count = 0;
    int total_unhealthy_app_count = 0;
    int total_write_unhealthy_app_count = 0;
    int total_read_unhealthy_app_count = 0;
    if (detailed && available_app_count > 0) {
        dsn::utils::table_printer tp_health("healthy_info");
        tp_health.add_title("app_id");
        tp_health.add_column("app_name");
        tp_health.add_column("partition_count");
        tp_health.add_column("fully_healthy");
        tp_health.add_column("unhealthy");
        tp_health.add_column("write_unhealthy");
        tp_health.add_column("read_unhealthy");
        for (const auto &info : apps) {
            if (info.status != app_status::AS_AVAILABLE) {
                continue;
            }
            int32_t app_id = 0;
            int32_t partition_count = 0;
            std::vector<partition_configuration> pcs;
            const auto &err = list_app(info.app_name, app_id, partition_count, pcs);
            if (err != ERR_OK) {
                LOG_ERROR("list app({}) failed, err={}", info.app_name, err);
                return error_s::make(err);
            }
            CHECK_EQ(info.app_id, app_id);
            CHECK_EQ(info.partition_count, partition_count);
            int fully_healthy = 0;
            int write_unhealthy = 0;
            int read_unhealthy = 0;
            for (const auto &pc : pcs) {
                int replica_count = 0;
                if (pc.hp_primary) {
                    replica_count++;
                }
                replica_count += pc.hp_secondaries.size();
                if (pc.hp_primary) {
                    if (replica_count >= pc.max_replica_count) {
                        fully_healthy++;
                    } else if (replica_count < 2) {
                        write_unhealthy++;
                    }
                } else {
                    write_unhealthy++;
                    read_unhealthy++;
                }
            }
            tp_health.add_row(info.app_id);
            tp_health.append_data(info.app_name);
            tp_health.append_data(info.partition_count);
            tp_health.append_data(fully_healthy);
            tp_health.append_data(info.partition_count - fully_healthy);
            tp_health.append_data(write_unhealthy);
            tp_health.append_data(read_unhealthy);

            if (fully_healthy == info.partition_count) {
                ++total_fully_healthy_app_count;
            } else {
                ++total_unhealthy_app_count;
            }
            if (write_unhealthy > 0) {
                ++total_write_unhealthy_app_count;
            }
            if (read_unhealthy > 0) {
                ++total_read_unhealthy_app_count;
            }
        }
        multi_printer.add(std::move(tp_health));
    }

    dsn::utils::table_printer tp_count("summary");
    tp_count.add_row_name_and_data("total_app_count", available_app_count);
    if (detailed && available_app_count > 0) {
        tp_count.add_row_name_and_data("fully_healthy_app_count", total_fully_healthy_app_count);
        tp_count.add_row_name_and_data("unhealthy_app_count", total_unhealthy_app_count);
        tp_count.add_row_name_and_data("write_unhealthy_app_count",
                                       total_write_unhealthy_app_count);
        tp_count.add_row_name_and_data("read_unhealthy_app_count", total_read_unhealthy_app_count);
    }
    multi_printer.add(std::move(tp_count));

    dsn::utils::output(output_file, json, multi_printer);

    return error_s::ok();
}

error_s replication_ddl_client::list_apps(bool detailed,
                                          bool json,
                                          const std::string &output_file,
                                          const dsn::app_status::type status)
{
    return list_apps(
        detailed, json, output_file, status, {}, utils::pattern_match_type::PMT_MATCH_ALL);
}

dsn::error_code replication_ddl_client::list_nodes(
    dsn::replication::node_status::type status,
    std::map<dsn::host_port, dsn::replication::node_status::type> &nodes)
{
    auto req = std::make_shared<configuration_list_nodes_request>();
    req->status = status;
    auto resp_task = request_meta(RPC_CM_LIST_NODES, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::replication::configuration_list_nodes_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    for (const auto &n : resp.infos) {
        host_port hp;
        GET_HOST_PORT(n, node, hp);
        nodes[hp] = n.status;
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

std::string replication_ddl_client::node_name(const host_port &hp, bool resolve_ip)
{
    if (!resolve_ip) {
        return hp.to_string();
    }

    return dns_resolver::instance().resolve_address(hp).to_string();
}

dsn::error_code replication_ddl_client::cluster_name(int64_t timeout_ms, std::string &cluster_name)
{
    auto req = std::make_shared<configuration_cluster_info_request>();

    auto resp_task = request_meta(RPC_CM_CLUSTER_INFO, req, timeout_ms);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    configuration_cluster_info_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    cluster_name.clear();
    for (int i = 0; i < resp.keys.size(); ++i) {
        if (resp.keys[i] == "cluster_name") {
            cluster_name = resp.values[i];
        }
    }

    return cluster_name.empty() ? dsn::ERR_UNKNOWN : dsn::ERR_OK;
}

dsn::error_code
replication_ddl_client::cluster_info(const std::string &file_name, bool resolve_ip, bool json)
{
    auto req = std::make_shared<configuration_cluster_info_request>();

    auto resp_task = request_meta(RPC_CM_CLUSTER_INFO, req);
    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    configuration_cluster_info_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
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

    if (resolve_ip) {
        for (int i = 0; i < resp.keys.size(); ++i) {
            if (resp.keys[i] == "meta_servers" || resp.keys[i] == "primary_meta_server") {
                resp.values[i] = dns_resolver::ip_ports_from_host_ports(resp.values[i]);
            }
        }
    }

    dsn::utils::table_printer tp("cluster_info");
    for (int i = 0; i < resp.keys.size(); i++) {
        tp.add_row_name_and_data(resp.keys[i], resp.values[i]);
    }
    tp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    return dsn::ERR_OK;
}

dsn::error_code replication_ddl_client::list_app(const std::string &app_name,
                                                 bool detailed,
                                                 bool json,
                                                 const std::string &file_name,
                                                 bool resolve_ip)
{
    dsn::utils::multi_table_printer mtp;
    dsn::utils::table_printer tp_params("parameters");
    if (!(app_name.empty() && file_name.empty())) {
        if (!app_name.empty())
            tp_params.add_row_name_and_data("app_name", app_name);
        if (!file_name.empty())
            tp_params.add_row_name_and_data("out_file", file_name);
    }
    tp_params.add_row_name_and_data("detailed", detailed);
    mtp.add(std::move(tp_params));
    int32_t app_id = 0;
    int32_t partition_count = 0;
    int32_t max_replica_count = 0;
    std::vector<partition_configuration> pcs;
    dsn::error_code err = list_app(app_name, app_id, partition_count, pcs);
    if (err != dsn::ERR_OK) {
        return err;
    }
    if (!pcs.empty()) {
        max_replica_count = pcs[0].max_replica_count;
    }

    // print query_cfg_response
    std::streambuf *buf;
    std::ofstream of;

    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    dsn::utils::table_printer tp_general("general");
    tp_general.add_row_name_and_data("app_name", app_name);
    tp_general.add_row_name_and_data("app_id", app_id);
    tp_general.add_row_name_and_data("partition_count", partition_count);
    tp_general.add_row_name_and_data("max_replica_count", max_replica_count);
    mtp.add(std::move(tp_general));

    if (detailed) {
        dsn::utils::table_printer tp_details("replicas");
        tp_details.add_title("pidx");
        tp_details.add_column("ballot");
        tp_details.add_column("replica_count");
        tp_details.add_column("primary");
        tp_details.add_column("secondaries");
        std::map<host_port, std::pair<int, int>> node_stat;

        int total_prim_count = 0;
        int total_sec_count = 0;
        int fully_healthy = 0;
        int write_unhealthy = 0;
        int read_unhealthy = 0;
        for (const auto &pc : pcs) {
            int replica_count = 0;
            if (pc.hp_primary) {
                replica_count++;
                node_stat[pc.hp_primary].first++;
                total_prim_count++;
            }
            replica_count += pc.hp_secondaries.size();
            total_sec_count += pc.hp_secondaries.size();
            if (pc.hp_primary) {
                if (replica_count >= pc.max_replica_count) {
                    fully_healthy++;
                } else if (replica_count < 2) {
                    write_unhealthy++;
                }
            } else {
                write_unhealthy++;
                read_unhealthy++;
            }
            for (const auto &secondary : pc.hp_secondaries) {
                node_stat[secondary].second++;
            }
            tp_details.add_row(pc.pid.get_partition_index());
            tp_details.append_data(pc.ballot);
            tp_details.append_data(fmt::format("{}/{}", replica_count, pc.max_replica_count));
            tp_details.append_data(pc.hp_primary ? pc.hp_primary.to_string() : "-");
            tp_details.append_data(fmt::format("[{}]", fmt::join(pc.hp_secondaries, ",")));
        }
        mtp.add(std::move(tp_details));

        // 'node' section.
        dsn::utils::table_printer tp_nodes("nodes");
        tp_nodes.add_title("node");
        tp_nodes.add_column("primary");
        tp_nodes.add_column("secondary");
        tp_nodes.add_column("total");
        for (const auto &[hp, pri_and_sec_rep_cnts] : node_stat) {
            tp_nodes.add_row(node_name(hp, resolve_ip));
            tp_nodes.append_data(pri_and_sec_rep_cnts.first);
            tp_nodes.append_data(pri_and_sec_rep_cnts.second);
            tp_nodes.append_data(pri_and_sec_rep_cnts.first + pri_and_sec_rep_cnts.second);
        }
        tp_nodes.add_row("");
        tp_nodes.append_data(total_prim_count);
        tp_nodes.append_data(total_sec_count);
        tp_nodes.append_data(total_prim_count + total_sec_count);
        mtp.add(std::move(tp_nodes));

        // healthy partition count section.
        dsn::utils::table_printer tp_hpc("healthy");
        tp_hpc.add_row_name_and_data("fully_healthy_partition_count", fully_healthy);
        tp_hpc.add_row_name_and_data("unhealthy_partition_count", partition_count - fully_healthy);
        tp_hpc.add_row_name_and_data("write_unhealthy_partition_count", write_unhealthy);
        tp_hpc.add_row_name_and_data("read_unhealthy_partition_count", read_unhealthy);
        mtp.add(std::move(tp_hpc));
    }
    mtp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    return dsn::ERR_OK;
#undef RESOLVE
}

dsn::error_code replication_ddl_client::list_app(const std::string &app_name,
                                                 int32_t &app_id,
                                                 int32_t &partition_count,
                                                 std::vector<partition_configuration> &pcs)
{
    RETURN_EC_NOT_OK_MSG(validate_app_name(app_name), "invalid app_name: '{}'", app_name);

    auto req = std::make_shared<query_cfg_request>();
    req->app_name = app_name;

    auto resp_task = request_meta(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, req);

    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    dsn::query_cfg_response resp;
    dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    app_id = resp.app_id;
    partition_count = resp.partition_count;
    pcs = resp.partitions;

    return dsn::ERR_OK;
}

dsn::replication::configuration_meta_control_response
replication_ddl_client::control_meta_function_level(meta_function_level::type level)
{
    auto req = std::make_shared<configuration_meta_control_request>();
    req->level = level;

    auto response_task = request_meta(RPC_CM_CONTROL_META, req);
    response_task->wait();
    configuration_meta_control_response resp;
    if (response_task->error() != dsn::ERR_OK) {
        resp.err = response_task->error();
    } else {
        dsn::unmarshall(response_task->get_response(), resp);
    }
    return resp;
}

dsn::error_code
replication_ddl_client::send_balancer_proposal(const configuration_balancer_request &request)
{
    auto req = std::make_shared<configuration_balancer_request>(request);

    auto response_task = request_meta(RPC_CM_PROPOSE_BALANCER, req);
    response_task->wait();
    if (response_task->error() != dsn::ERR_OK)
        return response_task->error();
    dsn::replication::configuration_balancer_response resp;
    dsn::unmarshall(response_task->get_response(), resp);
    return resp.err;
}

dsn::error_code replication_ddl_client::do_recovery(const std::vector<host_port> &replica_nodes,
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

    auto req = std::make_shared<configuration_recovery_request>();
    CLEAR_IP_AND_HOST_PORT(*req, recovery_nodes);
    for (const auto &node : replica_nodes) {
        if (utils::contains(req->hp_recovery_nodes, node)) {
            out << "duplicate replica node " << node << ", just ingore it" << std::endl;
        } else {
            ADD_IP_AND_HOST_PORT_BY_DNS(*req, recovery_nodes, node);
        }
    }
    if (req->hp_recovery_nodes.empty()) {
        CHECK(req->recovery_nodes.empty(),
              "recovery_nodes should be set together with hp_recovery_nodes");
        out << "node set for recovery it empty" << std::endl;
        return ERR_INVALID_PARAMETERS;
    }
    CHECK(!req->recovery_nodes.empty(),
          "recovery_nodes should be set together with hp_recovery_nodes");
    req->skip_bad_nodes = skip_bad_nodes;
    req->skip_lost_partitions = skip_lost_partitions;

    out << "Wait seconds: " << wait_seconds << std::endl;
    out << "Skip bad nodes: " << (skip_bad_nodes ? "true" : "false") << std::endl;
    out << "Skip lost partitions: " << (skip_lost_partitions ? "true" : "false") << std::endl;
    out << "Node list:" << std::endl;
    out << "=============================" << std::endl;
    for (auto &node : req->hp_recovery_nodes) {
        out << node << std::endl;
    }
    out << "=============================" << std::endl;

    auto response_task = request_meta(RPC_CM_START_RECOVERY, req, wait_seconds * 1000);
    bool wait_done = false;
    for (int i = 0; i < wait_seconds; ++i) {
        wait_done = response_task->wait(1000);
        if (wait_done)
            break;
        else
            out << "Wait recovery for " << i << " seconds" << std::endl;
    }

    if (!wait_done || response_task->get_response() == NULL) {
        out << "Wait recovery failed, administrator should check the meta for progress"
            << std::endl;
        return dsn::ERR_TIMEOUT;
    } else {
        configuration_recovery_response resp;
        dsn::unmarshall(response_task->get_response(), resp);
        out << "Recover result: " << resp.err << std::endl;
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
                                                   bool skip_bad_partition,
                                                   const std::string &restore_path)
{
    RETURN_EC_NOT_OK_MSG(
        validate_app_name(old_app_name), "invalid old_app_name: '{}'", old_app_name);
    RETURN_EC_NOT_OK_MSG(
        validate_app_name(new_app_name), "invalid new_app_name: '{}'", new_app_name);

    auto req = std::make_shared<configuration_restore_request>();

    req->cluster_name = cluster_name;
    req->policy_name = policy_name;
    req->app_name = old_app_name;
    req->app_id = old_app_id;
    req->new_app_name = new_app_name;
    req->backup_provider_name = backup_provider_name;
    req->time_stamp = timestamp;
    req->skip_bad_partition = skip_bad_partition;
    if (!restore_path.empty()) {
        req->__set_restore_path(restore_path);
        std::cout << "restore app from the specified path : " << restore_path << std::endl;
    }

    auto resp_task = request_meta(RPC_CM_START_RESTORE, req);
    bool finish = false;
    while (!finish) {
        std::cout << "sleep 1 second to wait complete..." << std::endl;
        finish = resp_task->wait(1000);
    }

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    } else {
        configuration_create_app_response resp;
        dsn::unmarshall(resp_task->get_response(), resp);
        if (resp.err == ERR_OBJECT_NOT_FOUND) {
            std::cout << "restore app failed: couldn't find valid app metadata" << std::endl;
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
    auto req = std::make_shared<configuration_add_backup_policy_request>();
    req->policy_name = policy_name;
    req->backup_provider_type = backup_provider_type;
    req->app_ids = app_ids;
    req->backup_interval_seconds = backup_interval_seconds;
    req->backup_history_count_to_keep = backup_history_cnt;
    req->start_time = start_time;
    auto resp_task = request_meta(RPC_CM_ADD_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_add_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);

    if (resp.err != ERR_OK) {
        std::cout << "add backup policy failed: " << resp.hint_message << std::endl;
        return resp.err;
    } else {
        std::cout << "add backup policy succeed, policy_name = " << policy_name << std::endl;
    }
    return ERR_OK;
}

error_with<start_backup_app_response> replication_ddl_client::backup_app(
    int32_t app_id, const std::string &backup_provider_type, const std::string &backup_path)
{
    set_meta_servers_leader();
    auto req = std::make_unique<start_backup_app_request>();
    req->app_id = app_id;
    req->backup_provider_type = backup_provider_type;
    if (!backup_path.empty()) {
        req->__set_backup_path(backup_path);
    }
    return call_rpc_sync(start_backup_app_rpc(std::move(req), RPC_CM_START_BACKUP_APP));
}

error_with<query_backup_status_response> replication_ddl_client::query_backup(int32_t app_id,
                                                                              int64_t backup_id)
{
    auto req = std::make_unique<query_backup_status_request>();
    req->app_id = app_id;

    if (backup_id > 0) {
        req->__set_backup_id(backup_id);
    }
    return call_rpc_sync(query_backup_status_rpc(std::move(req), RPC_CM_QUERY_BACKUP_STATUS));
}

dsn::error_code replication_ddl_client::disable_backup_policy(const std::string &policy_name,
                                                              bool force)
{
    auto req = std::make_shared<configuration_modify_backup_policy_request>();
    req->policy_name = policy_name;
    req->__set_is_disable(true);
    req->__set_force_disable(force);

    auto resp_task = request_meta(RPC_CM_MODIFY_BACKUP_POLICY, req);

    resp_task->wait();
    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != ERR_OK) {
        std::cout << "disable backup policy failed: " << resp.hint_message << std::endl;
        return resp.err;
    } else {
        std::cout << "disable policy result: " << resp.err << std::endl;
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
    auto req = std::make_shared<configuration_modify_backup_policy_request>();
    req->policy_name = policy_name;
    req->__set_is_disable(false);

    auto resp_task = request_meta(RPC_CM_MODIFY_BACKUP_POLICY, req);

    resp_task->wait();
    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != ERR_OK) {
        std::cout << "enable backup policy failed: " << resp.hint_message << std::endl;
        return resp.err;
    } else if (resp.err == ERR_BUSY) {
        std::cout << "policy is under backup, please try disable later" << std::endl;
        return ERR_OK;
    } else {
        std::cout << "enable policy result: " << resp.err << std::endl;
        if (!resp.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << resp.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
        return resp.err;
    }
}

static dsn::utils::table_printer print_policy_entry(const policy_entry &entry)
{
    dsn::utils::table_printer tp(entry.policy_name);
    tp.add_row_name_and_data("backup_provider_type", entry.backup_provider_type);
    tp.add_row_name_and_data("backup_interval", entry.backup_interval_seconds + "s");
    tp.add_row_name_and_data("app_ids", fmt::format("{{{}}}", fmt::join(entry.app_ids, ", ")));
    tp.add_row_name_and_data("start_time", entry.start_time);
    tp.add_row_name_and_data("status", entry.is_disable ? "disabled" : "enabled");
    tp.add_row_name_and_data("backup_history_count", entry.backup_history_count_to_keep);
    return tp;
}

static void print_backup_entry(dsn::utils::table_printer &tp, const backup_entry &bentry)
{
    char start_time[30] = {'\0'};
    char end_time[30] = {'\0'};
    ::dsn::utils::time_ms_to_date_time(bentry.start_time_ms, start_time, 30);
    if (bentry.end_time_ms == 0) {
        end_time[0] = '-';
        end_time[1] = '\0';
    } else {
        ::dsn::utils::time_ms_to_date_time(bentry.end_time_ms, end_time, 30);
    }

    tp.add_row(bentry.backup_id);
    tp.append_data(start_time);
    tp.append_data(end_time);
    tp.append_data(fmt::format("{{{}}}", fmt::join(bentry.app_ids, ", ")));
}

dsn::error_code replication_ddl_client::ls_backup_policy(bool json)
{
    auto req = std::make_shared<configuration_query_backup_policy_request>();
    req->policy_names.clear();
    req->backup_info_count = 0;

    auto resp_task = request_meta(RPC_CM_QUERY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }
    configuration_query_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);

    std::streambuf *buf;
    std::ofstream of;
    buf = std::cout.rdbuf();
    std::ostream out(buf);

    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        dsn::utils::multi_table_printer mtp;
        for (int32_t idx = 0; idx < resp.policys.size(); idx++) {
            dsn::utils::table_printer tp = print_policy_entry(resp.policys[idx]);
            mtp.add(std::move(tp));
        }
        mtp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    }
    return ERR_OK;
}

dsn::error_code replication_ddl_client::query_backup_policy(
    const std::vector<std::string> &policy_names, int backup_info_cnt, bool json)
{
    auto req = std::make_shared<configuration_query_backup_policy_request>();
    req->policy_names = policy_names;
    req->backup_info_count = backup_info_cnt;

    auto resp_task = request_meta(RPC_CM_QUERY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_query_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);

    std::streambuf *buf;
    std::ofstream of;
    buf = std::cout.rdbuf();
    std::ostream out(buf);

    if (resp.err != ERR_OK) {
        return resp.err;
    } else {
        dsn::utils::multi_table_printer mtp;
        for (int32_t idx = 0; idx < resp.policys.size(); idx++) {
            const policy_entry &pentry = resp.policys[idx];
            dsn::utils::table_printer tp_policy = print_policy_entry(pentry);
            mtp.add(std::move(tp_policy));
            const std::vector<backup_entry> &backup_infos = resp.backup_infos[idx];
            dsn::utils::table_printer tp_backup(pentry.policy_name + "_" +
                                                cold_backup_constant::BACKUP_INFO);
            tp_backup.add_title("id");
            tp_backup.add_column("start_time");
            tp_backup.add_column("end_time");
            tp_backup.add_column("app_ids");
            for (int bi_idx = 0; bi_idx < backup_infos.size(); bi_idx++) {
                print_backup_entry(tp_backup, backup_infos[bi_idx]);
            }
            mtp.add(std::move(tp_backup));
        }
        mtp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
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
    auto req = std::make_shared<configuration_modify_backup_policy_request>();
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
    auto resp_task = request_meta(RPC_CM_MODIFY_BACKUP_POLICY, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_modify_backup_policy_response resp;
    ::dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != ERR_OK) {
        std::cout << "modify backup policy failed: " << resp.hint_message << std::endl;
        return resp.err;
    } else {
        std::cout << "Modify policy result: " << resp.err << std::endl;
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
    auto req = std::make_shared<configuration_query_restore_request>();
    req->restore_app_id = restore_app_id;

    auto resp_task = request_meta(RPC_CM_QUERY_RESTORE_STATUS, req);

    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }

    configuration_query_restore_response response;
    ::dsn::unmarshall(resp_task->get_response(), response);
    if (response.err == ERR_OK) {
        int overall_progress = 0;
        for (const auto &progress : response.restore_progress) {
            overall_progress += progress;
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
    } else {
        return response.err;
    }
    return ERR_OK;
}

error_with<duplication_add_response>
replication_ddl_client::add_dup(const std::string &app_name,
                                const std::string &remote_cluster_name,
                                bool is_duplicating_checkpoint,
                                const std::string &remote_app_name,
                                const uint32_t remote_replica_count)
{
    RETURN_EW_NOT_OK_MSG(validate_app_name(remote_app_name, false),
                         duplication_add_response,
                         "invalid remote_app_name: '{}'",
                         remote_app_name);

    auto req = std::make_unique<duplication_add_request>();
    req->app_name = app_name;
    req->remote_cluster_name = remote_cluster_name;
    req->is_duplicating_checkpoint = is_duplicating_checkpoint;
    req->__set_remote_app_name(remote_app_name);
    req->__set_remote_replica_count(static_cast<int32_t>(remote_replica_count));
    return call_rpc_sync(duplication_add_rpc(std::move(req), RPC_CM_ADD_DUPLICATION));
}

error_with<duplication_modify_response> replication_ddl_client::change_dup_status(
    const std::string &app_name, int dupid, duplication_status::type status)
{
    auto req = std::make_unique<duplication_modify_request>();
    req->app_name = app_name;
    req->dupid = dupid;
    req->__set_status(status);
    return call_rpc_sync(duplication_modify_rpc(std::move(req), RPC_CM_MODIFY_DUPLICATION));
}

error_with<duplication_modify_response> replication_ddl_client::update_dup_fail_mode(
    const std::string &app_name, int dupid, duplication_fail_mode::type fmode)
{
    if (_duplication_fail_mode_VALUES_TO_NAMES.find(fmode) ==
        _duplication_fail_mode_VALUES_TO_NAMES.end()) {
        return FMT_ERR(ERR_INVALID_PARAMETERS, "unexpected duplication_fail_mode {}", fmode);
    }
    auto req = std::make_unique<duplication_modify_request>();
    req->app_name = app_name;
    req->dupid = dupid;
    req->__set_fail_mode(fmode);
    return call_rpc_sync(duplication_modify_rpc(std::move(req), RPC_CM_MODIFY_DUPLICATION));
}

error_with<duplication_query_response>
replication_ddl_client::query_dup(const std::string &app_name)
{
    auto req = std::make_unique<duplication_query_request>();
    req->app_name = app_name;
    return call_rpc_sync(duplication_query_rpc(std::move(req), RPC_CM_QUERY_DUPLICATION));
}

error_with<duplication_list_response>
replication_ddl_client::list_dups(const std::string &app_name_pattern,
                                  utils::pattern_match_type::type match_type)
{
    auto req = std::make_unique<duplication_list_request>();
    req->app_name_pattern = app_name_pattern;
    req->match_type = match_type;
    return call_rpc_sync(duplication_list_rpc(std::move(req), RPC_CM_LIST_DUPLICATION));
}

namespace {

bool need_retry(uint32_t attempt_count, const dsn::error_code &err)
{
    // For successful request, no need to retry.
    if (err == dsn::ERR_OK) {
        return false;
    }

    // As long as the max attempt count has not been reached, just do retry;
    // otherwise, do not attempt again.
    return attempt_count < FLAGS_ddl_client_max_attempt_count;
}

} // anonymous namespace

void replication_ddl_client::end_meta_request(const rpc_response_task_ptr &callback,
                                              uint32_t attempt_count,
                                              const error_code &err,
                                              dsn::message_ex *request,
                                              dsn::message_ex *resp)
{
    LOG_INFO(
        "send request to meta server: rpc_code={}, err={}, attempt_count={}, max_attempt_count={}",
        request->local_rpc_code,
        err,
        attempt_count,
        FLAGS_ddl_client_max_attempt_count);

    if (!need_retry(attempt_count, err)) {
        callback->enqueue(err, (message_ex *)resp);
        return;
    }

    rpc::call(dsn::dns_resolver::instance().resolve_address(_meta_server),
              request,
              &_tracker,
              [this, attempt_count, callback](
                  error_code err, dsn::message_ex *request, dsn::message_ex *response) mutable {
                  FAIL_POINT_INJECT_NOT_RETURN_F(
                      "ddl_client_request_meta",
                      [&err, this](std::string_view str) { err = pop_mock_error(); });

                  end_meta_request(callback, attempt_count + 1, err, request, response);
              });
}

dsn::error_code replication_ddl_client::get_app_envs(const std::string &app_name,
                                                     std::map<std::string, std::string> &envs)
{
    // Just match the table with the provided name exactly since we want to get the envs from
    // a specific table.
    std::vector<::dsn::app_info> apps;
    RETURN_EC_NOT_OK(list_apps(
        dsn::app_status::AS_AVAILABLE, app_name, utils::pattern_match_type::PMT_MATCH_EXACT, apps));

    for (const auto &app : apps) {
        // Once the meta server does not support `app_name` and `match_type` (still the old
        // version) for `RPC_CM_LIST_APPS`, the response would include all available tables.
        // Thus here we should still check if the table name in the response is the target.
        if (app.app_name != app_name) {
            continue;
        }

        envs = app.envs;
        return dsn::ERR_OK;
    }

    return dsn::ERR_OBJECT_NOT_FOUND;
}

error_with<configuration_update_app_env_response>
replication_ddl_client::set_app_envs(const std::string &app_name,
                                     const std::vector<std::string> &keys,
                                     const std::vector<std::string> &values)
{
    auto req = std::make_unique<configuration_update_app_env_request>();
    req->__set_app_name(app_name);
    req->__set_keys(keys);
    req->__set_values(values);
    req->__set_op(app_env_operation::type::APP_ENV_OP_SET);
    return call_rpc_sync(update_app_env_rpc(std::move(req), RPC_CM_UPDATE_APP_ENV));
}

::dsn::error_code replication_ddl_client::del_app_envs(const std::string &app_name,
                                                       const std::vector<std::string> &keys)
{
    auto req = std::make_shared<configuration_update_app_env_request>();
    req->__set_app_name(app_name);
    req->__set_op(app_env_operation::type::APP_ENV_OP_DEL);
    req->__set_keys(keys);

    auto resp_task = request_meta(RPC_CM_UPDATE_APP_ENV, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }
    configuration_update_app_env_response response;
    ::dsn::unmarshall(resp_task->get_response(), response);
    if (response.err != ERR_OK) {
        return response.err;
    } else {
        std::cout << "del app envs succeed" << std::endl;
        if (!response.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << response.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
    }
    return ERR_OK;
}

::dsn::error_code replication_ddl_client::clear_app_envs(const std::string &app_name,
                                                         bool clear_all,
                                                         const std::string &prefix)
{
    auto req = std::make_shared<configuration_update_app_env_request>();
    req->__set_app_name(app_name);
    req->__set_op(app_env_operation::type::APP_ENV_OP_CLEAR);
    if (clear_all) {
        req->__set_clear_prefix("");
    } else {
        CHECK(!prefix.empty(), "prefix can not be empty");
        req->__set_clear_prefix(prefix);
    }

    auto resp_task = request_meta(RPC_CM_UPDATE_APP_ENV, req);
    resp_task->wait();

    if (resp_task->error() != ERR_OK) {
        return resp_task->error();
    }
    configuration_update_app_env_response response;
    ::dsn::unmarshall(resp_task->get_response(), response);
    if (response.err != ERR_OK) {
        return response.err;
    } else {
        std::cout << "clear app envs succeed" << std::endl;
        if (!response.hint_message.empty()) {
            std::cout << "=============================" << std::endl;
            std::cout << response.hint_message << std::endl;
            std::cout << "=============================" << std::endl;
        }
    }
    return ERR_OK;
}

dsn::error_code
replication_ddl_client::ddd_diagnose(gpid pid, std::vector<ddd_partition_info> &ddd_partitions)
{
    auto req = std::make_shared<ddd_diagnose_request>();
    req->pid = pid;

    auto resp_task = request_meta(RPC_CM_DDD_DIAGNOSE, req);

    resp_task->wait();
    if (resp_task->error() != dsn::ERR_OK) {
        return resp_task->error();
    }

    ddd_diagnose_response resp;
    dsn::unmarshall(resp_task->get_response(), resp);
    if (resp.err != dsn::ERR_OK) {
        return resp.err;
    }

    ddd_partitions = std::move(resp.partitions);

    return dsn::ERR_OK;
}

void replication_ddl_client::query_disk_info(
    const std::vector<dsn::host_port> &targets,
    const std::string &app_name,
    /*out*/ std::map<dsn::host_port, error_with<query_disk_info_response>> &resps)
{
    std::map<dsn::host_port, query_disk_info_rpc> query_disk_info_rpcs;
    for (const auto &target : targets) {
        auto request = std::make_unique<query_disk_info_request>();
        SET_IP_AND_HOST_PORT_BY_DNS(*request, node1, target);
        request->app_name = app_name;
        query_disk_info_rpcs.emplace(target,
                                     query_disk_info_rpc(std::move(request), RPC_QUERY_DISK_INFO));
    }
    call_rpcs_sync(query_disk_info_rpcs, resps);
}

error_with<start_bulk_load_response>
replication_ddl_client::start_bulk_load(const std::string &app_name,
                                        const std::string &cluster_name,
                                        const std::string &file_provider_type,
                                        const std::string &remote_root_path,
                                        const bool ingest_behind)
{
    auto req = std::make_unique<start_bulk_load_request>();
    req->app_name = app_name;
    req->cluster_name = cluster_name;
    req->file_provider_type = file_provider_type;
    req->remote_root_path = remote_root_path;
    req->ingest_behind = ingest_behind;
    return call_rpc_sync(start_bulk_load_rpc(std::move(req), RPC_CM_START_BULK_LOAD));
}

error_with<control_bulk_load_response>
replication_ddl_client::control_bulk_load(const std::string &app_name,
                                          const bulk_load_control_type::type control_type)
{
    auto req = std::make_unique<control_bulk_load_request>();
    req->app_name = app_name;
    req->type = control_type;
    return call_rpc_sync(control_bulk_load_rpc(std::move(req), RPC_CM_CONTROL_BULK_LOAD));
}

error_with<query_bulk_load_response>
replication_ddl_client::query_bulk_load(const std::string &app_name)
{

    auto req = std::make_unique<query_bulk_load_request>();
    req->app_name = app_name;
    return call_rpc_sync(query_bulk_load_rpc(std::move(req), RPC_CM_QUERY_BULK_LOAD_STATUS));
}

error_with<clear_bulk_load_state_response>
replication_ddl_client::clear_bulk_load(const std::string &app_name)
{
    auto req = std::make_unique<clear_bulk_load_state_request>();
    req->app_name = app_name;
    return call_rpc_sync(clear_bulk_load_rpc(std::move(req), RPC_CM_CLEAR_BULK_LOAD));
}

error_code replication_ddl_client::detect_hotkey(const dsn::host_port &target,
                                                 detect_hotkey_request &req,
                                                 detect_hotkey_response &resp)
{
    std::map<dsn::host_port, detect_hotkey_rpc> detect_hotkey_rpcs;
    auto request = std::make_unique<detect_hotkey_request>(req);
    detect_hotkey_rpcs.emplace(target, detect_hotkey_rpc(std::move(request), RPC_DETECT_HOTKEY));
    std::map<dsn::host_port, error_with<detect_hotkey_response>> resps;
    call_rpcs_sync(detect_hotkey_rpcs, resps);
    resp = resps.begin()->second.get_value();
    return resps.begin()->second.get_error().code();
}

error_with<start_partition_split_response>
replication_ddl_client::start_partition_split(const std::string &app_name, int new_partition_count)
{
    auto req = std::make_unique<start_partition_split_request>();
    req->__set_app_name(app_name);
    req->__set_new_partition_count(new_partition_count);
    return call_rpc_sync(start_split_rpc(std::move(req), RPC_CM_START_PARTITION_SPLIT));
}

error_with<control_split_response>
replication_ddl_client::pause_partition_split(const std::string &app_name,
                                              const int32_t parent_pidx)
{
    return control_partition_split(app_name, split_control_type::PAUSE, parent_pidx, 0);
}

error_with<control_split_response>
replication_ddl_client::restart_partition_split(const std::string &app_name,
                                                const int32_t parent_pidx)
{
    return control_partition_split(app_name, split_control_type::RESTART, parent_pidx, 0);
}

error_with<control_split_response>
replication_ddl_client::cancel_partition_split(const std::string &app_name,
                                               const int32_t old_partition_count)
{
    return control_partition_split(app_name, split_control_type::CANCEL, -1, old_partition_count);
}

error_with<control_split_response>
replication_ddl_client::control_partition_split(const std::string &app_name,
                                                split_control_type::type control_type,
                                                const int32_t parent_pidx,
                                                const int32_t old_partition_count)
{
    auto req = std::make_unique<control_split_request>();
    req->__set_app_name(app_name);
    req->__set_control_type(control_type);
    req->__set_parent_pidx(parent_pidx);
    req->__set_old_partition_count(old_partition_count);
    return call_rpc_sync(control_split_rpc(std::move(req), RPC_CM_CONTROL_PARTITION_SPLIT));
}

error_with<query_split_response>
replication_ddl_client::query_partition_split(const std::string &app_name)
{
    auto req = std::make_unique<query_split_request>();
    req->__set_app_name(app_name);
    return call_rpc_sync(query_split_rpc(std::move(req), RPC_CM_QUERY_PARTITION_SPLIT));
}

error_with<add_new_disk_response> replication_ddl_client::add_new_disk(const host_port &target_node,
                                                                       const std::string &disk_str)
{
    auto req = std::make_unique<add_new_disk_request>();
    req->disk_str = disk_str;

    std::map<host_port, add_new_disk_rpc> add_new_disk_rpcs;
    add_new_disk_rpcs.emplace(target_node, add_new_disk_rpc(std::move(req), RPC_ADD_NEW_DISK));

    std::map<host_port, error_with<add_new_disk_response>> resps;
    call_rpcs_sync(add_new_disk_rpcs, resps);
    return resps.begin()->second.get_value();
}

error_with<start_app_manual_compact_response> replication_ddl_client::start_app_manual_compact(
    const std::string &app_name, bool bottommost, const int32_t level, const int32_t max_count)
{
    auto req = std::make_unique<start_app_manual_compact_request>();
    req->app_name = app_name;
    req->__set_trigger_time(dsn_now_s());
    req->__set_target_level(level);
    req->__set_bottommost(bottommost);
    if (max_count > 0) {
        req->__set_max_running_count(max_count);
    }
    return call_rpc_sync(start_manual_compact_rpc(std::move(req), RPC_CM_START_MANUAL_COMPACT));
}

error_with<query_app_manual_compact_response>
replication_ddl_client::query_app_manual_compact(const std::string &app_name)
{
    auto req = std::make_unique<query_app_manual_compact_request>();
    req->app_name = app_name;
    return call_rpc_sync(
        query_manual_compact_rpc(std::move(req), RPC_CM_QUERY_MANUAL_COMPACT_STATUS));
}

error_with<configuration_get_max_replica_count_response>
replication_ddl_client::get_max_replica_count(const std::string &app_name)
{
    auto req = std::make_unique<configuration_get_max_replica_count_request>();
    req->__set_app_name(app_name);
    return call_rpc_sync(
        configuration_get_max_replica_count_rpc(std::move(req), RPC_CM_GET_MAX_REPLICA_COUNT));
}

error_with<configuration_set_max_replica_count_response>
replication_ddl_client::set_max_replica_count(const std::string &app_name,
                                              int32_t max_replica_count)
{
    auto req = std::make_unique<configuration_set_max_replica_count_request>();
    req->__set_app_name(app_name);
    req->__set_max_replica_count(max_replica_count);
    return call_rpc_sync(
        configuration_set_max_replica_count_rpc(std::move(req), RPC_CM_SET_MAX_REPLICA_COUNT));
}

error_with<configuration_get_atomic_idempotent_response>
replication_ddl_client::get_atomic_idempotent(const std::string &app_name)
{
    auto req = std::make_unique<configuration_get_atomic_idempotent_request>();
    req->__set_app_name(app_name);
    return call_rpc_sync(
        configuration_get_atomic_idempotent_rpc(std::move(req), RPC_CM_GET_ATOMIC_IDEMPOTENT));
}

error_with<configuration_set_atomic_idempotent_response>
replication_ddl_client::set_atomic_idempotent(const std::string &app_name, bool atomic_idempotent)
{
    auto req = std::make_unique<configuration_set_atomic_idempotent_request>();
    req->__set_app_name(app_name);
    req->__set_atomic_idempotent(atomic_idempotent);
    return call_rpc_sync(
        configuration_set_atomic_idempotent_rpc(std::move(req), RPC_CM_SET_ATOMIC_IDEMPOTENT));
}

error_with<configuration_rename_app_response>
replication_ddl_client::rename_app(const std::string &old_app_name, const std::string &new_app_name)
{
    RETURN_ES_NOT_OK_MSG(
        validate_app_name(old_app_name), "invalid old_app_name: '{}'", old_app_name);
    RETURN_ES_NOT_OK_MSG(
        validate_app_name(new_app_name), "invalid new_app_name: '{}'", new_app_name);

    auto req = std::make_unique<configuration_rename_app_request>();
    req->__set_old_app_name(old_app_name);
    req->__set_new_app_name(new_app_name);
    return call_rpc_sync(configuration_rename_app_rpc(std::move(req), RPC_CM_RENAME_APP));
}
} // namespace replication
} // namespace dsn
