// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <string>

#include "runtime/api_layer1.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include "common/replica_envs.h"
#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "duplication_types.h"
#include "bulk_load_types.h"
#include "backup_types.h"
#include "consensus_types.h"
#include "replica_admin_types.h"
#include "common//duplication_common.h"
#include "utils/config_api.h"
#include "utils/output_utils.h"
#include "utils/time_utils.h"

#include "meta_http_service.h"
#include "meta_server_failure_detector.h"
#include "server_load_balancer.h"
#include "server_state.h"
#include "meta/duplication/meta_duplication_service.h"
#include "meta/meta_bulk_load_service.h"

namespace dsn {
namespace dist {
DSN_DECLARE_string(hosts_list);
} // namespace dist
namespace replication {

struct list_nodes_helper
{
    std::string node_address;
    std::string node_status;
    int primary_count;
    int secondary_count;
    list_nodes_helper(const std::string &a, const std::string &s)
        : node_address(a), node_status(s), primary_count(0), secondary_count(0)
    {
    }
};

void meta_http_service::get_app_handler(const http_request &req, http_response &resp)
{
    std::string app_name;
    bool detailed = false;
    for (const auto &p : req.query_args) {
        if (p.first == "name") {
            app_name = p.second;
        } else if (p.first == "detail") {
            detailed = true;
        } else {
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }
    if (!redirect_if_not_primary(req, resp))
        return;

    query_cfg_request request;
    query_cfg_response response;

    request.app_name = app_name;
    _service->_state->query_configuration_by_index(request, response);
    if (response.err == ERR_OBJECT_NOT_FOUND) {
        resp.status_code = http_status_code::not_found;
        resp.body = fmt::format("table not found: \"{}\"", app_name);
        return;
    }
    if (response.err != dsn::ERR_OK) {
        resp.body = response.err.to_string();
        resp.status_code = http_status_code::internal_server_error;
        return;
    }

    // output as json format
    dsn::utils::multi_table_printer mtp;
    std::ostringstream out;
    dsn::utils::table_printer tp_general("general");
    tp_general.add_row_name_and_data("app_name", app_name);
    tp_general.add_row_name_and_data("app_id", response.app_id);
    tp_general.add_row_name_and_data("partition_count", response.partition_count);
    if (!response.partitions.empty()) {
        tp_general.add_row_name_and_data("max_replica_count",
                                         response.partitions[0].max_replica_count);
    } else {
        tp_general.add_row_name_and_data("max_replica_count", 0);
    }
    mtp.add(std::move(tp_general));

    if (detailed) {
        dsn::utils::table_printer tp_details("replicas");
        tp_details.add_title("pidx");
        tp_details.add_column("ballot");
        tp_details.add_column("replica_count");
        tp_details.add_column("primary");
        tp_details.add_column("secondaries");
        std::map<rpc_address, std::pair<int, int>> node_stat;

        int total_prim_count = 0;
        int total_sec_count = 0;
        int fully_healthy = 0;
        int write_unhealthy = 0;
        int read_unhealthy = 0;
        for (const auto &p : response.partitions) {
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
                    fully_healthy++;
                else if (replica_count < 2)
                    write_unhealthy++;
            } else {
                write_unhealthy++;
                read_unhealthy++;
            }
            tp_details.add_row(p.pid.get_partition_index());
            tp_details.append_data(p.ballot);
            std::stringstream oss;
            oss << replica_count << "/" << p.max_replica_count;
            tp_details.append_data(oss.str());
            tp_details.append_data((p.primary.is_invalid() ? "-" : p.primary.to_std_string()));
            oss.str("");
            oss << "[";
            for (int j = 0; j < p.secondaries.size(); j++) {
                if (j != 0)
                    oss << ",";
                oss << p.secondaries[j].to_std_string();
                node_stat[p.secondaries[j]].second++;
            }
            oss << "]";
            tp_details.append_data(oss.str());
        }
        mtp.add(std::move(tp_details));

        // 'node' section.
        dsn::utils::table_printer tp_nodes("nodes");
        tp_nodes.add_title("node");
        tp_nodes.add_column("primary");
        tp_nodes.add_column("secondary");
        tp_nodes.add_column("total");
        for (auto &kv : node_stat) {
            tp_nodes.add_row(kv.first.to_std_string());
            tp_nodes.append_data(kv.second.first);
            tp_nodes.append_data(kv.second.second);
            tp_nodes.append_data(kv.second.first + kv.second.second);
        }
        tp_nodes.add_row("total");
        tp_nodes.append_data(total_prim_count);
        tp_nodes.append_data(total_sec_count);
        tp_nodes.append_data(total_prim_count + total_sec_count);
        mtp.add(std::move(tp_nodes));

        // healthy partition count section.
        dsn::utils::table_printer tp_hpc("healthy");
        tp_hpc.add_row_name_and_data("fully_healthy_partition_count", fully_healthy);
        tp_hpc.add_row_name_and_data("unhealthy_partition_count",
                                     response.partition_count - fully_healthy);
        tp_hpc.add_row_name_and_data("write_unhealthy_partition_count", write_unhealthy);
        tp_hpc.add_row_name_and_data("read_unhealthy_partition_count", read_unhealthy);
        mtp.add(std::move(tp_hpc));
    }

    mtp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::list_app_handler(const http_request &req, http_response &resp)
{
    bool detailed = false;
    for (const auto &p : req.query_args) {
        if (p.first == "detail") {
            detailed = true;
        } else {
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }
    if (!redirect_if_not_primary(req, resp))
        return;
    configuration_list_apps_response response;
    configuration_list_apps_request request;
    request.status = dsn::app_status::AS_INVALID;

    _service->_state->list_apps(request, response);

    if (response.err != dsn::ERR_OK) {
        resp.body = response.err.to_string();
        resp.status_code = http_status_code::internal_server_error;
        return;
    }
    std::vector<::dsn::app_info> &apps = response.infos;

    // output as json format
    std::ostringstream out;
    dsn::utils::multi_table_printer mtp;
    int available_app_count = 0;
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
    for (const auto &app : apps) {
        if (app.status != dsn::app_status::AS_AVAILABLE) {
            continue;
        }
        std::string status_str = enum_to_string(app.status);
        status_str = status_str.substr(status_str.find("AS_") + 3);
        std::string create_time = "-";
        if (app.create_second > 0) {
            char buf[24];
            dsn::utils::time_ms_to_string((uint64_t)app.create_second * 1000, buf);
            create_time = buf;
        }
        std::string drop_time = "-";
        std::string drop_expire_time = "-";
        if (app.status == app_status::AS_AVAILABLE) {
            available_app_count++;
        } else if (app.status == app_status::AS_DROPPED && app.expire_second > 0) {
            if (app.drop_second > 0) {
                char buf[24];
                dsn::utils::time_ms_to_string((uint64_t)app.drop_second * 1000, buf);
                drop_time = buf;
            }
            if (app.expire_second > 0) {
                char buf[24];
                dsn::utils::time_ms_to_string((uint64_t)app.expire_second * 1000, buf);
                drop_expire_time = buf;
            }
        }

        tp_general.add_row(app.app_id);
        tp_general.append_data(status_str);
        tp_general.append_data(app.app_name);
        tp_general.append_data(app.app_type);
        tp_general.append_data(app.partition_count);
        tp_general.append_data(app.max_replica_count);
        tp_general.append_data(app.is_stateful);
        tp_general.append_data(create_time);
        tp_general.append_data(drop_time);
        tp_general.append_data(drop_expire_time);
        tp_general.append_data(app.envs.size());
    }
    mtp.add(std::move(tp_general));

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
        for (auto &info : apps) {
            if (info.status != app_status::AS_AVAILABLE) {
                continue;
            }
            query_cfg_request request;
            query_cfg_response response;
            request.app_name = info.app_name;
            _service->_state->query_configuration_by_index(request, response);
            CHECK_EQ(info.app_id, response.app_id);
            CHECK_EQ(info.partition_count, response.partition_count);
            int fully_healthy = 0;
            int write_unhealthy = 0;
            int read_unhealthy = 0;
            for (int i = 0; i < response.partitions.size(); i++) {
                const dsn::partition_configuration &p = response.partitions[i];
                int replica_count = 0;
                if (!p.primary.is_invalid()) {
                    replica_count++;
                }
                replica_count += p.secondaries.size();
                if (!p.primary.is_invalid()) {
                    if (replica_count >= p.max_replica_count)
                        fully_healthy++;
                    else if (replica_count < 2)
                        write_unhealthy++;
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

            if (fully_healthy == info.partition_count)
                total_fully_healthy_app_count++;
            else
                total_unhealthy_app_count++;
            if (write_unhealthy > 0)
                total_write_unhealthy_app_count++;
            if (read_unhealthy > 0)
                total_read_unhealthy_app_count++;
        }
        mtp.add(std::move(tp_health));
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
    mtp.add(std::move(tp_count));

    mtp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::list_node_handler(const http_request &req, http_response &resp)
{
    bool detailed = false;
    for (const auto &p : req.query_args) {
        if (p.first == "detail") {
            detailed = true;
        } else {
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }
    if (!redirect_if_not_primary(req, resp))
        return;

    std::map<dsn::rpc_address, list_nodes_helper> tmp_map;
    for (const auto &node : _service->_alive_set) {
        tmp_map.emplace(node, list_nodes_helper(node.to_std_string(), "ALIVE"));
    }
    for (const auto &node : _service->_dead_set) {
        tmp_map.emplace(node, list_nodes_helper(node.to_std_string(), "UNALIVE"));
    }
    int alive_node_count = (_service->_alive_set).size();
    int unalive_node_count = (_service->_dead_set).size();

    if (detailed) {
        configuration_list_apps_response response;
        configuration_list_apps_request request;
        request.status = dsn::app_status::AS_AVAILABLE;
        _service->_state->list_apps(request, response);
        for (const auto &app : response.infos) {
            query_cfg_request request_app;
            query_cfg_response response_app;
            request_app.app_name = app.app_name;
            _service->_state->query_configuration_by_index(request_app, response_app);
            CHECK_EQ(app.app_id, response_app.app_id);
            CHECK_EQ(app.partition_count, response_app.partition_count);

            for (int i = 0; i < response_app.partitions.size(); i++) {
                const dsn::partition_configuration &p = response_app.partitions[i];
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

    // output as json format
    std::ostringstream out;
    dsn::utils::multi_table_printer mtp;
    dsn::utils::table_printer tp_details("details");
    tp_details.add_title("address");
    tp_details.add_column("status");
    if (detailed) {
        tp_details.add_column("replica_count");
        tp_details.add_column("primary_count");
        tp_details.add_column("secondary_count");
    }
    for (const auto &kv : tmp_map) {
        tp_details.add_row(kv.second.node_address);
        tp_details.append_data(kv.second.node_status);
        if (detailed) {
            tp_details.append_data(kv.second.primary_count + kv.second.secondary_count);
            tp_details.append_data(kv.second.primary_count);
            tp_details.append_data(kv.second.secondary_count);
        }
    }
    mtp.add(std::move(tp_details));

    dsn::utils::table_printer tp_count("summary");
    tp_count.add_row_name_and_data("total_node_count", alive_node_count + unalive_node_count);
    tp_count.add_row_name_and_data("alive_node_count", alive_node_count);
    tp_count.add_row_name_and_data("unalive_node_count", unalive_node_count);
    mtp.add(std::move(tp_count));
    mtp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::get_cluster_info_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp))
        return;

    dsn::utils::table_printer tp;
    std::ostringstream out;
    std::string meta_servers_str;
    int ms_size = _service->_opts.meta_servers.size();
    for (int i = 0; i < ms_size; i++) {
        meta_servers_str += _service->_opts.meta_servers[i].to_std_string();
        if (i != ms_size - 1) {
            meta_servers_str += ",";
        }
    }
    tp.add_row_name_and_data("meta_servers", meta_servers_str);
    tp.add_row_name_and_data("primary_meta_server", dsn_primary_address().to_std_string());
    tp.add_row_name_and_data("zookeeper_hosts", dsn::dist::FLAGS_hosts_list);
    tp.add_row_name_and_data("zookeeper_root", _service->_cluster_root);
    tp.add_row_name_and_data(
        "meta_function_level",
        _meta_function_level_VALUES_TO_NAMES.find(_service->get_function_level())->second + 3);
    std::vector<std::string> balance_operation_type;
    balance_operation_type.emplace_back("detail");
    tp.add_row_name_and_data(
        "balance_operation_count",
        _service->_balancer->get_balance_operation_count(balance_operation_type));
    double primary_stddev, total_stddev;
    _service->_state->get_cluster_balance_score(primary_stddev, total_stddev);
    tp.add_row_name_and_data("primary_replica_count_stddev", primary_stddev);
    tp.add_row_name_and_data("total_replica_count_stddev", total_stddev);
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::get_app_envs_handler(const http_request &req, http_response &resp)
{
    // only primary process the request
    if (!redirect_if_not_primary(req, resp))
        return;

    std::string app_name;
    for (const auto &p : req.query_args) {
        if ("name" == p.first) {
            app_name = p.second;
            break;
        }
    }
    if (app_name.empty()) {
        resp.status_code = http_status_code::bad_request;
        resp.body = "app name shouldn't be empty";
        return;
    }

    // get all of the apps
    configuration_list_apps_response response;
    configuration_list_apps_request request;
    request.status = dsn::app_status::AS_AVAILABLE;
    _service->_state->list_apps(request, response);
    if (response.err != dsn::ERR_OK) {
        resp.body = response.err.to_string();
        resp.status_code = http_status_code::internal_server_error;
        return;
    }

    // using app envs to generate a table_printer
    dsn::utils::table_printer tp;
    for (auto &app : response.infos) {
        if (app.app_name == app_name) {
            for (auto env : app.envs) {
                tp.add_row_name_and_data(env.first, env.second);
            }
            break;
        }
    }

    // output as json format
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

std::string set_to_string(const std::set<int32_t> &s)
{
    std::stringstream out;
    rapidjson::OStreamWrapper wrapper(out);
    dsn::json::JsonWriter writer(wrapper);
    dsn::json::json_encode(writer, s);
    return out.str();
}

void meta_http_service::query_backup_policy_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp))
        return;

    if (_service->_backup_handler == nullptr) {
        resp.body = "cold_backup_disabled";
        resp.status_code = http_status_code::not_found;
        return;
    }
    auto request = dsn::make_unique<configuration_query_backup_policy_request>();
    std::vector<std::string> policy_names;
    for (const auto &p : req.query_args) {
        if (p.first == "name") {
            policy_names.push_back(p.second);
        } else {
            resp.body = "Invalid parameter";
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }
    request->policy_names = std::move(policy_names);
    query_backup_policy_rpc http_to_rpc(std::move(request), LPC_DEFAULT_CALLBACK);
    _service->_backup_handler->query_backup_policy(http_to_rpc);
    auto rpc_return = http_to_rpc.response();

    dsn::utils::table_printer tp_query_backup_policy;
    tp_query_backup_policy.add_title("name");
    tp_query_backup_policy.add_column("backup_provider_type");
    tp_query_backup_policy.add_column("backup_interval");
    tp_query_backup_policy.add_column("app_ids");
    tp_query_backup_policy.add_column("start_time");
    tp_query_backup_policy.add_column("status");
    tp_query_backup_policy.add_column("backup_history_count");
    for (const auto &cur_policy : rpc_return.policys) {
        tp_query_backup_policy.add_row(cur_policy.policy_name);
        tp_query_backup_policy.append_data(cur_policy.backup_provider_type);
        tp_query_backup_policy.append_data(cur_policy.backup_interval_seconds);
        tp_query_backup_policy.append_data(set_to_string(cur_policy.app_ids));
        tp_query_backup_policy.append_data(cur_policy.start_time);
        tp_query_backup_policy.append_data(cur_policy.is_disable ? "disabled" : "enabled");
        tp_query_backup_policy.append_data(cur_policy.backup_history_count_to_keep);
    }
    std::ostringstream out;
    tp_query_backup_policy.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::query_duplication_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp)) {
        return;
    }
    if (_service->_dup_svc == nullptr) {
        resp.body = "duplication is not enabled [FLAGS_duplication_enabled=false]";
        resp.status_code = http_status_code::not_found;
        return;
    }
    duplication_query_request rpc_req;
    auto it = req.query_args.find("name");
    if (it == req.query_args.end()) {
        resp.body = "name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    rpc_req.app_name = it->second;
    duplication_query_response rpc_resp;
    _service->_dup_svc->query_duplication_info(rpc_req, rpc_resp);
    if (rpc_resp.err != ERR_OK) {
        resp.body = rpc_resp.err.to_string();
        if (rpc_resp.err == ERR_APP_NOT_EXIST) {
            resp.status_code = http_status_code::not_found;
        } else {
            resp.status_code = http_status_code::internal_server_error;
        }
        return;
    }
    resp.status_code = http_status_code::ok;
    resp.body = duplication_query_response_to_string(rpc_resp);
}

void meta_http_service::start_bulk_load_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp)) {
        return;
    }

    if (_service->_bulk_load_svc == nullptr) {
        resp.body = "bulk load is not enabled";
        resp.status_code = http_status_code::not_found;
        return;
    }

    start_bulk_load_request request;
    bool ret = json::json_forwarder<start_bulk_load_request>::decode(req.body, request);
    if (!ret) {
        resp.body = "invalid request structure";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (request.app_name.empty()) {
        resp.body = "app_name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (request.cluster_name.empty()) {
        resp.body = "cluster_name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (request.file_provider_type.empty()) {
        resp.body = "file_provider_type should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (request.remote_root_path.empty()) {
        resp.body = "remote_root_path should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }

    auto rpc_req = dsn::make_unique<start_bulk_load_request>(request);
    start_bulk_load_rpc rpc(std::move(rpc_req), LPC_META_CALLBACK);
    _service->_bulk_load_svc->on_start_bulk_load(rpc);

    auto rpc_resp = rpc.response();
    // output as json format
    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("error", rpc_resp.err.to_string());
    tp.add_row_name_and_data("hint_msg", rpc_resp.hint_msg);
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::query_bulk_load_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp)) {
        return;
    }

    if (_service->_bulk_load_svc == nullptr) {
        resp.body = "bulk load is not enabled";
        resp.status_code = http_status_code::not_found;
        return;
    }

    auto it = req.query_args.find("name");
    if (it == req.query_args.end()) {
        resp.body = "name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }

    auto rpc_req = dsn::make_unique<query_bulk_load_request>();
    rpc_req->app_name = it->second;
    query_bulk_load_rpc rpc(std::move(rpc_req), LPC_META_CALLBACK);
    _service->_bulk_load_svc->on_query_bulk_load_status(rpc);
    auto rpc_resp = rpc.response();
    // output as json format
    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("error", rpc_resp.err.to_string());
    tp.add_row_name_and_data("app_status", dsn::enum_to_string(rpc_resp.app_status));
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void meta_http_service::start_compaction_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp)) {
        return;
    }

    // validate parameters
    manual_compaction_info info;
    bool ret = json::json_forwarder<manual_compaction_info>::decode(req.body, info);

    if (!ret) {
        resp.body = "invalid request structure";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.app_name.empty()) {
        resp.body = "app_name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.type.empty() || (info.type != "once" && info.type != "periodic")) {
        resp.body = "type should ony be 'once' or 'periodic'";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.target_level < -1) {
        resp.body = "target_level should be >= -1";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.bottommost_level_compaction.empty() || (info.bottommost_level_compaction != "skip" &&
                                                     info.bottommost_level_compaction != "force")) {
        resp.body = "bottommost_level_compaction should ony be 'skip' or 'force'";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.max_concurrent_running_count < 0) {
        resp.body = "max_running_count should be >= 0";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.type == "periodic" && info.trigger_time.empty()) {
        resp.body = "trigger_time should not be empty when type is periodic";
        resp.status_code = http_status_code::bad_request;
        return;
    }

    // create configuration_update_app_env_request
    std::vector<std::string> keys;
    std::vector<std::string> values;
    if (info.type == "once") {
        keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL);
        keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION);
        keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME);
    } else {
        keys.emplace_back(replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL);
        keys.emplace_back(replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION);
        keys.emplace_back(replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME);
    }
    values.emplace_back(std::to_string(info.target_level));
    values.emplace_back(info.bottommost_level_compaction);
    values.emplace_back(info.type == "once" ? std::to_string(dsn_now_s()) : info.trigger_time);
    if (info.max_concurrent_running_count > 0) {
        keys.emplace_back(replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT);
        values.emplace_back(std::to_string(info.max_concurrent_running_count));
    }
    update_app_env(info.app_name, keys, values, resp);
}

void meta_http_service::update_scenario_handler(const http_request &req, http_response &resp)
{
    if (!redirect_if_not_primary(req, resp)) {
        return;
    }

    // validate paramters
    usage_scenario_info info;
    bool ret = json::json_forwarder<usage_scenario_info>::decode(req.body, info);
    if (!ret) {
        resp.body = "invalid request structure";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.app_name.empty()) {
        resp.body = "app_name should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    if (info.scenario.empty() || (info.scenario != "bulk_load" && info.scenario != "normal")) {
        resp.body = "scenario should ony be 'normal' or 'bulk_load'";
        resp.status_code = http_status_code::bad_request;
        return;
    }

    // create configuration_update_app_env_request
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.emplace_back(replica_envs::ROCKSDB_USAGE_SCENARIO);
    values.emplace_back(info.scenario);
    update_app_env(info.app_name, keys, values, resp);
}

bool meta_http_service::redirect_if_not_primary(const http_request &req, http_response &resp)
{
#ifdef DSN_MOCK_TEST
    return true;
#endif
    rpc_address leader;
    if (_service->_failure_detector->get_leader(&leader))
        return true;
    // set redirect response
    resp.location = "http://" + leader.to_std_string() + '/' + req.path;
    if (!req.query_args.empty()) {
        resp.location += '?';
        for (const auto &i : req.query_args) {
            resp.location += i.first + '=' + i.second + '&';
        }
        resp.location.pop_back(); // remove final '&'
    }
    resp.location.erase(std::remove(resp.location.begin(), resp.location.end(), '\0'),
                        resp.location.end()); // remove final '\0'
    resp.status_code = http_status_code::temporary_redirect;
    return false;
}

void meta_http_service::update_app_env(const std::string &app_name,
                                       const std::vector<std::string> &keys,
                                       const std::vector<std::string> &values,
                                       http_response &resp)
{
    configuration_update_app_env_request request;
    request.app_name = app_name;
    request.op = app_env_operation::APP_ENV_OP_SET;
    request.__set_keys(keys);
    request.__set_values(values);

    auto rpc_req = dsn::make_unique<configuration_update_app_env_request>(request);
    update_app_env_rpc rpc(std::move(rpc_req), LPC_META_STATE_NORMAL);
    _service->_state->set_app_envs(rpc);

    auto rpc_resp = rpc.response();
    // output as json format
    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("error", rpc_resp.err.to_string());
    tp.add_row_name_and_data("hint_message", rpc_resp.hint_message);
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

} // namespace replication
} // namespace dsn
