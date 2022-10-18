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

#pragma once

#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "duplication_types.h"
#include "bulk_load_types.h"
#include "backup_types.h"
#include "consensus_types.h"
#include "replica_admin_types.h"
#include "runtime/rpc/rpc_holder.h"

namespace dsn {
namespace replication {

typedef rpc_holder<configuration_update_app_env_request, configuration_update_app_env_response>
    app_env_rpc;
typedef rpc_holder<ddd_diagnose_request, ddd_diagnose_response> ddd_diagnose_rpc;
typedef rpc_holder<configuration_query_by_node_request, configuration_query_by_node_response>
    configuration_query_by_node_rpc;
typedef rpc_holder<configuration_query_by_index_request, configuration_query_by_index_response>
    configuration_query_by_index_rpc;
typedef rpc_holder<configuration_list_apps_request, configuration_list_apps_response>
    configuration_list_apps_rpc;
typedef rpc_holder<configuration_list_nodes_request, configuration_list_nodes_response>
    configuration_list_nodes_rpc;
typedef rpc_holder<configuration_cluster_info_request, configuration_cluster_info_response>
    configuration_cluster_info_rpc;
typedef rpc_holder<configuration_balancer_request, configuration_balancer_response>
    configuration_balancer_rpc;
typedef rpc_holder<configuration_meta_control_request, configuration_meta_control_response>
    configuration_meta_control_rpc;
typedef rpc_holder<configuration_recovery_request, configuration_recovery_response>
    configuration_recovery_rpc;
typedef rpc_holder<configuration_report_restore_status_request,
                   configuration_report_restore_status_response>
    configuration_report_restore_status_rpc;
typedef rpc_holder<configuration_query_restore_request, configuration_query_restore_response>
    configuration_query_restore_rpc;
typedef rpc_holder<configuration_query_backup_policy_request,
                   configuration_query_backup_policy_response>
    query_backup_policy_rpc;
typedef rpc_holder<configuration_modify_backup_policy_request,
                   configuration_modify_backup_policy_response>
    configuration_modify_backup_policy_rpc;
typedef rpc_holder<start_backup_app_request, start_backup_app_response> start_backup_app_rpc;
typedef rpc_holder<query_backup_status_request, query_backup_status_response>
    query_backup_status_rpc;
typedef rpc_holder<configuration_get_max_replica_count_request,
                   configuration_get_max_replica_count_response>
    configuration_get_max_replica_count_rpc;
typedef rpc_holder<configuration_set_max_replica_count_request,
                   configuration_set_max_replica_count_response>
    configuration_set_max_replica_count_rpc;

} // namespace replication
} // namespace dsn
