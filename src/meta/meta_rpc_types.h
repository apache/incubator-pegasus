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
#include "rpc/rpc_holder.h"

namespace dsn::replication {

using app_env_rpc =
    rpc_holder<configuration_update_app_env_request, configuration_update_app_env_response>;
using ddd_diagnose_rpc = rpc_holder<ddd_diagnose_request, ddd_diagnose_response>;
using configuration_query_by_node_rpc =
    rpc_holder<configuration_query_by_node_request, configuration_query_by_node_response>;
using configuration_query_by_index_rpc = rpc_holder<query_cfg_request, query_cfg_response>;
using configuration_list_apps_rpc =
    rpc_holder<configuration_list_apps_request, configuration_list_apps_response>;
using configuration_list_nodes_rpc =
    rpc_holder<configuration_list_nodes_request, configuration_list_nodes_response>;
using configuration_cluster_info_rpc =
    rpc_holder<configuration_cluster_info_request, configuration_cluster_info_response>;
using configuration_balancer_rpc =
    rpc_holder<configuration_balancer_request, configuration_balancer_response>;
using configuration_meta_control_rpc =
    rpc_holder<configuration_meta_control_request, configuration_meta_control_response>;
using configuration_recovery_rpc =
    rpc_holder<configuration_recovery_request, configuration_recovery_response>;
using configuration_report_restore_status_rpc =
    rpc_holder<configuration_report_restore_status_request,
               configuration_report_restore_status_response>;
using configuration_query_restore_rpc =
    rpc_holder<configuration_query_restore_request, configuration_query_restore_response>;
using query_backup_policy_rpc = rpc_holder<configuration_query_backup_policy_request,
                                           configuration_query_backup_policy_response>;
using configuration_modify_backup_policy_rpc =
    rpc_holder<configuration_modify_backup_policy_request,
               configuration_modify_backup_policy_response>;
using start_backup_app_rpc = rpc_holder<start_backup_app_request, start_backup_app_response>;
using query_backup_status_rpc =
    rpc_holder<query_backup_status_request, query_backup_status_response>;
using configuration_get_max_replica_count_rpc =
    rpc_holder<configuration_get_max_replica_count_request,
               configuration_get_max_replica_count_response>;
using configuration_set_max_replica_count_rpc =
    rpc_holder<configuration_set_max_replica_count_request,
               configuration_set_max_replica_count_response>;
using configuration_get_atomic_idempotent_rpc =
    rpc_holder<configuration_get_atomic_idempotent_request,
               configuration_get_atomic_idempotent_response>;
using configuration_set_atomic_idempotent_rpc =
    rpc_holder<configuration_set_atomic_idempotent_request,
               configuration_set_atomic_idempotent_response>;
using configuration_rename_app_rpc =
    rpc_holder<configuration_rename_app_request, configuration_rename_app_response>;

} // namespace dsn::replication
