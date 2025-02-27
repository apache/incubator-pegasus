/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <getopt.h>
#include <thread>
#include <iomanip>
#include <fstream>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <rocksdb/db.h>
#include "utils/filesystem.h"
#include "utils/output_utils.h"
#include "utils/string_conv.h"
#include <string_view>
#include "client/replication_ddl_client.h"
#include "tools/mutation_log_tool.h"

#include <rrdb/rrdb.code.definition.h>
#include <rrdb/rrdb_types.h>
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <pegasus/error.h>

#include "command_executor.h"
#include "command_helper.h"
#include "args.h"

using namespace dsn::replication;
using tp_alignment = ::dsn::utils::table_printer::alignment;
using tp_output_format = ::dsn::utils::table_printer::output_format;

static const char *INDENT = "  ";
struct list_nodes_helper
{
    std::string node_name;
    std::string node_status;
    int primary_count;
    int secondary_count;
    int64_t memused_res_mb;
    int64_t block_cache_bytes;
    int64_t mem_tbl_bytes;
    int64_t mem_idx_bytes;
    int64_t disk_available_total_ratio;
    int64_t disk_available_min_ratio;
    double get_qps;
    double put_qps;
    double multi_get_qps;
    double batch_get_qps;
    double multi_put_qps;
    double get_p99;
    double put_p99;
    double multi_get_p99;
    double batch_get_p99;
    double multi_put_p99;
    double read_cu;
    double write_cu;
    list_nodes_helper(const std::string &n, const std::string &s)
        : node_name(n),
          node_status(s),
          primary_count(0),
          secondary_count(0),
          memused_res_mb(0),
          block_cache_bytes(0),
          mem_tbl_bytes(0),
          mem_idx_bytes(0),
          disk_available_total_ratio(0),
          disk_available_min_ratio(0),
          get_qps(0.0),
          put_qps(0.0),
          multi_get_qps(0.0),
          multi_put_qps(0.0),
          get_p99(0.0),
          put_p99(0.0),
          multi_get_p99(0.0),
          multi_put_p99(0.0),
          read_cu(0.0),
          write_cu(0.0)
    {
    }
};

// == miscellaneous (see 'commands/misc.cpp') == //

bool help_info(command_executor *e, shell_context *sc, arguments args);

bool version(command_executor *e, shell_context *sc, arguments args);

bool exit_shell(command_executor *e, shell_context *sc, arguments args);

// == global properties (see 'commands/global_properties.cpp') == //

bool use_app_as_current(command_executor *e, shell_context *sc, arguments args);

bool process_escape_all(command_executor *e, shell_context *sc, arguments args);

bool process_timeout(command_executor *e, shell_context *sc, arguments args);

bool cc_command(command_executor *e, shell_context *sc, arguments args);

// == node management (see 'commands/node_management.cpp') == //

bool query_cluster_info(command_executor *e, shell_context *sc, arguments args);

bool ls_nodes(command_executor *e, shell_context *sc, arguments args);

bool server_info(command_executor *e, shell_context *sc, arguments args);

bool server_stat(command_executor *e, shell_context *sc, arguments args);

bool remote_command(command_executor *e, shell_context *sc, arguments args);

bool flush_log(command_executor *e, shell_context *sc, arguments args);

// == table management (see 'commands/table_management.cpp') == //

bool ls_apps(command_executor *e, shell_context *sc, arguments args);

bool query_app(command_executor *e, shell_context *sc, arguments args);

bool app_disk(command_executor *e, shell_context *sc, arguments args);

bool app_stat(command_executor *e, shell_context *sc, arguments args);

bool create_app(command_executor *e, shell_context *sc, arguments args);

bool drop_app(command_executor *e, shell_context *sc, arguments args);

bool recall_app(command_executor *e, shell_context *sc, arguments args);

bool rename_app(command_executor *e, shell_context *sc, arguments args);

bool get_app_envs(command_executor *e, shell_context *sc, arguments args);

bool set_app_envs(command_executor *e, shell_context *sc, arguments args);

bool del_app_envs(command_executor *e, shell_context *sc, arguments args);

bool clear_app_envs(command_executor *e, shell_context *sc, arguments args);

bool get_max_replica_count(command_executor *e, shell_context *sc, arguments args);

bool set_max_replica_count(command_executor *e, shell_context *sc, arguments args);

// == data operations (see 'commands/data_operations.cpp') == //

bool data_operations(command_executor *e, shell_context *sc, arguments args);

bool set_value(command_executor *e, shell_context *sc, arguments args);

bool multi_set_value(command_executor *e, shell_context *sc, arguments args);

bool get_value(command_executor *e, shell_context *sc, arguments args);

bool multi_get_value(command_executor *e, shell_context *sc, arguments args);

bool multi_get_range(command_executor *e, shell_context *sc, arguments args);

bool multi_get_sortkeys(command_executor *e, shell_context *sc, arguments args);

bool delete_value(command_executor *e, shell_context *sc, arguments args);

bool multi_del_value(command_executor *e, shell_context *sc, arguments args);

bool multi_del_range(command_executor *e, shell_context *sc, arguments args);

bool incr(command_executor *e, shell_context *sc, arguments args);

bool check_and_set(command_executor *e, shell_context *sc, arguments args);

bool check_and_mutate(command_executor *e, shell_context *sc, arguments args);

bool exist(command_executor *e, shell_context *sc, arguments args);

bool sortkey_count(command_executor *e, shell_context *sc, arguments args);

bool get_ttl(command_executor *e, shell_context *sc, arguments args);

bool calculate_hash_value(command_executor *e, shell_context *sc, arguments args);

bool hash_scan(command_executor *e, shell_context *sc, arguments args);

bool full_scan(command_executor *e, shell_context *sc, arguments args);

bool copy_data(command_executor *e, shell_context *sc, arguments args);

bool clear_data(command_executor *e, shell_context *sc, arguments args);

bool count_data(command_executor *e, shell_context *sc, arguments args);

// == load balancing(see 'commands/rebalance.cpp') == //

bool set_meta_level(command_executor *e, shell_context *sc, arguments args);

bool get_meta_level(command_executor *e, shell_context *sc, arguments args);

bool propose(command_executor *e, shell_context *sc, arguments args);

bool balance(command_executor *e, shell_context *sc, arguments args);

// == data recovery(see 'commands/recovery.cpp') == //

bool recover(command_executor *e, shell_context *sc, arguments args);

bool ddd_diagnose(command_executor *e, shell_context *sc, arguments args);

// == cold backup (see 'commands/cold_backup.cpp') == //

bool add_backup_policy(command_executor *e, shell_context *sc, arguments args);

bool ls_backup_policy(command_executor *e, shell_context *sc, arguments args);

bool modify_backup_policy(command_executor *e, shell_context *sc, arguments args);

extern const std::string disable_backup_policy_help;
bool disable_backup_policy(command_executor *e, shell_context *sc, arguments args);

bool enable_backup_policy(command_executor *e, shell_context *sc, arguments args);

bool restore(command_executor *e, shell_context *sc, arguments args);

bool query_backup_policy(command_executor *e, shell_context *sc, arguments args);

bool query_restore_status(command_executor *e, shell_context *sc, arguments args);

// == debugger (see 'commands/debugger.cpp') == //Debugging tool

bool sst_dump(command_executor *e, shell_context *sc, arguments args);

bool mlog_dump(command_executor *e, shell_context *sc, arguments args);

bool local_get(command_executor *e, shell_context *sc, arguments args);

bool rdb_key_hex2str(command_executor *e, shell_context *sc, arguments args);

bool rdb_key_str2hex(command_executor *e, shell_context *sc, arguments args);

bool rdb_value_hex2str(command_executor *e, shell_context *sc, arguments args);

// == duplication (see 'commands/duplication.cpp') == //

bool add_dup(command_executor *e, shell_context *sc, arguments args);

bool query_dup(command_executor *e, shell_context *sc, arguments args);

bool ls_dups(command_executor *e, shell_context *sc, arguments args);

bool remove_dup(command_executor *e, shell_context *sc, arguments args);

bool start_dup(command_executor *e, shell_context *sc, arguments args);

bool pause_dup(command_executor *e, shell_context *sc, arguments args);

bool set_dup_fail_mode(command_executor *e, shell_context *sc, arguments args);

// == bulk load (see 'commands/bulk_load.cpp') == //

bool start_bulk_load(command_executor *e, shell_context *sc, arguments args);

bool query_bulk_load_status(command_executor *e, shell_context *sc, arguments args);

bool pause_bulk_load(command_executor *e, shell_context *sc, arguments args);

bool restart_bulk_load(command_executor *e, shell_context *sc, arguments args);

bool cancel_bulk_load(command_executor *e, shell_context *sc, arguments args);

bool clear_bulk_load(command_executor *e, shell_context *sc, arguments args);

// == detect hotkey (see 'commands/detect_hotkey.cpp') == //

bool detect_hotkey(command_executor *e, shell_context *sc, arguments args);

// == local partition split (see 'commands/local_partition_split.cpp') == //
extern const std::string local_partition_split_help;
bool local_partition_split(command_executor *e, shell_context *sc, arguments args);
